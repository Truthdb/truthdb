//! The vector engine on a real store: vectors survive a crash, a crash at
//! any op of a put leaves the put whole or absent, a batch too large for one
//! transaction goes over several, a search is exact, and the engine ships
//! to a standby and restores from a log backup.

use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use truthdb_core::relstore::ctx::{FAIL_APPLY_OPS_AFTER, OpMode, RelCtx};
use truthdb_core::relstore::recovery::LogicalUndo;
use truthdb_core::storage::{
    CATALOG_ENGINE_ID, EngineParticipant, FIRST_ENGINE_OBJECT_ID, Storage, StorageError,
    StorageFile, StorageOptions,
};
use truthdb_core::wal::records::{PageOpUndo, RelRecord};

use crate::{
    DistanceMetric, IndexInfo, NewIndex, Query, VecEngine, VecFile, VectorError, VectorInput,
};

/// The catalog slot with nothing in it: the vector engine runs alone.
struct NoCatalog;

struct NoUndo;

impl LogicalUndo for NoUndo {
    fn undo(
        &self,
        _ctx: &mut RelCtx<'_>,
        _mode: &mut OpMode<'_>,
        object_id: u32,
        _undo: &PageOpUndo,
    ) -> Result<(), StorageError> {
        Err(StorageError::InvalidFile(format!(
            "no catalog tree {object_id}"
        )))
    }
}

impl EngineParticipant for NoCatalog {
    fn engine_id(&self) -> u32 {
        CATALOG_ENGINE_ID
    }
    fn owns_object(&self, _object_id: u32) -> bool {
        false
    }
    fn open(&mut self, _metadata_root: Option<u64>, _db_options: u8) {}
    fn metadata_root(&self) -> Option<u64> {
        None
    }
    fn set_metadata_root(&mut self, _page: u64) {}
    fn options_byte(&self) -> u8 {
        0
    }
    fn reload(&mut self, _core: &mut StorageFile) -> Result<(), StorageError> {
        Ok(())
    }
    fn recovery_undo(&self) -> Box<dyn LogicalUndo> {
        Box::new(NoUndo)
    }
    fn capture_standby(
        &mut self,
        _core: &mut StorageFile,
        _records: &[(u64, RelRecord)],
        _floor: u64,
    ) {
    }
    fn finish_recovery(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn engines() -> Vec<Box<dyn EngineParticipant>> {
    vec![Box::new(NoCatalog), VecEngine::boxed()]
}

const RING: u64 = 8 * 1024 * 1024;

fn temp_path(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("truthdb-vector-{label}-{nanos}.db"))
}

fn create(path: &Path) -> Storage {
    Storage::create_with_wal_bounds(
        path.to_path_buf(),
        StorageOptions {
            size_gib: 1,
            wal_ratio: 0.05,
            metadata_ratio: 0.08,
            snapshot_ratio: 0.02,
            allocator_ratio: 0.02,
            reserved_ratio: 0.17,
            default_collation: None,
        },
        RING,
        RING,
        engines,
    )
    .expect("create")
}

fn open(path: &Path) -> Storage {
    Storage::open(path.to_path_buf(), engines).expect("open")
}

/// A deterministic pseudo-random stream in [-1, 1).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }

    fn vector(&mut self, dimension: usize) -> Vec<f32> {
        (0..dimension).map(|_| self.next()).collect()
    }
}

fn setup(storage: &Storage, metric: DistanceMetric, dimension: u16) -> IndexInfo {
    VecFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("vb1", "111111111111", 1)?.unwrap();
            tx.create_index(
                "vb1",
                &NewIndex {
                    name: "idx".into(),
                    dimension,
                    metric,
                    non_filterable: vec!["text".into()],
                },
                1,
            )
        })
        .unwrap()
        .unwrap()
}

/// Takes a checkpoint, as the engine does when the log fills (the tests run
/// several bounded transactions back to back on a small ring).
fn checkpoint(storage: &Storage) {
    storage.write_checkpoint(b"x", 1, 2, 1).expect("checkpoint");
}

/// Puts `vectors` over as many transactions as they take; returns how many.
fn put(storage: &Storage, index: &IndexInfo, vectors: &[VectorInput]) -> usize {
    let mut from = 0;
    let mut transactions = 0;
    while from < vectors.len() {
        from = VecFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_vectors(index, vectors, from))
            .unwrap()
            .unwrap();
        checkpoint(storage);
        transactions += 1;
    }
    transactions
}

fn query(
    storage: &Storage,
    index: &IndexInfo,
    vector: Vec<f32>,
    top_k: usize,
    filter: Option<Value>,
) -> Vec<(String, f32)> {
    let mut query = Query::new(index.clone(), vector, top_k, filter.as_ref()).unwrap();
    loop {
        let mut guard = storage.lock();
        let mut file = VecFile::new(&mut guard);
        if query.step(&mut file, 7).unwrap() {
            return query
                .finish(&mut file, false)
                .unwrap()
                .into_iter()
                .map(|hit| (hit.key, hit.distance))
                .collect();
        }
    }
}

fn keys(storage: &Storage, index: &IndexInfo) -> Vec<String> {
    VecFile::new(&mut storage.lock())
        .list_vectors(index, None, 10_000, false, false)
        .unwrap()
        .into_iter()
        .map(|entry| entry.key)
        .collect()
}

#[test]
fn vectors_are_kept_read_back_and_survive_a_crash() {
    let path = temp_path("basic");
    let storage = create(&path);
    let index = setup(&storage, DistanceMetric::Euclidean, 3);
    let vectors = vec![
        VectorInput {
            key: "a".into(),
            data: vec![1.0, 2.0, 3.0],
            metadata: Some(json!({"genre": "drama", "text": "long text"})),
        },
        VectorInput {
            key: "b".into(),
            data: vec![0.5, 0.0, -1.0],
            metadata: None,
        },
    ];
    assert_eq!(put(&storage, &index, &vectors), 1);
    drop(storage);
    let storage = open(&path);
    assert_eq!(
        VecFile::new(&mut storage.lock())
            .bucket("vb1")
            .unwrap()
            .unwrap()
            .owner
            .as_deref(),
        Some("111111111111")
    );
    let got = VecFile::new(&mut storage.lock())
        .get_vectors(
            &index,
            &["a".into(), "missing".into(), "b".into()],
            true,
            true,
        )
        .unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].data.as_deref(), Some(&[1.0, 2.0, 3.0][..]));
    assert_eq!(
        got[0].metadata.as_ref().unwrap()["text"],
        json!("long text")
    );
    assert_eq!(got[1].metadata.as_ref().unwrap().len(), 0);
    // A put of the same key replaces the vector.
    put(
        &storage,
        &index,
        &[VectorInput {
            key: "a".into(),
            data: vec![9.0, 9.0, 9.0],
            metadata: None,
        }],
    );
    assert_eq!(keys(&storage, &index), ["a", "b"]);
    let got = VecFile::new(&mut storage.lock())
        .get_vectors(&index, &["a".into()], true, false)
        .unwrap();
    assert_eq!(got[0].data.as_deref(), Some(&[9.0, 9.0, 9.0][..]));
    // Refusals: a wrong dimension, a value not finite, a bad key, metadata
    // past its limits; nothing of the batch is written.
    let refused = |vectors: Vec<VectorInput>| {
        VecFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_vectors(&index, &vectors, 0))
            .unwrap()
            .unwrap_err()
    };
    let good = VectorInput {
        key: "c".into(),
        data: vec![1.0, 1.0, 1.0],
        metadata: None,
    };
    for bad in [
        VectorInput {
            data: vec![1.0, 2.0],
            ..good.clone()
        },
        VectorInput {
            data: vec![f32::NAN, 0.0, 0.0],
            ..good.clone()
        },
        VectorInput {
            key: String::new(),
            ..good.clone()
        },
        VectorInput {
            metadata: Some(json!({"nested": {"x": 1}})),
            ..good.clone()
        },
    ] {
        assert!(matches!(
            refused(vec![good.clone(), bad]),
            VectorError::Invalid(_)
        ));
    }
    assert_eq!(keys(&storage, &index), ["a", "b"]);
    // Deleting vectors, then the bucket only once its index is gone.
    VecFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_vectors(&index, &["a".into(), "zz".into()], 0))
        .unwrap()
        .unwrap();
    assert_eq!(keys(&storage, &index), ["b"]);
    let delete_bucket = || {
        VecFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_bucket("vb1"))
            .unwrap()
    };
    assert_eq!(delete_bucket(), Err(VectorError::BucketNotEmpty));
    VecFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_index("vb1", "idx"))
        .unwrap()
        .unwrap();
    assert_eq!(delete_bucket(), Ok(()));
    while !VecFile::new(&mut storage.lock())
        .transaction(|tx| tx.purge_step())
        .unwrap()
    {}
    assert!(!VecFile::new(&mut storage.lock()).purge_pending().unwrap());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_search_is_exact_for_both_metrics_and_every_filter() {
    for metric in [DistanceMetric::Euclidean, DistanceMetric::Cosine] {
        let path = temp_path("exact");
        let storage = create(&path);
        let index = setup(&storage, metric, 16);
        let mut rng = Rng(7);
        let vectors: Vec<VectorInput> = (0..300)
            .map(|n| VectorInput {
                key: format!("v{n:03}"),
                data: rng.vector(16),
                metadata: Some(json!({"group": n % 3, "tag": format!("t{}", n % 5)})),
            })
            .collect();
        put(&storage, &index, &vectors);
        let probe = rng.vector(16);
        let reference = |filter: &dyn Fn(usize) -> bool| -> Vec<String> {
            let dot = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
            let mut all: Vec<(f32, String)> = vectors
                .iter()
                .enumerate()
                .filter(|(n, _)| filter(*n))
                .map(|(_, v)| {
                    let distance = match metric {
                        DistanceMetric::Euclidean => v
                            .data
                            .iter()
                            .zip(&probe)
                            .map(|(a, b)| (a - b) * (a - b))
                            .sum::<f32>()
                            .sqrt(),
                        DistanceMetric::Cosine => {
                            1.0 - dot(&v.data, &probe)
                                / (dot(&v.data, &v.data).sqrt() * dot(&probe, &probe).sqrt())
                        }
                    };
                    (distance, v.key.clone())
                })
                .collect();
            all.sort_by(|a, b| a.0.total_cmp(&b.0));
            all.into_iter().take(10).map(|(_, key)| key).collect()
        };
        let found = |filter: Option<Value>| -> Vec<String> {
            query(&storage, &index, probe.clone(), 10, filter)
                .into_iter()
                .map(|(key, _)| key)
                .collect()
        };
        assert_eq!(found(None), reference(&|_| true), "{metric:?}");
        assert_eq!(found(Some(json!({"group": 1}))), reference(&|n| n % 3 == 1));
        assert_eq!(
            found(Some(json!({"$or": [{"tag": "t2"}, {"group": {"$gt": 1}}]}))),
            reference(&|n| n % 5 == 2 || n % 3 > 1)
        );
        assert_eq!(
            found(Some(
                json!({"tag": {"$nin": ["t0", "t1"]}, "group": {"$ne": 0}})
            )),
            reference(&|n| n % 5 >= 2 && n % 3 != 0)
        );
        // Distances come nearest first.
        let hits = query(&storage, &index, probe.clone(), 300, None);
        assert_eq!(hits.len(), 300);
        assert!(hits.windows(2).all(|pair| pair[0].1 <= pair[1].1));
        drop(storage);
        std::fs::remove_file(&path).ok();
    }
}

#[test]
fn a_crash_at_any_op_of_a_put_leaves_it_whole_or_absent() {
    let mut crash_at = 0;
    loop {
        let path = temp_path("crash");
        let storage = create(&path);
        let index = setup(&storage, DistanceMetric::Euclidean, 1000);
        put(
            &storage,
            &index,
            &[VectorInput {
                key: "k".into(),
                data: vec![1.0; 1000],
                metadata: Some(json!({"v": 1})),
            }],
        );
        let replacement = vec![
            VectorInput {
                key: "k".into(),
                data: vec![2.0; 1000],
                metadata: Some(json!({"v": 2, "text": "x".repeat(5000)})),
            },
            VectorInput {
                key: "new".into(),
                data: vec![3.0; 1000],
                metadata: None,
            },
        ];
        FAIL_APPLY_OPS_AFTER.with(|fail| fail.set(Some(crash_at)));
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            VecFile::new(&mut storage.lock()).transaction(|tx| {
                // The failed op stands for the crash: no rollback runs.
                Ok(tx
                    .put_vectors(&index, &replacement, 0)
                    .unwrap_or_else(|err| panic!("crash: {err}")))
            })
        }))
        .is_err();
        let fired = FAIL_APPLY_OPS_AFTER
            .with(|fail| fail.replace(None))
            .is_none();
        drop(storage);
        let storage = open(&path);
        let got = VecFile::new(&mut storage.lock())
            .get_vectors(&index, &["k".into(), "new".into()], true, true)
            .unwrap();
        if crashed {
            assert!(fired);
            assert_eq!(got.len(), 1, "crash at {crash_at}");
            assert_eq!(got[0].data.as_deref(), Some(&vec![1.0; 1000][..]));
            assert_eq!(got[0].metadata.as_ref().unwrap()["v"], json!(1));
        } else {
            assert_eq!(got.len(), 2);
            assert_eq!(got[0].data.as_deref(), Some(&vec![2.0; 1000][..]));
        }
        let hits = query(&storage, &index, vec![1.0; 1000], 5, None);
        assert_eq!(hits.len(), got.len(), "crash at {crash_at}");
        drop(storage);
        std::fs::remove_file(&path).ok();
        if !crashed {
            assert!(crash_at > 10, "the put crashed at only {crash_at} points");
            break;
        }
        crash_at += 1;
    }
}

#[test]
fn a_large_batch_goes_over_several_transactions_and_a_deleted_index_purges_in_steps() {
    let path = temp_path("budget");
    let storage = create(&path);
    let index = setup(&storage, DistanceMetric::Cosine, 4096);
    let mut rng = Rng(3);
    // 500 vectors of 4096 dimensions with 20 KB of metadata each: far more
    // log than one transaction may write on an 8 MiB ring.
    let vectors: Vec<VectorInput> = (0..500)
        .map(|n| VectorInput {
            key: format!("k{n}"),
            data: rng.vector(4096),
            metadata: Some(json!({"n": n, "text": "y".repeat(20_000)})),
        })
        .collect();
    let transactions = put(&storage, &index, &vectors);
    assert!(transactions > 5, "{transactions} transactions");
    assert_eq!(keys(&storage, &index).len(), 500);
    // Deleted: the name is free at once; the vectors go a step at a time.
    VecFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_index("vb1", "idx"))
        .unwrap()
        .unwrap();
    let again = VecFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_index(
                "vb1",
                &NewIndex {
                    name: "idx".into(),
                    dimension: 4096,
                    metric: DistanceMetric::Cosine,
                    non_filterable: Vec::new(),
                },
                2,
            )
        })
        .unwrap()
        .unwrap();
    assert_ne!(again.id, index.id);
    assert!(keys(&storage, &again).is_empty());
    let mut steps = 0;
    while !VecFile::new(&mut storage.lock())
        .transaction(|tx| tx.purge_step())
        .unwrap()
    {
        checkpoint(&storage);
        steps += 1;
    }
    assert!(steps > 5, "{steps} purge steps");
    assert!(keys(&storage, &index).is_empty());
    drop(storage);
    let storage = open(&path);
    assert!(!VecFile::new(&mut storage.lock()).purge_pending().unwrap());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_query_leaves_out_vectors_replaced_or_deleted_while_it_ran() {
    let path = temp_path("concurrent");
    let storage = create(&path);
    let index = setup(&storage, DistanceMetric::Euclidean, 2);
    let vectors: Vec<VectorInput> = (0..20)
        .map(|n| VectorInput {
            key: format!("k{n:02}"),
            data: vec![n as f32, 0.0],
            metadata: None,
        })
        .collect();
    put(&storage, &index, &vectors);
    let mut query = Query::new(index.clone(), vec![0.0, 0.0], 3, None).unwrap();
    {
        let mut guard = storage.lock();
        assert!(!query.step(&mut VecFile::new(&mut guard), 10).unwrap());
    }
    // k00 moves far away and k01 goes, after the query read them.
    put(
        &storage,
        &index,
        &[VectorInput {
            key: "k00".into(),
            data: vec![100.0, 0.0],
            metadata: None,
        }],
    );
    VecFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_vectors(&index, &["k01".into()], 0))
        .unwrap()
        .unwrap();
    let hits = loop {
        let mut guard = storage.lock();
        let mut file = VecFile::new(&mut guard);
        if query.step(&mut file, 10).unwrap() {
            break query.finish(&mut file, false).unwrap();
        }
    };
    // The three nearest read were k00, k01 and k02; the first two changed
    // since, so only k02 is returned (the moved k00, read later, was not
    // among the nearest).
    let keys: Vec<&str> = hits.iter().map(|hit| hit.key.as_str()).collect();
    assert_eq!(keys, ["k02"]);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn the_engine_owns_its_own_object_ids_only() {
    let engine = VecEngine::default();
    assert!(engine.owns_object(FIRST_ENGINE_OBJECT_ID + 0x1000));
    assert!(engine.owns_object(FIRST_ENGINE_OBJECT_ID + 0x1FFF));
    assert!(!engine.owns_object(FIRST_ENGINE_OBJECT_ID + 0xFFF));
    assert!(!engine.owns_object(FIRST_ENGINE_OBJECT_ID + 0x2000));
    assert!(!engine.owns_object(FIRST_ENGINE_OBJECT_ID));
}

#[test]
fn vectors_ship_to_a_standby_and_restore_from_a_log_backup() {
    let path = temp_path("ship-src");
    let bak = temp_path("ship-bak");
    let log = temp_path("ship-log");
    let seed = temp_path("ship-seed");
    let dst = temp_path("ship-dst");
    let primary = create(&path);
    primary.lock().commit_db_options(0, Some(true)).unwrap();
    let summary = primary.backup_full(&bak).unwrap();
    let index = setup(&primary, DistanceMetric::Euclidean, 4);
    let mut rng = Rng(11);
    let vectors: Vec<VectorInput> = (0..50)
        .map(|n| VectorInput {
            key: format!("k{n}"),
            data: rng.vector(4),
            metadata: Some(json!({"n": n})),
        })
        .collect();
    put(&primary, &index, &vectors);
    VecFile::new(&mut primary.lock())
        .transaction(|tx| tx.delete_vectors(&index, &["k7".into()], 0))
        .unwrap()
        .unwrap();
    let probe = rng.vector(4);
    let expected = query(
        &primary,
        &index,
        probe.clone(),
        5,
        Some(json!({"n": {"$lt": 30}})),
    );
    assert_eq!(expected.len(), 5);
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, primary.wal_flushed_lsn())
        .unwrap();
    primary.backup_log(&log, true, false).unwrap();

    Storage::restore_full_standby(&seed, &bak, &[], engines).unwrap();
    let standby = open(&seed);
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .unwrap();
    assert_eq!(
        query(
            &standby,
            &index,
            probe.clone(),
            5,
            Some(json!({"n": {"$lt": 30}}))
        ),
        expected
    );
    assert_eq!(keys(&standby, &index).len(), 49);
    drop(standby);

    Storage::restore_full_with_logs(&dst, &bak, &[log.clone()], None, engines).unwrap();
    let restored = open(&dst);
    assert_eq!(
        query(&restored, &index, probe, 5, Some(json!({"n": {"$lt": 30}}))),
        expected
    );
    drop(restored);
    drop(primary);
    for p in [&path, &bak, &log, &seed, &dst] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn cosine_and_euclidean_distances_hold_for_extreme_magnitudes() {
    let path = temp_path("extreme");
    let storage = create(&path);
    let index = setup(&storage, DistanceMetric::Cosine, 2);
    let vector = |key: &str, data: [f32; 2]| VectorInput {
        key: key.into(),
        data: data.to_vec(),
        metadata: None,
    };
    put(
        &storage,
        &index,
        &[
            vector("exact", [1.0, 1.0]),
            vector("tiny", [1e-30, 0.0]),
            vector("huge", [3e38, 3e38]),
            vector("opposite", [-1.0, -1.0]),
        ],
    );
    let hits = query(&storage, &index, vec![1.0, 1.0], 4, None);
    let keys: Vec<&str> = hits.iter().map(|(key, _)| key.as_str()).collect();
    assert!(
        keys[..2].contains(&"exact") && keys[..2].contains(&"huge"),
        "{hits:?}"
    );
    assert_eq!(&keys[2..], ["tiny", "opposite"]);
    assert!(
        hits.iter().all(|(_, distance)| distance.is_finite()),
        "{hits:?}"
    );
    assert!((hits[2].1 - (1.0 - std::f32::consts::FRAC_1_SQRT_2)).abs() < 1e-6);
    assert!((hits[3].1 - 2.0).abs() < 1e-6);
    let hits = query(&storage, &index, vec![3e19, 0.0], 4, None);
    assert_eq!(hits[0].0, "tiny");
    assert!(hits[0].1.abs() < 1e-6, "{hits:?}");
    drop(storage);
    std::fs::remove_file(&path).ok();
}
