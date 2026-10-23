//! The streams engine on a real store: topics, appends and reads survive a
//! restart, a source appends each sequence number once (even across a crash
//! at any op), retention trims and dropped topics are purged, and the
//! engine ships to a standby and restores from a log backup.

use std::path::{Path, PathBuf};

use truthdb_core::relstore::ctx::{FAIL_APPLY_OPS_AFTER, OpMode, RelCtx};
use truthdb_core::relstore::recovery::LogicalUndo;
use truthdb_core::storage::{
    CATALOG_ENGINE_ID, EngineParticipant, Storage, StorageError, StorageFile, StorageOptions,
};
use truthdb_core::wal::records::{PageOpUndo, RelRecord};

use crate::{NewRecord, StreamEngine, StreamError, StreamFile};

/// The catalog slot with nothing in it: the streams engine runs alone.
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
    vec![Box::new(NoCatalog), StreamEngine::boxed()]
}

const RING: u64 = 8 * 1024 * 1024;

fn temp_path(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("truthdb-streams-{label}-{nanos}.db"))
}

fn create(path: &Path) -> Storage {
    Storage::create_with_wal_bounds(
        path.to_path_buf(),
        StorageOptions {
            size_bytes: 64 << 20,
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

/// Runs `body` as one transaction.
fn in_txn<T>(
    storage: &Storage,
    body: impl FnOnce(&mut crate::StreamTxn<'_, '_>) -> Result<T, StorageError>,
) -> T {
    StreamFile::new(&mut storage.lock())
        .transaction(body)
        .unwrap()
}

/// The rows of the group-offsets tree, of every topic.
fn group_offset_rows(storage: &Storage) -> usize {
    let mut guard = storage.lock();
    let mut file = StreamFile::new(&mut guard);
    let Some(tree) = file.existing_tree(crate::GROUP_OFFSETS_TREE).unwrap() else {
        return 0;
    };
    tree.scan(&mut file.page_ctx()).unwrap().len()
}

fn record(value: &str) -> NewRecord {
    NewRecord {
        key: None,
        value: value.as_bytes().to_vec(),
    }
}

fn values(storage: &Storage, topic: &str, partition: u32, from: u64) -> Vec<(u64, String)> {
    StreamFile::new(&mut storage.lock())
        .read(topic, partition, from, 1000, usize::MAX)
        .unwrap()
        .unwrap()
        .0
        .into_iter()
        .map(|record| (record.offset, String::from_utf8(record.value).unwrap()))
        .collect()
}

#[test]
fn topics_take_appends_in_order_once_per_source_and_go_when_dropped() {
    let path = temp_path("topics");
    let storage = create(&path);
    assert_eq!(
        in_txn(&storage, |tx| tx
            .create_topic("bad name", 1, 0, None, 1)
            .map(|r| r.map(|_| ()))),
        Err(StreamError::InvalidName(
            "a topic name is letters, digits, '.', '_' and '-'"
        ))
    );
    assert_eq!(
        in_txn(&storage, |tx| tx
            .create_topic("t", 0, 0, None, 1)
            .map(|r| r.map(|_| ()))),
        Err(StreamError::InvalidPartitions)
    );
    let topic = in_txn(&storage, |tx| {
        tx.create_topic("events", 2, 0, Some("123456789012"), 1_000)
    })
    .unwrap();
    assert_eq!((topic.name.as_str(), topic.partitions), ("events", 2));
    assert_eq!(
        in_txn(&storage, |tx| tx
            .create_topic("events", 1, 0, None, 1)
            .map(|r| r.map(|_| ()))),
        Err(StreamError::TopicExists)
    );
    // Offsets are contiguous per partition; timestamps never go back.
    let appended = in_txn(&storage, |tx| {
        tx.append("events", 0, &[record("a"), record("b")], None, 5_000)
    })
    .unwrap();
    assert_eq!((appended.offset, appended.written), (Some(0), true));
    in_txn(&storage, |tx| {
        tx.append("events", 0, &[record("c")], None, 4_000)
    })
    .unwrap();
    in_txn(&storage, |tx| {
        tx.append("events", 1, &[record("x")], None, 6_000)
    })
    .unwrap();
    assert_eq!(
        values(&storage, "events", 0, 0),
        [(0, "a".into()), (1, "b".into()), (2, "c".into())]
    );
    assert_eq!(values(&storage, "events", 0, 2), [(2, "c".into())]);
    assert_eq!(values(&storage, "events", 1, 0), [(0, "x".into())]);
    let (read, state) = StreamFile::new(&mut storage.lock())
        .read("events", 0, 0, 10, 1)
        .unwrap()
        .unwrap();
    assert_eq!(read.len(), 1, "max bytes, past the first record");
    assert_eq!((state.start, state.next, state.last_ms), (0, 3, 5_000));
    assert_eq!(read[0].timestamp_ms, 5_000);
    let at = |time: u64| {
        StreamFile::new(&mut storage.lock())
            .offset_for_time("events", 0, time)
            .unwrap()
            .unwrap()
    };
    assert_eq!((at(0), at(5_000), at(5_001)), (0, 0, 3));
    assert_eq!(
        in_txn(&storage, |tx| tx.append(
            "events",
            2,
            &[record("z")],
            None,
            1
        )),
        Err(StreamError::InvalidPartition)
    );
    assert_eq!(
        in_txn(&storage, |tx| tx.append(
            "missing",
            0,
            &[record("z")],
            None,
            1
        )),
        Err(StreamError::NoSuchTopic)
    );
    let large = NewRecord {
        key: None,
        value: vec![0; crate::store::MAX_RECORD_BYTES + 1],
    };
    assert_eq!(
        in_txn(&storage, |tx| tx.append(
            "events",
            0,
            std::slice::from_ref(&large),
            None,
            1
        )),
        Err(StreamError::TooLarge)
    );
    // A source appends each sequence number once.
    let by_source = |seq: u64, value: &str| {
        StreamFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.append("events", 1, &[record(value)], Some(("journal", seq)), 7_000)
            })
            .unwrap()
            .unwrap()
    };
    let appended = |seq: u64, value: &str| {
        let appended = by_source(seq, value);
        (appended.offset, appended.written)
    };
    assert_eq!(appended(10, "s10"), (Some(1), true));
    assert_eq!(
        appended(10, "again"),
        (Some(1), false),
        "the original offset"
    );
    assert_eq!(appended(9, "older"), (None, false));
    assert_eq!(appended(11, "s11"), (Some(2), true));
    assert_eq!(
        values(&storage, "events", 1, 0),
        [(0, "x".into()), (1, "s10".into()), (2, "s11".into())]
    );
    // Group offsets (more groups than a purge step removes).
    for group in ["readers", "a", "b", "c"] {
        in_txn(&storage, |tx| tx.commit_offset(group, "events", 1, 2)).unwrap();
    }
    assert_eq!(
        StreamFile::new(&mut storage.lock())
            .committed_offset("readers", "events", 1)
            .unwrap(),
        Some(2)
    );
    // All of it survives a restart.
    drop(storage);
    let storage = open(&path);
    assert_eq!(values(&storage, "events", 1, 0).len(), 3);
    let (topic, _) = StreamFile::new(&mut storage.lock())
        .topic("events")
        .unwrap()
        .unwrap();
    assert_eq!(topic.account.as_deref(), Some("123456789012"));
    assert!(
        !StreamFile::new(&mut storage.lock())
            .transaction(|tx| tx.append("events", 1, &[record("x")], Some(("journal", 11)), 1))
            .unwrap()
            .unwrap()
            .written
    );
    // Dropped: gone at once, its records purged in steps.
    StreamFile::new(&mut storage.lock())
        .transaction(|tx| tx.drop_topic("events"))
        .unwrap()
        .unwrap();
    assert!(
        StreamFile::new(&mut storage.lock())
            .topic("events")
            .unwrap()
            .is_none()
    );
    assert!(
        StreamFile::new(&mut storage.lock())
            .purge_pending()
            .unwrap()
    );
    let mut steps = 0;
    while !StreamFile::new(&mut storage.lock())
        .transaction(|tx| tx.purge_step(2))
        .unwrap()
    {
        steps += 1;
        assert!(steps < 100);
    }
    assert!(steps >= 2, "{steps} steps");
    assert_eq!(
        group_offset_rows(&storage),
        0,
        "its groups' offsets went too"
    );
    assert!(
        !StreamFile::new(&mut storage.lock())
            .purge_pending()
            .unwrap()
    );
    // A topic of the same name starts afresh.
    StreamFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_topic("events", 1, 0, None, 1))
        .unwrap()
        .unwrap();
    assert!(values(&storage, "events", 0, 0).is_empty());
    assert!(
        StreamFile::new(&mut storage.lock())
            .transaction(|tx| tx.append("events", 0, &[record("new")], Some(("journal", 1)), 1))
            .unwrap()
            .unwrap()
            .written
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn retention_trims_the_oldest_records() {
    let path = temp_path("retention");
    let storage = create(&path);
    StreamFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_topic("short", 1, 1_000, None, 0)?.unwrap();
            tx.create_topic("forever", 1, 0, None, 0)?.unwrap();
            for (time, value) in [(1_000, "a"), (2_000, "b"), (3_000, "c")] {
                tx.append("short", 0, &[record(value)], None, time)?
                    .unwrap();
                tx.append("forever", 0, &[record(value)], None, time)?
                    .unwrap();
            }
            Ok(())
        })
        .unwrap();
    let due = |now| StreamFile::new(&mut storage.lock()).trim_due(now).unwrap();
    assert!(!due(1_500), "nothing older than the retention yet");
    assert!(due(3_500));
    let trimmed = StreamFile::new(&mut storage.lock())
        .transaction(|tx| tx.trim_step(3_500, 1))
        .unwrap();
    assert_eq!(trimmed, (1, true), "the limit");
    let trimmed = StreamFile::new(&mut storage.lock())
        .transaction(|tx| tx.trim_step(3_500, 100))
        .unwrap();
    assert_eq!(trimmed, (1, false));
    assert_eq!(values(&storage, "short", 0, 0), [(2, "c".into())]);
    assert_eq!(values(&storage, "forever", 0, 0).len(), 3);
    let state = StreamFile::new(&mut storage.lock())
        .topic("short")
        .unwrap()
        .unwrap()
        .1[0];
    assert_eq!((state.start, state.next), (2, 3));
    assert!(!due(3_500), "trimmed up to the retention");
    assert_eq!(
        StreamFile::new(&mut storage.lock())
            .transaction(|tx| tx.trim_step(3_500, 100))
            .unwrap(),
        (0, false)
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_sourced_append_is_whole_or_absent_after_a_crash_at_any_op() {
    let mut crash_at = 0;
    let mut crashes = 0;
    loop {
        let path = temp_path("crash");
        let storage = create(&path);
        StreamFile::new(&mut storage.lock())
            .transaction(|tx| tx.create_topic("events", 1, 0, None, 1))
            .unwrap()
            .unwrap();
        let big = NewRecord {
            key: Some(b"k".to_vec()),
            value: vec![9; 4_000],
        };
        FAIL_APPLY_OPS_AFTER.with(|fail| fail.set(Some(crash_at)));
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            StreamFile::new(&mut storage.lock())
                .transaction(|tx| {
                    Ok(tx
                        .append(
                            "events",
                            0,
                            &[record("a"), big.clone()],
                            Some(("src", 1)),
                            5,
                        )
                        .unwrap_or_else(|err| panic!("crash: {err:?}"))
                        .unwrap())
                })
                .unwrap_or_else(|err| panic!("crash: {err}"))
        }))
        .is_err();
        let fired = FAIL_APPLY_OPS_AFTER
            .with(|fail| fail.replace(None))
            .is_none();
        drop(storage);
        let storage = open(&path);
        let held = values(&storage, "events", 0, 0).len();
        if crashed {
            assert!(fired);
            crashes += 1;
        }
        assert!(
            held == 0 || held == 2,
            "crash at {crash_at}: {held} records"
        );
        // Appended again, it lands exactly once.
        StreamFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.append(
                    "events",
                    0,
                    &[record("a"), big.clone()],
                    Some(("src", 1)),
                    6,
                )
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            values(&storage, "events", 0, 0).len(),
            2,
            "crash at {crash_at}"
        );
        drop(storage);
        std::fs::remove_file(&path).ok();
        if !crashed {
            assert!(crashes >= 5, "the append crashed at only {crashes} points");
            break;
        }
        crash_at += 1;
    }
}

#[test]
fn topics_ship_to_a_standby_and_restore_from_a_log_backup() {
    let path = temp_path("ship-src");
    let bak = temp_path("ship-bak");
    let log = temp_path("ship-log");
    let seed = temp_path("ship-seed");
    let dst = temp_path("ship-dst");
    let primary = create(&path);
    primary.lock().commit_db_options(0, Some(true)).unwrap();
    let summary = primary.backup_full(&bak).unwrap();
    StreamFile::new(&mut primary.lock())
        .transaction(|tx| {
            tx.create_topic("events", 1, 0, None, 1)?.unwrap();
            tx.append(
                "events",
                0,
                &[record("a"), record("b")],
                Some(("src", 3)),
                9,
            )?
            .unwrap();
            tx.commit_offset("g", "events", 0, 1)?.unwrap();
            Ok(())
        })
        .unwrap();
    let expected = values(&primary, "events", 0, 0);
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, primary.wal_flushed_lsn())
        .unwrap();
    primary.backup_log(&log, true, false).unwrap();
    Storage::restore_full_standby(&seed, &bak, &[], engines).unwrap();
    let standby = open(&seed);
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .unwrap();
    assert_eq!(values(&standby, "events", 0, 0), expected);
    drop(standby);
    Storage::restore_full_with_logs(&dst, &bak, &[log.clone()], None, engines).unwrap();
    let restored = open(&dst);
    assert_eq!(values(&restored, "events", 0, 0), expected);
    assert_eq!(
        StreamFile::new(&mut restored.lock())
            .committed_offset("g", "events", 0)
            .unwrap(),
        Some(1)
    );
    drop(restored);
    drop(primary);
    for p in [&path, &bak, &log, &seed, &dst] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn a_trim_stopped_by_the_log_budget_says_more_is_due() {
    let path = temp_path("trim-budget");
    let storage = create(&path);
    in_txn(&storage, |tx| tx.create_topic("big", 1, 1_000, None, 0)).unwrap();
    let value = vec![7u8; 16 * 1024];
    for _ in 0..16 {
        let records = vec![
            NewRecord {
                key: None,
                value: value.clone(),
            };
            8
        ];
        in_txn(&storage, |tx| tx.append("big", 0, &records, None, 1)).unwrap();
        // As the engine does when the log fills.
        storage.write_checkpoint(b"x", 1, 2, 1).expect("checkpoint");
    }
    let mut steps = 0;
    let mut removed = 0;
    loop {
        let (count, more) = in_txn(&storage, |tx| tx.trim_step(5_000, usize::MAX));
        storage.write_checkpoint(b"x", 1, 2, 1).expect("checkpoint");
        removed += count;
        steps += 1;
        if !more {
            break;
        }
        assert!(count > 0 && steps < 100, "{steps} steps");
    }
    assert_eq!(removed, 128);
    assert!(steps >= 2, "the budget stopped a step: {steps} steps");
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn the_largest_append_fits_a_transactions_budget_on_the_smallest_ring() {
    let path = temp_path("largest-append");
    let storage = Storage::create_with_wal_bounds(
        path.to_path_buf(),
        StorageOptions {
            size_bytes: 1 << 30,
            wal_ratio: 0.25,
            metadata_ratio: 0.08,
            snapshot_ratio: 0.02,
            allocator_ratio: 0.02,
            reserved_ratio: 0.17,
            default_collation: None,
        },
        truthdb_core::storage_layout::WAL_MIN_BYTES,
        truthdb_core::storage_layout::WAL_MIN_BYTES,
        engines,
    )
    .expect("create");
    in_txn(&storage, |tx| tx.create_topic("big", 1, 0, None, 0)).unwrap();
    let largest = NewRecord {
        key: None,
        value: vec![7u8; crate::MAX_RECORD_BYTES],
    };
    let empty = NewRecord {
        key: None,
        value: Vec::new(),
    };
    // Two of the largest records, or as many of the smallest as chunks.
    for (record, count) in [(largest, 2), (empty, crate::MAX_APPEND_CHUNKS)] {
        let mut records = vec![record; count + 1];
        assert_eq!(
            in_txn(&storage, |tx| tx.append("big", 0, &records, None, 1)),
            Err(StreamError::TooLarge),
            "one more record than fits"
        );
        records.pop();
        let (written, budget) = in_txn(&storage, |tx| {
            let start = tx.log_tail();
            tx.append("big", 0, &records, None, 1)?.unwrap();
            Ok((tx.log_since(start), tx.log_budget()))
        });
        assert!(
            written <= budget,
            "{count} records: {written} bytes of log, budget {budget}"
        );
        storage.write_checkpoint(b"x", 1, 2, 1).expect("checkpoint");
    }
    drop(storage);
    std::fs::remove_file(&path).ok();
}
