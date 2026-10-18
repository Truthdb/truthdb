use std::path::{Path, PathBuf};

use md5::{Digest, Md5};
use truthdb_core::relstore::ctx::{FAIL_APPLY_OPS_AFTER, OpMode, RelCtx};
use truthdb_core::relstore::recovery::LogicalUndo;
use truthdb_core::storage::{
    CATALOG_ENGINE_ID, EngineParticipant, Storage, StorageError, StorageFile, StorageOptions,
};
use truthdb_core::wal::records::{PageOpUndo, RelRecord};

use crate::body::SEGMENT_BYTES;
use crate::{BodyLease, BodyWriter, ObjEngine, ObjFile};

const RING: u64 = 8 * 1024 * 1024;
const GRACE: u64 = 1_000;
const ABANDON: u64 = 60_000;

/// The catalog slot with nothing in it: the object engine runs alone.
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
    vec![Box::new(NoCatalog), ObjEngine::boxed()]
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("truthdb-object-{label}-{nanos}.db"))
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

/// Bytes that differ at every offset, `len` long.
fn bytes(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            seed.wrapping_add((i % 251) as u8)
                .wrapping_add((i / 251) as u8)
        })
        .collect()
}

fn put(storage: &Storage, data: &[u8], now_ms: u64) -> u64 {
    let mut writer = BodyWriter::begin(storage, now_ms).expect("begin");
    for chunk in data.chunks(300_000) {
        writer.write(storage, chunk, now_ms).expect("write");
    }
    let sealed = writer.finish(storage, now_ms).expect("finish");
    let info = ObjFile::new(&mut storage.lock())
        .transaction(|tx| sealed.seal_in(tx))
        .expect("seal");
    assert_eq!(info.len, data.len() as u64);
    assert_eq!(info.md5, <[u8; 16]>::from(Md5::digest(data)));
    info.body
}

fn read(storage: &Storage, body: u64, offset: u64, len: usize) -> Result<Vec<u8>, StorageError> {
    let mut out = vec![0u8; len];
    ObjFile::new(&mut storage.lock()).read_body(body, offset, &mut out)?;
    Ok(out)
}

fn allocated_pages(storage: &Storage) -> u64 {
    let guard = storage.lock();
    (0..guard.data_pages())
        .filter(|&page| guard.is_page_allocated(page))
        .count() as u64
}

fn now() -> u64 {
    10_000_000
}

#[test]
fn a_body_reads_back_whole_and_by_range_across_segments_and_a_crash() {
    let path = temp_path("roundtrip");
    let storage = create(&path);
    let data = bytes(1, 2 * SEGMENT_BYTES + 12_345);
    let body = put(&storage, &data, now());
    assert_eq!(read(&storage, body, 0, data.len()).unwrap(), data);
    let at = SEGMENT_BYTES as u64 - 10;
    assert_eq!(
        read(&storage, body, at, 30).unwrap(),
        data[at as usize..at as usize + 30],
        "a range across a segment boundary"
    );
    assert!(
        read(&storage, body, data.len() as u64 - 5, 6).is_err(),
        "past the end"
    );
    drop(storage); // crash

    let storage = open(&path);
    assert_eq!(read(&storage, body, 0, data.len()).unwrap(), data);
    let info = ObjFile::new(&mut storage.lock()).body_info(body).unwrap();
    assert_eq!(info.md5, <[u8; 16]>::from(Md5::digest(&data)));
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn an_empty_body_seals_and_reads_back() {
    let path = temp_path("empty");
    let storage = create(&path);
    let body = put(&storage, &[], now());
    assert_eq!(read(&storage, body, 0, 0).unwrap(), Vec::<u8>::new());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_dropped_body_is_freed_after_its_grace_unless_leased() {
    let path = temp_path("gc");
    let storage = create(&path);
    let baseline = allocated_pages(&storage);
    let data = bytes(2, SEGMENT_BYTES + 100);
    let body = put(&storage, &data, now());
    let with_body = allocated_pages(&storage);
    assert!(with_body > baseline);

    let collect = |at: u64| {
        ObjFile::new(&mut storage.lock())
            .collect_garbage(at, ABANDON, u64::MAX)
            .unwrap()
            .bodies
    };
    let in_tx = |op: &dyn Fn(&mut crate::ObjTxn<'_, '_>) -> Result<(), StorageError>| {
        ObjFile::new(&mut storage.lock()).transaction(op).unwrap()
    };
    in_tx(&|tx| tx.add_ref(body));
    in_tx(&|tx| tx.drop_ref(body, now(), GRACE));
    assert_eq!(collect(now() + 2 * GRACE), 0, "still referenced");
    {
        // A reader leases the body while it is live; the last reference
        // drops while it reads.
        let _lease = BodyLease::take(&storage, body).unwrap();
        in_tx(&|tx| tx.drop_ref(body, now(), GRACE));
        assert_eq!(collect(now()), 0, "not due yet");
        assert_eq!(collect(now() + GRACE), 0, "leased");
    }
    assert!(
        BodyLease::take(&storage, body).is_err(),
        "a body without references cannot be leased"
    );
    assert_eq!(collect(now() + GRACE), 1, "the lease dropped");
    let after = allocated_pages(&storage);
    drop(storage);
    let storage = open(&path);
    assert_eq!(allocated_pages(&storage), after, "the frees are durable");
    assert!(read(&storage, body, 0, 1).is_err(), "the body is gone");
    // Only the engine's tree pages stay allocated.
    assert!(after < with_body, "{after} < {with_body}");
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_body_abandoned_mid_write_is_collected_after_a_crash() {
    let path = temp_path("abandon");
    let storage = create(&path);
    let kept = put(&storage, &bytes(3, 1000), now());
    // Collect once so every tree the engine uses exists before the baseline.
    ObjFile::new(&mut storage.lock())
        .collect_garbage(now(), ABANDON, u64::MAX)
        .unwrap();
    let before = allocated_pages(&storage);
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    writer
        .write(&storage, &bytes(4, SEGMENT_BYTES + 10), now())
        .unwrap();
    let abandoned = writer.body();
    drop(writer);
    drop(storage); // crash mid-upload

    let storage = open(&path);
    assert!(
        allocated_pages(&storage) > before,
        "the written segment survives"
    );
    let freed = ObjFile::new(&mut storage.lock())
        .collect_garbage(now() + ABANDON, ABANDON, u64::MAX)
        .unwrap();
    assert_eq!(freed.bodies, 1);
    assert_eq!(allocated_pages(&storage), before, "nothing leaked");
    assert!(read(&storage, abandoned, 0, 1).is_err());
    assert_eq!(read(&storage, kept, 0, 1000).unwrap(), bytes(3, 1000));
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_segment_that_fails_midway_rolls_back_its_pages() {
    let path = temp_path("fail");
    let storage = create(&path);
    // A first body creates every tree a segment write touches.
    put(&storage, &bytes(4, 100), now());
    let body = ObjFile::new(&mut storage.lock()).begin_body(now()).unwrap();
    let before = allocated_pages(&storage);
    // The segment row's insert is the first page op after the pages are
    // allocated and written: fail it.
    FAIL_APPLY_OPS_AFTER.with(|fail| fail.set(Some(0)));
    let err = ObjFile::new(&mut storage.lock()).append_segment(body, &bytes(5, 5000), now());
    FAIL_APPLY_OPS_AFTER.with(|fail| fail.set(None));
    assert!(err.is_err());
    assert_eq!(
        allocated_pages(&storage),
        before,
        "the rollback freed the pages"
    );
    {
        let mut guard = storage.lock();
        let mut file = ObjFile::new(&mut guard);
        file.append_segment(body, &bytes(5, 5000), now()).unwrap();
        file.transaction(|tx| tx.seal_body(body, [0; 16])).unwrap();
    }
    assert_eq!(read(&storage, body, 0, 5000).unwrap(), bytes(5, 5000));
    drop(storage);
    let storage = open(&path);
    assert_eq!(read(&storage, body, 0, 5000).unwrap(), bytes(5, 5000));
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_body_ships_to_a_standby_and_restores_from_a_log_backup() {
    let path = temp_path("ship-src");
    let bak = temp_path("ship-bak");
    let log = temp_path("ship-log");
    let seed = temp_path("ship-seed");
    let dst = temp_path("ship-dst");
    let primary = create(&path);
    primary.lock().commit_db_options(0, Some(true)).unwrap();
    let summary = primary.backup_full(&bak).unwrap();
    let data = bytes(6, SEGMENT_BYTES + 777);
    let body = put(&primary, &data, now());
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, primary.wal_flushed_lsn())
        .unwrap();
    primary.backup_log(&log, true, false).unwrap();

    Storage::restore_full_standby(&seed, &bak, &[], engines).unwrap();
    let standby = open(&seed);
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .unwrap();
    assert_eq!(read(&standby, body, 0, data.len()).unwrap(), data);
    drop(standby);

    Storage::restore_full_with_logs(&dst, &bak, &[log.clone()], None, engines).unwrap();
    let restored = open(&dst);
    assert_eq!(read(&restored, body, 0, data.len()).unwrap(), data);
    drop(restored);
    drop(primary);
    for p in [&path, &bak, &log, &seed, &dst] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn a_slow_upload_that_keeps_writing_is_not_taken_for_abandoned() {
    let path = temp_path("slow");
    let storage = create(&path);
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    // The last segment lands just before the abandon age runs out.
    writer
        .write(&storage, &bytes(7, SEGMENT_BYTES), now() + ABANDON - 1)
        .unwrap();
    let freed = ObjFile::new(&mut storage.lock())
        .collect_garbage(now() + ABANDON, ABANDON, u64::MAX)
        .unwrap();
    assert_eq!(freed.bodies, 0, "touched recently");
    let sealed = writer.finish(&storage, now() + ABANDON).unwrap();
    let body = ObjFile::new(&mut storage.lock())
        .transaction(|tx| sealed.seal_in(tx))
        .unwrap()
        .body;
    assert_eq!(read(&storage, body, 0, 10).unwrap(), bytes(7, 10));
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_writer_refuses_further_use_after_an_error() {
    let path = temp_path("poison");
    let storage = create(&path);
    put(&storage, &bytes(8, 10), now());
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    FAIL_APPLY_OPS_AFTER.with(|fail| fail.set(Some(0)));
    let err = writer.write(&storage, &bytes(8, SEGMENT_BYTES), now());
    FAIL_APPLY_OPS_AFTER.with(|fail| fail.set(None));
    assert!(err.is_err());
    let again = writer.write(&storage, &bytes(8, 10), now()).unwrap_err();
    assert!(again.to_string().contains("failed earlier"), "{again}");
    assert!(writer.finish(&storage, now()).is_err());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_read_on_a_store_the_engine_never_wrote_changes_nothing() {
    let path = temp_path("read-only");
    let storage = create(&path);
    let tail = storage.wal_flushed_lsn();
    let err = read(&storage, 1, 0, 1).unwrap_err();
    assert!(err.to_string().contains("no body 1"), "{err}");
    assert!(ObjFile::new(&mut storage.lock()).body_info(1).is_err());
    let freed = ObjFile::new(&mut storage.lock())
        .collect_garbage(now(), ABANDON, u64::MAX)
        .unwrap();
    assert_eq!(freed.bodies, 0);
    assert_eq!(storage.wal_flushed_lsn(), tail, "nothing was logged");
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_seal_rolls_back_with_the_transaction_it_joined() {
    let path = temp_path("seal-rollback");
    let storage = create(&path);
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    writer.write(&storage, &bytes(9, 100), now()).unwrap();
    let sealed = writer.finish(&storage, now()).unwrap();
    let err = ObjFile::new(&mut storage.lock()).transaction(|tx| {
        sealed.seal_in(tx)?;
        Err::<(), _>(StorageError::InvalidConfig("the object row failed".into()))
    });
    assert!(err.is_err());
    let info = ObjFile::new(&mut storage.lock()).body_info(sealed.body);
    assert!(info.is_err(), "the body is still staging");
    let freed = ObjFile::new(&mut storage.lock())
        .collect_garbage(now() + ABANDON, ABANDON, u64::MAX)
        .unwrap();
    assert_eq!(freed.bodies, 1, "and is collected as abandoned");
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_wedged_store_refuses_every_object_operation() {
    let path = temp_path("wedged");
    let storage = create(&path);
    let body = put(&storage, &bytes(10, 100), now());
    storage.lock().set_wedged();
    assert!(BodyWriter::begin(&storage, now()).is_err(), "write");
    assert!(read(&storage, body, 0, 100).is_err(), "read");
    assert!(
        ObjFile::new(&mut storage.lock()).body_info(body).is_err(),
        "info"
    );
    assert!(
        ObjFile::new(&mut storage.lock())
            .collect_garbage(now(), ABANDON, u64::MAX)
            .is_err(),
        "collect"
    );
    assert!(BodyLease::take(&storage, body).is_err(), "lease");
    drop(storage);
    let storage = open(&path);
    assert_eq!(
        read(&storage, body, 0, 100).unwrap(),
        bytes(10, 100),
        "a restart clears it"
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn access_keys_are_created_looked_up_listed_and_dropped_durably() {
    let path = temp_path("keys");
    let storage = create(&path);
    {
        let mut guard = storage.lock();
        let mut file = ObjFile::new(&mut guard);
        assert_eq!(file.access_key_secret("A").unwrap(), None, "no keys yet");
        file.transaction(|tx| tx.create_access_key("A", "secret-a"))
            .unwrap();
        file.transaction(|tx| tx.create_access_key("B", "secret-b"))
            .unwrap();
        let dup = file
            .transaction(|tx| tx.create_access_key("A", "other"))
            .unwrap_err();
        assert!(dup.to_string().contains("already exists"), "{dup}");
        assert!(
            file.transaction(|tx| tx.create_access_key("", "x"))
                .is_err(),
            "an empty id is refused"
        );
    }
    drop(storage); // crash

    let storage = open(&path);
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    assert_eq!(
        file.access_key_secret("A").unwrap().as_deref(),
        Some("secret-a")
    );
    assert_eq!(file.access_key_ids().unwrap(), vec!["A", "B"]);
    file.transaction(|tx| tx.drop_access_key("A")).unwrap();
    assert!(file.transaction(|tx| tx.drop_access_key("A")).is_err());
    assert_eq!(file.access_key_secret("A").unwrap(), None);
    assert_eq!(file.access_key_ids().unwrap(), vec!["B"]);
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_collection_pass_stops_at_its_limit_and_says_so() {
    let path = temp_path("gc-limit");
    let storage = create(&path);
    let bodies: Vec<u64> = (0..20)
        .map(|i| put(&storage, &bytes(i, 10), now()))
        .collect();
    for body in &bodies {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.drop_ref(*body, now(), 0))
            .unwrap();
    }
    let mut passes = Vec::new();
    loop {
        let outcome = ObjFile::new(&mut storage.lock())
            .collect_garbage(now(), ABANDON, 8)
            .unwrap();
        passes.push(outcome.bodies);
        if !outcome.more {
            break;
        }
    }
    assert_eq!(passes, vec![8, 8, 4]);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn buckets_are_created_listed_and_deleted_only_when_empty() {
    use crate::{BucketError, Versioning};
    let path = temp_path("buckets");
    let storage = create(&path);
    let in_tx = |op: &dyn Fn(
        &mut crate::ObjTxn<'_, '_>,
    ) -> Result<Result<(), BucketError>, StorageError>| {
        ObjFile::new(&mut storage.lock()).transaction(op).unwrap()
    };
    assert_eq!(in_tx(&|tx| tx.create_bucket("alpha", 5)), Ok(()));
    assert_eq!(in_tx(&|tx| tx.create_bucket("beta", 6)), Ok(()));
    assert_eq!(
        in_tx(&|tx| tx.create_bucket("alpha", 7)),
        Err(BucketError::AlreadyOwnedByYou)
    );
    assert!(matches!(
        in_tx(&|tx| tx.create_bucket("Bad_Name", 7)),
        Err(BucketError::InvalidName(_))
    ));
    assert_eq!(
        in_tx(&|tx| tx.delete_bucket("gamma")),
        Err(BucketError::NoSuchBucket)
    );
    // An object key under "alpha" (the objects tree's layout: bucket, 0, ...).
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            let objects = tx.trees.objects();
            objects.insert_unique(
                tx.ctx,
                &mut truthdb_core::relstore::ctx::OpMode::Txn(tx.txn),
                b"alpha\0key",
                b"row",
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        in_tx(&|tx| tx.delete_bucket("alpha")),
        Err(BucketError::NotEmpty)
    );
    assert_eq!(in_tx(&|tx| tx.delete_bucket("beta")), Ok(()));
    drop(storage);

    let storage = open(&path);
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    let buckets = file.list_buckets().unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].name, "alpha");
    assert_eq!(buckets[0].created_ms, 5);
    assert_eq!(buckets[0].versioning, Versioning::Unversioned);
    assert!(file.bucket("beta").unwrap().is_none());
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn an_object_is_put_overwritten_and_read_back_with_its_attributes() {
    use crate::{ObjectAttrs, ObjectError, VersionSel};
    let path = temp_path("objects");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", now()))
        .unwrap()
        .unwrap();
    let put_version = |data: &[u8], content_type: &str| {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, data, now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        let attrs = ObjectAttrs {
            content_type: Some(content_type.to_string()),
            user: vec![("owner".to_string(), "me".to_string())],
            ..ObjectAttrs::default()
        };
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_object("bkt", "dir/key.txt", &sealed, &attrs, now(), 0))
            .unwrap()
            .unwrap()
    };
    let first = put_version(b"first", "text/plain");
    let second = put_version(b"second!", "application/json");
    assert!(second.version > first.version);
    assert!(
        second.null_version,
        "an unversioned bucket writes null versions"
    );

    let head = |which| {
        ObjFile::new(&mut storage.lock())
            .head_object("bkt", "dir/key.txt", which)
            .unwrap()
    };
    let latest = head(VersionSel::Latest).unwrap();
    assert_eq!(latest.size, 7);
    assert_eq!(
        latest.attrs.content_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(
        latest.attrs.user,
        vec![("owner".to_string(), "me".to_string())]
    );
    assert_eq!(read(&storage, latest.body, 0, 7).unwrap(), b"second!");
    assert_eq!(
        head(VersionSel::Seq(first.version)),
        Err(ObjectError::NoSuchVersion),
        "the overwrite replaced the null version"
    );
    let freed = ObjFile::new(&mut storage.lock())
        .collect_garbage(now(), ABANDON, u64::MAX)
        .unwrap();
    assert_eq!(freed.bodies, 1, "the first body was collected");

    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .head_object("nope", "k", VersionSel::Latest)
            .unwrap(),
        Err(ObjectError::NoSuchBucket)
    );
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .head_object("bkt", "missing", VersionSel::Latest)
            .unwrap(),
        Err(ObjectError::NoSuchKey)
    );
    assert!(matches!(
        ObjFile::new(&mut storage.lock())
            .head_object("bkt", "a\0b", VersionSel::Latest)
            .unwrap(),
        Err(ObjectError::InvalidKey(_))
    ));
    drop(storage);

    let storage = open(&path);
    let latest = ObjFile::new(&mut storage.lock())
        .head_object("bkt", "dir/key.txt", VersionSel::Latest)
        .unwrap()
        .unwrap();
    assert_eq!(latest.version, second.version);
    assert_eq!(read(&storage, latest.body, 0, 7).unwrap(), b"second!");
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn listings_follow_key_order_prefixes_delimiters_and_pages() {
    use crate::{ListAfter, Listing, ObjectAttrs};
    let path = temp_path("listing");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", now()))
        .unwrap()
        .unwrap();
    let keys = [
        "a", "a/b", "a/c", "b/x/1", "b/x/2", "b/y", "c", "ca", "d/", "d/e",
    ];
    for key in keys.iter().chain(["a/b"].iter()) {
        // "a/b" twice: an overwrite lists once.
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_object("bkt", key, &sealed, &ObjectAttrs::default(), now(), 0))
            .unwrap()
            .unwrap();
    }
    let list = |prefix: &str,
                delimiter: Option<&str>,
                after: Option<ListAfter<'_>>,
                max: usize|
     -> Listing {
        ObjFile::new(&mut storage.lock())
            .list_objects("bkt", prefix, delimiter, after, max)
            .unwrap()
            .unwrap()
    };
    let names = |listing: &Listing| -> Vec<String> {
        listing.objects.iter().map(|o| o.key.clone()).collect()
    };

    let all = list("", None, None, 1000);
    assert_eq!(names(&all), keys);
    assert!(!all.truncated);

    // Pages of 3, resuming after each page's last key.
    let mut paged = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = list("", None, after.as_deref().map(ListAfter::Key), 3);
        paged.extend(names(&page));
        if !page.truncated {
            break;
        }
        after = page.last.map(|(key, _)| key);
    }
    assert_eq!(paged, keys);

    let top = list("", Some("/"), None, 1000);
    assert_eq!(names(&top), ["a", "c", "ca"]);
    assert_eq!(top.common_prefixes, ["a/", "b/", "d/"]);

    let b = list("b/", Some("/"), None, 1000);
    assert_eq!(names(&b), ["b/y"]);
    assert_eq!(b.common_prefixes, ["b/x/"]);

    // A page that ends on a common prefix resumes past every key under it.
    let first = list("", Some("/"), None, 2);
    assert_eq!(
        (names(&first), first.common_prefixes.clone()),
        (vec!["a".to_string()], vec!["a/".to_string()])
    );
    assert!(first.truncated);
    assert_eq!(first.last, Some(("a/".to_string(), true)));
    let second = list("", Some("/"), Some(ListAfter::CommonPrefix("a/")), 2);
    assert_eq!(second.common_prefixes, ["b/"]);
    assert_eq!(names(&second), ["c"]);

    // A client's start-after is a plain key.
    assert_eq!(
        names(&list("", None, Some(ListAfter::Key("b/x/1")), 2)),
        ["b/x/2", "b/y"]
    );
    assert_eq!(names(&list("zzz", None, None, 10)), Vec::<String>::new());
    assert!(
        ObjFile::new(&mut storage.lock())
            .list_objects("nope", "", None, None, 10)
            .unwrap()
            .is_err()
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_deleted_object_is_gone_and_its_body_collected() {
    use crate::{Deleted, ObjectAttrs, ObjectError, VersionSel};
    let path = temp_path("delete");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", now()))
        .unwrap()
        .unwrap();
    let put = |key: &str| {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_object("bkt", key, &sealed, &ObjectAttrs::default(), now(), 0))
            .unwrap()
            .unwrap()
    };
    let delete = |key: &str, which| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_object("bkt", key, which, now(), 0))
            .unwrap()
    };
    put("one");
    put("two");
    assert_eq!(delete("one", None), Ok(Deleted::default()));
    assert_eq!(
        delete("one", None),
        Ok(Deleted::default()),
        "deleting again succeeds"
    );
    let removed = delete("two", Some(VersionSel::Null)).unwrap();
    assert!(removed.version.is_some() && removed.null_version);
    for key in ["one", "two"] {
        assert_eq!(
            ObjFile::new(&mut storage.lock())
                .head_object("bkt", key, VersionSel::Latest)
                .unwrap(),
            Err(ObjectError::NoSuchKey)
        );
    }
    let freed = ObjFile::new(&mut storage.lock())
        .collect_garbage(now(), ABANDON, u64::MAX)
        .unwrap();
    assert_eq!(freed.bodies, 2, "both bodies were collected");
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_object("nope", "k", None, now(), 0))
            .unwrap(),
        Err(ObjectError::NoSuchBucket)
    );
    // The bucket is empty again, so it can be deleted.
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_bucket("bkt"))
            .unwrap(),
        Ok(())
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_multipart_upload_completes_into_a_composite_and_aborts_release_parts() {
    use crate::{MIN_PART_BYTES, ObjectAttrs, UploadError, VersionSel};
    let path = temp_path("multipart");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", now()))
        .unwrap()
        .unwrap();
    let attrs = ObjectAttrs {
        content_type: Some("video/mp4".to_string()),
        ..ObjectAttrs::default()
    };
    let upload = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_upload("bkt", "movie.mp4", &attrs, now()))
        .unwrap()
        .unwrap();
    let send_part = |number: u32, data: &[u8]| {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, data, now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        let result = ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.upload_part(upload, number, &sealed, now(), 0))
            .unwrap();
        // The log is small in these tests: checkpoint between parts.
        storage.write_checkpoint(b"x", 1, 2, 1).unwrap();
        result
    };
    let big = MIN_PART_BYTES as usize;
    let part1 = bytes(1, big);
    let part2 = bytes(2, big + 3);
    let part3 = bytes(3, 1000);
    let md5_1 = send_part(1, &part1).unwrap().md5;
    let _replaced = send_part(2, &bytes(9, 10)).unwrap();
    let md5_2 = send_part(2, &part2).unwrap().md5;
    let md5_3 = send_part(3, &part3).unwrap().md5;
    let md5_4 = send_part(4, &bytes(4, 10)).unwrap().md5;
    assert_eq!(send_part(0, b"x"), Err(UploadError::InvalidPartNumber));

    let complete = |listed: &[(u32, [u8; 16])]| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.complete_upload(upload, listed, now(), 0))
            .unwrap()
    };
    assert_eq!(
        complete(&[(2, md5_2), (1, md5_1)]),
        Err(UploadError::InvalidPartOrder)
    );
    assert_eq!(
        complete(&[(1, md5_2), (3, md5_3)]),
        Err(UploadError::InvalidPart(1))
    );
    assert_eq!(
        complete(&[(3, md5_3), (4, md5_4)]),
        Err(UploadError::EntityTooSmall(3)),
        "a part under 5 MiB that is not last"
    );
    let (parts, truncated) = ObjFile::new(&mut storage.lock())
        .list_parts(upload, 0, 2)
        .unwrap();
    assert_eq!(parts.iter().map(|p| p.number).collect::<Vec<_>>(), [1, 2]);
    assert!(truncated);

    let object = complete(&[(1, md5_1), (2, md5_2), (3, md5_3)]).unwrap();
    assert_eq!(object.parts, 3);
    assert!(object.etag().ends_with("-3\""), "{}", object.etag());
    let whole: Vec<u8> = [part1.clone(), part2.clone(), part3.clone()].concat();
    assert_eq!(object.size, whole.len() as u64);
    assert_eq!(read(&storage, object.body, 0, whole.len()).unwrap(), whole);
    let across = big as u64 - 5;
    assert_eq!(
        read(&storage, object.body, across, 10).unwrap(),
        whole[across as usize..across as usize + 10],
        "a read across a part boundary"
    );
    let head = ObjFile::new(&mut storage.lock())
        .head_object("bkt", "movie.mp4", VersionSel::Latest)
        .unwrap()
        .unwrap();
    assert_eq!(head.attrs.content_type.as_deref(), Some("video/mp4"));
    assert!(
        ObjFile::new(&mut storage.lock())
            .upload(upload)
            .unwrap()
            .is_none()
    );
    // The replaced part 2 and the unlisted part 4 are collected.
    let freed = ObjFile::new(&mut storage.lock())
        .collect_garbage(now(), ABANDON, u64::MAX)
        .unwrap();
    assert_eq!(freed.bodies, 2);

    // An aborted upload releases its parts.
    let aborted = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_upload("bkt", "other", &ObjectAttrs::default(), now()))
        .unwrap()
        .unwrap();
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    writer.write(&storage, b"part", now()).unwrap();
    let sealed = writer.finish(&storage, now()).unwrap();
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.upload_part(aborted, 1, &sealed, now(), 0))
        .unwrap()
        .unwrap();
    let (listed, _) = ObjFile::new(&mut storage.lock())
        .list_uploads("bkt", "", None, 10)
        .unwrap();
    assert_eq!(
        listed.iter().map(|u| u.key.as_str()).collect::<Vec<_>>(),
        ["other"]
    );
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.abort_upload(aborted, now(), 0))
        .unwrap()
        .unwrap();
    let freed = ObjFile::new(&mut storage.lock())
        .collect_garbage(now(), ABANDON, u64::MAX)
        .unwrap();
    assert_eq!(freed.bodies, 1);

    // An open upload keeps its bucket until it is aborted.
    use crate::BucketError;
    let pending = ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("upl", now())?.unwrap();
            tx.create_upload("upl", "k", &ObjectAttrs::default(), now())
        })
        .unwrap()
        .unwrap();
    let delete_bucket = || {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_bucket("upl"))
            .unwrap()
    };
    assert_eq!(delete_bucket(), Err(BucketError::NotEmpty));
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.abort_upload(pending, now(), 0))
        .unwrap()
        .unwrap();
    assert_eq!(delete_bucket(), Ok(()));
    drop(storage);

    // The composite survives a crash and its parts go when it does.
    let storage = open(&path);
    assert_eq!(read(&storage, object.body, 0, whole.len()).unwrap(), whole);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_object("bkt", "movie.mp4", None, now(), 0))
        .unwrap()
        .unwrap();
    let collect = || {
        ObjFile::new(&mut storage.lock())
            .collect_garbage(now(), ABANDON, u64::MAX)
            .unwrap()
            .bodies
    };
    assert_eq!(collect(), 1, "the composite, which releases its parts");
    assert_eq!(collect(), 3, "then its three parts");
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_copy_shares_the_body_and_outlives_its_source() {
    use crate::{MIN_PART_BYTES, ObjectAttrs, VersionSel};
    let path = temp_path("copy");
    let storage = create(&path);
    for bucket in ["src", "dst"] {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.create_bucket(bucket, now()))
            .unwrap()
            .unwrap();
    }
    let data = bytes(7, 5000);
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    writer.write(&storage, &data, now()).unwrap();
    let sealed = writer.finish(&storage, now()).unwrap();
    let attrs = ObjectAttrs {
        content_type: Some("text/plain".to_string()),
        ..ObjectAttrs::default()
    };
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.put_object("src", "a", &sealed, &attrs, now(), 0))
        .unwrap()
        .unwrap();
    let head = |bucket: &str, key: &str| {
        ObjFile::new(&mut storage.lock())
            .head_object(bucket, key, VersionSel::Latest)
            .unwrap()
    };
    let copy = |bucket: &str, key: &str, attrs: &ObjectAttrs| {
        let source = head("src", "a").unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.copy_object(bucket, key, &source, attrs, now(), 0))
            .unwrap()
    };
    let pages = allocated_pages(&storage);
    let copied = copy("dst", "b", &attrs).unwrap();
    let source = head("src", "a").unwrap();
    assert_eq!(copied.body, source.body, "the copy shares the body");
    assert_eq!(copied.etag(), source.etag());
    assert_eq!(allocated_pages(&storage), pages, "no bytes were copied");
    // Replacing the source's metadata in place keeps its one body alive.
    let replaced = ObjectAttrs {
        content_type: Some("text/csv".to_string()),
        ..ObjectAttrs::default()
    };
    copy("src", "a", &replaced).unwrap();
    assert_eq!(
        head("src", "a").unwrap().attrs.content_type.as_deref(),
        Some("text/csv")
    );
    assert_eq!(
        copy("nope", "b", &attrs),
        Err(crate::ObjectError::NoSuchBucket)
    );
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_object("src", "a", None, now(), 0))
        .unwrap()
        .unwrap();
    let collected = ObjFile::new(&mut storage.lock())
        .collect_garbage(now(), ABANDON, u64::MAX)
        .unwrap();
    assert_eq!(collected.bodies, 0, "the copy still holds the body");
    let copied = head("dst", "b").unwrap();
    assert_eq!(read(&storage, copied.body, 0, data.len()).unwrap(), data);

    // A whole plain object shares its body as a part; a multipart object's
    // body is refused (composites do not nest).
    let upload = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_upload("dst", "joined", &ObjectAttrs::default(), now()))
        .unwrap()
        .unwrap();
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    writer
        .write(&storage, &bytes(8, MIN_PART_BYTES as usize), now())
        .unwrap();
    let first = writer.finish(&storage, now()).unwrap();
    let part1 = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.upload_part(upload, 1, &first, now(), 0))
        .unwrap()
        .unwrap();
    storage.write_checkpoint(b"x", 1, 2, 1).unwrap();
    let part2 = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.upload_part_shared(upload, 2, copied.body, now(), 0))
        .unwrap()
        .unwrap();
    assert_eq!((part2.body, part2.md5), (copied.body, copied.md5));
    let joined = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.complete_upload(upload, &[(1, part1.md5), (2, part2.md5)], now(), 0))
        .unwrap()
        .unwrap();
    let other = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_upload("dst", "again", &ObjectAttrs::default(), now()))
        .unwrap()
        .unwrap();
    assert!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.upload_part_shared(other, 1, joined.body, now(), 0))
            .is_err()
    );
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_object("dst", "b", None, now(), 0))
        .unwrap()
        .unwrap();
    let whole = read(&storage, joined.body, 0, joined.size as usize).unwrap();
    assert_eq!(&whole[MIN_PART_BYTES as usize..], &data[..]);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn tags_are_set_replaced_and_cleared_per_version() {
    use crate::{ObjectAttrs, ObjectError, VersionSel};
    let path = temp_path("tags");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", now()))
        .unwrap()
        .unwrap();
    let put = |key: &str, attrs: &ObjectAttrs| {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_object("bkt", key, &sealed, attrs, now(), 0))
            .unwrap()
            .unwrap()
    };
    let set = |key: &str, which, tags: &[(&str, &str)]| {
        let tags = tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.set_tags("bkt", key, which, tags))
            .unwrap()
    };
    let head = |key: &str| {
        ObjFile::new(&mut storage.lock())
            .head_object("bkt", key, VersionSel::Latest)
            .unwrap()
            .unwrap()
            .attrs
    };
    let typed = ObjectAttrs {
        content_type: Some("text/plain".to_string()),
        tags: vec![("a".to_string(), "1".to_string())],
        ..ObjectAttrs::default()
    };
    let plain = put("plain", &ObjectAttrs::default());
    put("typed", &typed);
    assert_eq!(head("typed"), typed, "tags given at PUT are kept");

    // Tags on a version without attributes, then cleared again.
    set("plain", VersionSel::Seq(plain.version), &[("x", "y")]).unwrap();
    assert_eq!(head("plain").tags, [("x".to_string(), "y".to_string())]);
    set("plain", VersionSel::Latest, &[]).unwrap();
    assert_eq!(head("plain"), ObjectAttrs::default());
    // Replacing the tags keeps the other attributes.
    set("typed", VersionSel::Latest, &[("b", "2")]).unwrap();
    assert_eq!(
        head("typed"),
        ObjectAttrs {
            tags: vec![("b".to_string(), "2".to_string())],
            ..typed.clone()
        }
    );
    assert_eq!(
        set("missing", VersionSel::Latest, &[]),
        Err(ObjectError::NoSuchKey)
    );
    assert_eq!(
        set("plain", VersionSel::Seq(999), &[]),
        Err(ObjectError::NoSuchVersion)
    );
    drop(storage);

    // The tags survive a crash.
    let storage = open(&path);
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .head_object("bkt", "typed", VersionSel::Latest)
            .unwrap()
            .unwrap()
            .attrs
            .tags,
        [("b".to_string(), "2".to_string())]
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}
