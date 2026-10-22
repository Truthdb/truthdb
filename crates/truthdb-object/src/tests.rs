use std::path::{Path, PathBuf};

use md5::{Digest, Md5};
use truthdb_core::relstore::ctx::{FAIL_APPLY_OPS_AFTER, OpMode, RelCtx};
use truthdb_core::relstore::recovery::LogicalUndo;
use truthdb_core::storage::{
    CATALOG_ENGINE_ID, EngineParticipant, Storage, StorageError, StorageFile, StorageOptions,
};
use truthdb_core::wal::records::{PageOpUndo, RelRecord};

use crate::body::SEGMENT_BYTES;
use crate::{BodyLease, BodyWriter, DEFAULT_ACCOUNT, ObjEngine, ObjFile};

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
        assert_eq!(file.access_key("A").unwrap(), None, "no keys yet");
        file.transaction(|tx| tx.create_access_key("A", "secret-a", DEFAULT_ACCOUNT, None, 5))
            .unwrap()
            .unwrap();
        file.transaction(|tx| tx.create_access_key("B", "secret-b", DEFAULT_ACCOUNT, None, 6))
            .unwrap()
            .unwrap();
        let dup = file
            .transaction(|tx| tx.create_access_key("A", "other", DEFAULT_ACCOUNT, None, 7))
            .unwrap_err();
        assert!(dup.to_string().contains("already exists"), "{dup}");
        for (id, secret) in [("", "x"), ("C", "with space"), ("C", "\u{1}x")] {
            assert!(
                file.transaction(|tx| tx.create_access_key(id, secret, DEFAULT_ACCOUNT, None, 7))
                    .is_err(),
                "{id:?} {secret:?} is refused"
            );
        }
    }
    drop(storage); // crash

    let storage = open(&path);
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    let (key, secret) = file.access_key("A").unwrap().expect("key A");
    assert_eq!(secret, "secret-a");
    assert_eq!(
        key,
        crate::AccessKey {
            id: "A".to_string(),
            account: DEFAULT_ACCOUNT.to_string(),
            user: None,
            active: true,
            created_ms: 5,
        }
    );
    let ids = |file: &mut ObjFile<'_>| -> Vec<String> {
        file.access_keys()
            .unwrap()
            .into_iter()
            .map(|key| key.id)
            .collect()
    };
    assert_eq!(ids(&mut file), vec!["A", "B"]);
    file.transaction(|tx| tx.drop_access_key("A")).unwrap();
    assert!(file.transaction(|tx| tx.drop_access_key("A")).is_err());
    assert_eq!(file.access_key("A").unwrap(), None);
    assert_eq!(ids(&mut file), vec!["B"]);
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn accounts_own_users_keys_and_buckets_and_drop_only_when_empty() {
    use crate::{AccountError, BucketError};
    let path = temp_path("accounts");
    let storage = create(&path);
    {
        let mut guard = storage.lock();
        let mut file = ObjFile::new(&mut guard);
        let mut tx = |op: &dyn Fn(
            &mut crate::ObjTxn<'_, '_>,
        ) -> Result<Result<(), AccountError>, StorageError>| {
            file.transaction(op).unwrap()
        };
        assert_eq!(
            tx(&|tx| tx.create_account("111111111111", "acme", 1)),
            Ok(())
        );
        assert_eq!(
            tx(&|tx| tx.create_account("111111111111", "again", 1)),
            Err(AccountError::AccountExists)
        );
        assert_eq!(
            tx(&|tx| tx.create_account(DEFAULT_ACCOUNT, "x", 1)),
            Err(AccountError::AccountExists)
        );
        assert!(matches!(
            tx(&|tx| tx.create_account("12345", "short", 1)),
            Err(AccountError::InvalidName(_))
        ));
        assert!(matches!(
            tx(&|tx| tx.create_account("222222222222", "bad name", 1)),
            Err(AccountError::InvalidName(_))
        ));
        assert_eq!(
            tx(&|tx| tx.create_user("111111111111", "alice", "AIDAALICE", 2)),
            Ok(())
        );
        assert_eq!(
            tx(&|tx| tx.create_user("111111111111", "alice", "AIDAOTHER", 2)),
            Err(AccountError::UserExists)
        );
        assert_eq!(
            tx(&|tx| tx.create_user("999999999999", "bob", "AIDABOB", 2)),
            Err(AccountError::NoSuchAccount)
        );
        assert_eq!(
            tx(&|tx| tx.create_access_key("K1", "s1", "111111111111", Some("alice"), 3)),
            Ok(())
        );
        assert_eq!(
            tx(&|tx| tx.create_access_key("K2", "s2", "111111111111", Some("carol"), 3)),
            Err(AccountError::NoSuchUser)
        );
        assert_eq!(
            tx(&|tx| tx.create_access_key("K3", "s3", "999999999999", None, 3)),
            Err(AccountError::NoSuchAccount)
        );
    }
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    let bucket = |file: &mut ObjFile<'_>, name: &str, owner: &str| {
        file.transaction(|tx| tx.create_bucket(name, owner, 4))
            .unwrap()
    };
    assert_eq!(bucket(&mut file, "acme-data", "111111111111"), Ok(()));
    assert_eq!(
        bucket(&mut file, "acme-data", DEFAULT_ACCOUNT),
        Err(BucketError::AlreadyExists),
        "another account's bucket name"
    );
    assert_eq!(
        bucket(&mut file, "acme-data", "111111111111"),
        Err(BucketError::AlreadyOwnedByYou)
    );
    assert_eq!(
        file.bucket("acme-data").unwrap().unwrap().owner,
        "111111111111"
    );

    // Dropped only once its users, keys and buckets are gone.
    let drop_account = |file: &mut ObjFile<'_>| {
        file.transaction(|tx| tx.drop_account("111111111111"))
            .unwrap()
    };
    assert_eq!(
        drop_account(&mut file),
        Err(AccountError::NotEmpty("users"))
    );
    assert_eq!(
        file.transaction(|tx| tx.drop_user("111111111111", "alice"))
            .unwrap(),
        Err(AccountError::NotEmpty("access keys"))
    );
    file.transaction(|tx| tx.drop_access_key("K1")).unwrap();
    assert_eq!(
        file.transaction(|tx| tx.drop_user("111111111111", "alice"))
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        drop_account(&mut file),
        Err(AccountError::NotEmpty("buckets"))
    );
    file.transaction(|tx| tx.delete_bucket("acme-data"))
        .unwrap()
        .unwrap();
    file.transaction(|tx| tx.create_access_key("K4", "s4", "111111111111", None, 5))
        .unwrap()
        .unwrap();
    assert_eq!(
        drop_account(&mut file),
        Err(AccountError::NotEmpty("access keys"))
    );
    file.transaction(|tx| tx.drop_access_key("K4")).unwrap();
    assert_eq!(drop_account(&mut file), Ok(()));
    assert_eq!(
        file.transaction(|tx| tx.drop_account(DEFAULT_ACCOUNT))
            .unwrap(),
        Err(AccountError::DefaultAccount)
    );
    let ids: Vec<String> = file.accounts().unwrap().into_iter().map(|a| a.id).collect();
    assert_eq!(ids, vec![DEFAULT_ACCOUNT]);
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn rows_written_before_accounts_belong_to_the_default_account() {
    // An access key row was the bare secret; a bucket row had no owner.
    let key = crate::credentials::KeyRow::decode(b"abcSECRET+/9").unwrap();
    assert_eq!(
        (
            key.account.as_str(),
            key.user,
            key.active,
            key.secret.as_str()
        ),
        (DEFAULT_ACCOUNT, None, true, "abcSECRET+/9")
    );
    let mut old = 5u64.to_le_bytes().to_vec();
    old.push(1);
    old.extend_from_slice(&9u64.to_le_bytes());
    for row in [&old[..9], &old[..]] {
        let bucket = crate::buckets::BucketRow::decode(row).unwrap();
        assert_eq!(bucket.owner, DEFAULT_ACCOUNT);
        assert_eq!(bucket.created_ms, 5);
    }
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
    assert_eq!(
        in_tx(&|tx| tx.create_bucket("alpha", DEFAULT_ACCOUNT, 5)),
        Ok(())
    );
    assert_eq!(
        in_tx(&|tx| tx.create_bucket("beta", DEFAULT_ACCOUNT, 6)),
        Ok(())
    );
    assert_eq!(
        in_tx(&|tx| tx.create_bucket("alpha", DEFAULT_ACCOUNT, 7)),
        Err(BucketError::AlreadyOwnedByYou)
    );
    assert!(matches!(
        in_tx(&|tx| tx.create_bucket("Bad_Name", DEFAULT_ACCOUNT, 7)),
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
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
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
            .transaction(|tx| {
                tx.put_object(
                    "bkt",
                    "dir/key.txt",
                    &sealed,
                    &attrs,
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
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
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
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
            .transaction(|tx| {
                tx.put_object(
                    "bkt",
                    key,
                    &sealed,
                    &ObjectAttrs::default(),
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
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
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    let put = |key: &str| {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_object(
                    "bkt",
                    key,
                    &sealed,
                    &ObjectAttrs::default(),
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
            .unwrap()
            .unwrap()
    };
    let delete = |key: &str, which| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_object("bkt", key, which, false, now(), 0))
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
            .transaction(|tx| tx.delete_object("nope", "k", None, false, now(), 0))
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
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    let attrs = ObjectAttrs {
        content_type: Some("video/mp4".to_string()),
        ..ObjectAttrs::default()
    };
    let upload = ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_upload(
                "bkt",
                "movie.mp4",
                &attrs,
                crate::StorageClass::Standard,
                now(),
            )
        })
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
        .transaction(|tx| {
            tx.create_upload(
                "bkt",
                "other",
                &ObjectAttrs::default(),
                crate::StorageClass::Standard,
                now(),
            )
        })
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
            tx.create_bucket("upl", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.create_upload(
                "upl",
                "k",
                &ObjectAttrs::default(),
                crate::StorageClass::Standard,
                now(),
            )
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
        .transaction(|tx| tx.delete_object("bkt", "movie.mp4", None, false, now(), 0))
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
            .transaction(|tx| tx.create_bucket(bucket, DEFAULT_ACCOUNT, now()))
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
        .transaction(|tx| {
            tx.put_object(
                "src",
                "a",
                &sealed,
                &attrs,
                crate::StorageClass::Standard,
                now(),
                0,
            )
        })
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
            .transaction(|tx| {
                tx.copy_object(
                    bucket,
                    key,
                    &source,
                    attrs,
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
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
        .transaction(|tx| tx.delete_object("src", "a", None, false, now(), 0))
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
        .transaction(|tx| {
            tx.create_upload(
                "dst",
                "joined",
                &ObjectAttrs::default(),
                crate::StorageClass::Standard,
                now(),
            )
        })
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
        .transaction(|tx| {
            tx.create_upload(
                "dst",
                "again",
                &ObjectAttrs::default(),
                crate::StorageClass::Standard,
                now(),
            )
        })
        .unwrap()
        .unwrap();
    assert!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.upload_part_shared(other, 1, joined.body, now(), 0))
            .is_err()
    );
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_object("dst", "b", None, false, now(), 0))
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
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    let put = |key: &str, attrs: &ObjectAttrs| {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_object(
                    "bkt",
                    key,
                    &sealed,
                    attrs,
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
            .unwrap()
            .unwrap()
    };
    let set = |key: &str, which, tags: &[(&str, &str)]| {
        let tags = tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.set_tags("bkt", key, which, tags, now()))
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

#[test]
fn versions_list_newest_first_with_markers_and_resume_mid_key() {
    use crate::{ListAfter, ObjectAttrs, Versioning};
    let path = temp_path("versions");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("bkt", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.set_versioning("bkt", Versioning::Enabled)
        })
        .unwrap()
        .unwrap();
    let put = |key: &str| {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_object(
                    "bkt",
                    key,
                    &sealed,
                    &ObjectAttrs::default(),
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
            .unwrap()
            .unwrap()
            .version
    };
    let a1 = put("a");
    let a2 = put("a");
    let marker = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_object("bkt", "a", None, false, now(), 0))
        .unwrap()
        .unwrap()
        .version
        .unwrap();
    let b1 = put("b");
    let d1 = put("dir/x");
    put("dir/y");
    let list = |delimiter: Option<&str>, after: Option<ListAfter<'_>>, max: usize| {
        ObjFile::new(&mut storage.lock())
            .list_versions("bkt", "", delimiter, after, max)
            .unwrap()
            .unwrap()
    };
    let all = list(None, None, 100);
    let rows: Vec<(&str, u64, bool, bool)> = all
        .objects
        .iter()
        .map(|o| {
            (
                o.key.as_str(),
                o.info.version,
                o.latest,
                o.info.delete_marker,
            )
        })
        .collect();
    assert_eq!(
        rows[..4],
        [
            ("a", marker, true, true),
            ("a", a2, false, false),
            ("a", a1, false, false),
            ("b", b1, true, false),
        ]
    );
    assert_eq!(rows.len(), 6);
    // The plain listing leaves out the deleted key.
    let current = ObjFile::new(&mut storage.lock())
        .list_objects("bkt", "", None, None, 100)
        .unwrap()
        .unwrap();
    assert_eq!(
        current
            .objects
            .iter()
            .map(|o| o.key.as_str())
            .collect::<Vec<_>>(),
        ["b", "dir/x", "dir/y"]
    );

    // Paging two at a time resumes inside a key's versions.
    let page = list(None, None, 2);
    assert!(page.truncated);
    assert_eq!(page.last_version, Some(a2));
    let next = list(None, Some(ListAfter::Version("a", a2)), 2);
    assert_eq!(
        next.objects
            .iter()
            .map(|o| (o.info.version, o.latest))
            .collect::<Vec<_>>(),
        [(a1, false), (b1, true)]
    );
    // A page that ends on a common prefix hands it back as the marker; the
    // next page does not list it again.
    let page = list(Some("/"), Some(ListAfter::Key("b")), 1);
    assert_eq!(page.common_prefixes, ["dir/"]);
    assert_eq!(page.last, Some(("dir/".to_string(), true)));
    let next = list(Some("/"), Some(ListAfter::Key("dir/")), 10);
    assert!(next.objects.is_empty() && next.common_prefixes.is_empty());
    assert!(!next.truncated);
    let resumed = list(None, Some(ListAfter::Key("dir/")), 10);
    assert_eq!(resumed.objects.first().map(|o| o.info.version), Some(d1));
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_key_with_many_versions_neither_ends_a_listing_nor_unbounds_it() {
    use crate::buckets::BucketRow;
    use crate::{ListAfter, ObjectAttrs, Versioning};
    let path = temp_path("many-versions");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("bkt", DEFAULT_ACCOUNT, now())?.unwrap();
            // Versioning on, through the row (the API to set it comes later).
            let row = BucketRow {
                created_ms: now(),
                versioning: Versioning::Enabled,
                journal_from: 0,
                owner: DEFAULT_ACCOUNT.to_string(),
                object_lock: false,
            };
            tx.trees
                .buckets()
                .update(tx.ctx, &mut OpMode::Txn(tx.txn), b"bkt", &row.encode())?;
            Ok(())
        })
        .unwrap();
    let put = |key: &str| {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_object(
                    "bkt",
                    key,
                    &sealed,
                    &ObjectAttrs::default(),
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
            .unwrap()
            .unwrap();
    };
    put("a");
    // More versions than a scan batch, and than a call's row budget.
    for _ in 0..40 {
        put("k");
    }
    put("z");
    let list = |after: Option<ListAfter<'_>>| {
        ObjFile::new(&mut storage.lock())
            .list_objects("bkt", "", None, after, 1000)
            .unwrap()
            .unwrap()
    };
    let mut keys = Vec::new();
    let mut page = list(None);
    loop {
        keys.extend(page.objects.iter().map(|o| o.key.clone()));
        if !page.truncated {
            break;
        }
        let (last, _) = page
            .last
            .clone()
            .expect("a truncated page says where it ends");
        page = list(Some(ListAfter::Key(&last)));
    }
    assert_eq!(keys, ["a", "k", "z"]);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_folder_whose_keys_are_all_deleted_is_not_listed() {
    use crate::buckets::BucketRow;
    use crate::{ObjectAttrs, Versioning};
    let path = temp_path("deleted-folder");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("bkt", DEFAULT_ACCOUNT, now())?.unwrap();
            let row = BucketRow {
                created_ms: now(),
                versioning: Versioning::Enabled,
                journal_from: 0,
                owner: DEFAULT_ACCOUNT.to_string(),
                object_lock: false,
            };
            tx.trees
                .buckets()
                .update(tx.ctx, &mut OpMode::Txn(tx.txn), b"bkt", &row.encode())?;
            Ok(())
        })
        .unwrap();
    for key in ["dir/x", "dir/y", "e"] {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        writer.write(&storage, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(&storage, now()).unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_object(
                    "bkt",
                    key,
                    &sealed,
                    &ObjectAttrs::default(),
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
            .unwrap()
            .unwrap();
    }
    let list = || {
        ObjFile::new(&mut storage.lock())
            .list_objects("bkt", "", Some("/"), None, 1000)
            .unwrap()
            .unwrap()
    };
    // One live key keeps the folder; delete markers on both remove it.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_object("bkt", "dir/x", None, false, now(), 0))
        .unwrap()
        .unwrap();
    assert_eq!(list().common_prefixes, ["dir/"]);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_object("bkt", "dir/y", None, false, now(), 0))
        .unwrap()
        .unwrap();
    let listing = list();
    assert!(listing.common_prefixes.is_empty(), "{listing:?}");
    assert_eq!(
        listing
            .objects
            .iter()
            .map(|o| o.key.as_str())
            .collect::<Vec<_>>(),
        ["e"]
    );
    // A version named by number is removed alone.
    let version = listing.objects[0].info.version;
    let delete_version = || {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.delete_object(
                    "bkt",
                    "e",
                    Some(crate::VersionSel::Seq(version)),
                    false,
                    now(),
                    0,
                )
            })
            .unwrap()
            .unwrap()
    };
    let removed = delete_version();
    assert_eq!(removed.version, Some(version));
    assert!(!removed.delete_marker);
    assert_eq!(delete_version().version, None, "nothing left to remove");
    assert!(list().objects.is_empty());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn uploads_past_their_age_are_found_oldest_first() {
    use crate::ObjectAttrs;
    let path = temp_path("expired-uploads");
    let storage = create(&path);
    let start = |at: u64| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()).ok();
                tx.create_upload(
                    "bkt",
                    "k",
                    &ObjectAttrs::default(),
                    crate::StorageClass::Standard,
                    at,
                )
            })
            .unwrap()
            .unwrap()
    };
    let old = start(1_000);
    let older_still_running = start(2_000);
    let fresh = start(9_000);
    let expired = |before: u64, max: usize| {
        ObjFile::new(&mut storage.lock())
            .expired_uploads(before, max)
            .unwrap()
    };
    assert_eq!(expired(5_000, 10), [old, older_still_running]);
    assert_eq!(expired(5_000, 1), [old], "at most `max`");
    assert_eq!(expired(500, 10), Vec::<u64>::new());
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.abort_upload(old, now(), 0))
        .unwrap()
        .unwrap();
    assert_eq!(expired(10_000, 10), [older_still_running, fresh]);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn the_journal_records_every_change_in_its_own_transaction() {
    use crate::{JournalRecord, ObjectAttrs, RecordType, VersionSel, Versioning};
    let path = temp_path("journal");
    let storage = create(&path);
    let journal = |storage: &Storage| -> Vec<JournalRecord> {
        ObjFile::new(&mut storage.lock()).journal(0, 1000).unwrap()
    };
    let shape = |records: &[JournalRecord]| -> Vec<(RecordType, String, Option<bool>)> {
        records
            .iter()
            .map(|r| (r.record_type, r.key.clone(), r.is_delete_marker))
            .collect()
    };
    let put = |storage: &Storage, bucket: &str, key: &str| {
        let mut writer = BodyWriter::begin(storage, now()).unwrap();
        writer.write(storage, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(storage, now()).unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_object(
                    bucket,
                    key,
                    &sealed,
                    &ObjectAttrs::default(),
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
            .unwrap()
            .unwrap()
    };
    let delete = |storage: &Storage, bucket: &str, key: &str, which| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_object(bucket, key, which, false, now(), 0))
            .unwrap()
            .unwrap()
    };
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("plain", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.create_bucket("hist", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.set_versioning("hist", Versioning::Enabled)
        })
        .unwrap()
        .unwrap();

    // An unversioned bucket: an overwrite records only the new version.
    let first = put(&storage, "plain", "k");
    put(&storage, "plain", "k");
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.set_tags(
                "plain",
                "k",
                VersionSel::Latest,
                vec![("a".into(), "1".into())],
                now(),
            )
        })
        .unwrap()
        .unwrap();
    delete(&storage, "plain", "k", None);
    let records = journal(&storage);
    assert_eq!(
        shape(&records),
        [
            (RecordType::Create, "k".to_string(), Some(false)),
            (RecordType::Create, "k".to_string(), Some(false)),
            (RecordType::UpdateMetadata, "k".to_string(), Some(false)),
            (RecordType::Delete, "k".to_string(), None),
        ]
    );
    assert_eq!(records[0].version, first.version);
    assert!(
        records
            .iter()
            .all(|r| r.null_version && r.bucket == "plain")
    );
    assert_eq!(records[0].object.unwrap().size, 1);
    assert!(
        records[3].object.is_none(),
        "a permanent delete has no object columns"
    );
    assert!(records.windows(2).all(|w| w[0].sequence < w[1].sequence));

    // A versioned bucket: a delete adds a marker; removing a version by id is
    // a permanent delete.
    let kept = put(&storage, "hist", "v");
    let marker = delete(&storage, "hist", "v", None).version.unwrap();
    delete(&storage, "hist", "v", Some(VersionSel::Seq(kept.version)));
    let records = journal(&storage);
    let tail = &records[4..];
    assert_eq!(
        shape(tail),
        [
            (RecordType::Create, "v".to_string(), Some(false)),
            (RecordType::Delete, "v".to_string(), Some(true)),
            (RecordType::Delete, "v".to_string(), None),
        ]
    );
    assert_eq!((tail[1].version, tail[2].version), (marker, kept.version));
    assert_eq!(tail[1].object.unwrap().size, 0, "a marker's size is 0");

    // A suspended bucket: the null version replaced by a null marker records
    // the marker only.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.set_versioning("hist", Versioning::Suspended))
        .unwrap()
        .unwrap();
    put(&storage, "hist", "s");
    delete(&storage, "hist", "s", None);
    let records = journal(&storage);
    assert_eq!(
        shape(&records[7..]),
        [
            (RecordType::Create, "s".to_string(), Some(false)),
            (RecordType::Delete, "s".to_string(), Some(true)),
        ]
    );
    assert!(records[8].null_version);

    // A change rolled back leaves no row.
    let before = journal(&storage).len();
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    writer.write(&storage, b"x", now()).unwrap();
    let sealed = writer.finish(&storage, now()).unwrap();
    let failed: Result<(), StorageError> = ObjFile::new(&mut storage.lock()).transaction(|tx| {
        tx.put_object(
            "plain",
            "gone",
            &sealed,
            &ObjectAttrs::default(),
            crate::StorageClass::Standard,
            now(),
            0,
        )?
        .unwrap();
        Err(StorageError::InvalidConfig("abort".into()))
    });
    assert!(failed.is_err());
    assert_eq!(journal(&storage).len(), before);
    drop(storage);

    // The journal survives a crash, numbering goes on, and trimming deletes
    // the oldest rows up to its limit and stops at the first young row.
    let storage = open(&path);
    assert_eq!(journal(&storage).len(), before);
    put(&storage, "plain", "after");
    let records = journal(&storage);
    assert_eq!(records.len(), before + 1);
    assert!(records[before].sequence > records[before - 1].sequence);
    // Trimming at `at` with a 1 ms retention takes the rows stamped before it.
    let trim = |at: u64, limit: usize| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.trim_journal(at, 1, u64::MAX, limit))
            .unwrap()
    };
    let due = |at: u64| {
        ObjFile::new(&mut storage.lock())
            .journal_due(at, 1, u64::MAX)
            .unwrap()
    };
    assert_eq!(trim(now() + 1, 3), 3);
    assert_eq!(journal(&storage).len(), before + 1 - 3);
    assert!(!due(now()));
    assert_eq!(trim(now(), 100), 0, "nothing is older than now()");
    assert!(due(now() + 1));
    assert_eq!(trim(now() + 1, 1000), before + 1 - 3);
    assert!(journal(&storage).is_empty());
    assert!(!due(now() + 1));

    // A row stamped by a clock that ran ahead goes with the rows recorded
    // after it, once they expire.
    let tag = |at: u64| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.set_tags("plain", "after", VersionSel::Latest, Vec::new(), at))
            .unwrap()
            .unwrap();
    };
    tag(now() + 1_000_000);
    tag(now());
    tag(now());
    assert!(due(now() + 1));
    assert_eq!(trim(now() + 1, 100), 3);
    assert!(journal(&storage).is_empty());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn the_journal_ships_to_a_standby_and_restores_from_a_log_backup() {
    use crate::ObjectAttrs;
    let path = temp_path("journal-ship-src");
    let bak = temp_path("journal-ship-bak");
    let log = temp_path("journal-ship-log");
    let seed = temp_path("journal-ship-seed");
    let dst = temp_path("journal-ship-dst");
    let primary = create(&path);
    primary.lock().commit_db_options(0, Some(true)).unwrap();
    let summary = primary.backup_full(&bak).unwrap();
    ObjFile::new(&mut primary.lock())
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    for key in ["a", "b"] {
        let mut writer = BodyWriter::begin(&primary, now()).unwrap();
        writer.write(&primary, key.as_bytes(), now()).unwrap();
        let sealed = writer.finish(&primary, now()).unwrap();
        ObjFile::new(&mut primary.lock())
            .transaction(|tx| {
                tx.put_object(
                    "bkt",
                    key,
                    &sealed,
                    &ObjectAttrs::default(),
                    crate::StorageClass::Standard,
                    now(),
                    0,
                )
            })
            .unwrap()
            .unwrap();
    }
    ObjFile::new(&mut primary.lock())
        .transaction(|tx| tx.delete_object("bkt", "a", None, false, now(), 0))
        .unwrap()
        .unwrap();
    let journal = |storage: &Storage| ObjFile::new(&mut storage.lock()).journal(0, 100).unwrap();
    let expected = journal(&primary);
    assert_eq!(expected.len(), 3);
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, primary.wal_flushed_lsn())
        .unwrap();
    primary.backup_log(&log, true, false).unwrap();

    Storage::restore_full_standby(&seed, &bak, &[], engines).unwrap();
    let standby = open(&seed);
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .unwrap();
    assert_eq!(journal(&standby), expected);
    drop(standby);

    Storage::restore_full_with_logs(&dst, &bak, &[log.clone()], None, engines).unwrap();
    let restored = open(&dst);
    assert_eq!(journal(&restored), expected);
    drop(restored);
    drop(primary);
    for p in [&path, &bak, &log, &seed, &dst] {
        std::fs::remove_file(p).ok();
    }
}

/// Puts `text` as `key` in `bucket` (a text object) and returns its version.
fn put_text(storage: &Storage, bucket: &str, key: &str, text: &str) -> u64 {
    use crate::ObjectAttrs;
    let mut writer = BodyWriter::begin(storage, now()).unwrap();
    writer.write(storage, text.as_bytes(), now()).unwrap();
    let sealed = writer.finish(storage, now()).unwrap();
    let attrs = ObjectAttrs {
        content_type: Some("text/plain".into()),
        ..ObjectAttrs::default()
    };
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.put_object(
                bucket,
                key,
                &sealed,
                &attrs,
                crate::StorageClass::Standard,
                now(),
                0,
            )
        })
        .unwrap()
        .unwrap()
        .version
}

/// The documents matching `condition` in `bucket`'s full-text index.
fn ft_hits(storage: &Storage, bucket: &str, condition: &str) -> Vec<u64> {
    let mut search = truthdb_fulltext::Search::new(condition).unwrap();
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    assert!(file.fulltext_step(&mut search, bucket, usize::MAX).unwrap());
    let stats = file.fulltext_stats(bucket).unwrap();
    // A hit counts when its version row names it (a document written in
    // part has none).
    let mut docs: Vec<u64> = search
        .rank(stats)
        .iter()
        .map(|hit| hit.doc)
        .filter(|&doc| file.fulltext_version_key(bucket, doc).unwrap().is_some())
        .collect();
    docs.sort();
    docs
}

/// The bucket's full-text generation (0 when it has no full-text state).
fn ft_generation(storage: &Storage, bucket: &str) -> u64 {
    ObjFile::new(&mut storage.lock())
        .fulltext_buckets()
        .unwrap()
        .into_iter()
        .find(|ft| ft.name == bucket)
        .map_or(0, |ft| ft.generation)
}

/// Runs `body` in one transaction; an error inside it panics instead of
/// rolling back (with `FAIL_APPLY_OPS_AFTER`, a crash at that op).
fn ft_txn<T>(
    storage: &Storage,
    body: impl FnOnce(&mut crate::ObjTxn<'_, '_>) -> Result<T, StorageError>,
) -> T {
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| Ok(body(tx).unwrap_or_else(|err| panic!("crash: {err}"))))
        .unwrap()
}

/// Syncs the index for `key` as the indexer does: removes what it holds,
/// then indexes the key's latest version with `text`, a step per
/// transaction. Returns whether every step went through.
fn ft_sync(storage: &Storage, bucket: &str, key: &str, text: &str) -> bool {
    let generation = ft_generation(storage, bucket);
    loop {
        match ft_txn(storage, |tx| {
            tx.fulltext_remove_step(bucket, key, generation)
        }) {
            None => return false,
            Some(true) => break,
            Some(false) => {}
        }
    }
    let latest = ObjFile::new(&mut storage.lock())
        .head_object(bucket, key, crate::VersionSel::Latest)
        .unwrap()
        .ok()
        .map(|info| info.version);
    let Some(version) = latest else {
        return true;
    };
    let terms = truthdb_fulltext::DocTerms::from_text(text);
    let mut progress = truthdb_fulltext::Progress::default();
    loop {
        match ft_txn(storage, |tx| {
            tx.fulltext_build_step(bucket, key, generation, version, &terms, &mut progress)
        }) {
            None => return false,
            Some(true) => break,
            Some(false) => {}
        }
    }
    let chunks = progress.next_chunk;
    ft_txn(storage, |tx| {
        tx.fulltext_finish(bucket, key, generation, version, terms.len, chunks)
    })
}

#[test]
fn applying_full_text_holds_the_latest_version_only_and_survives_a_crash_at_any_op() {
    use crate::FtState;
    // Version 1 indexed; then syncing to version 2 crashes after each of
    // its page operations in turn (across its transactions: removing version
    // 1, writing version 2 a step at a time, completing it). After every
    // crash a search finds version 1 or nothing, never version 2 in part;
    // the next sync leaves exactly version 2. Version 2 has enough terms
    // that writing it takes several transactions of the 8 MiB ring's
    // budget, so the crash points are taken a stride apart.
    let v2_text: String = (0..1500).map(|n| format!("t{n} ")).collect::<String>() + "shared";
    let mut crash_at = 0;
    let mut crashes = 0;
    loop {
        let path = temp_path("ft-crash");
        let storage = create(&path);
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.create_bucket("docs", DEFAULT_ACCOUNT, now())?.unwrap();
                tx.enable_fulltext("docs", 1 << 20)
            })
            .unwrap()
            .unwrap();
        let v1 = put_text(&storage, "docs", "k", "alpha shared");
        assert!(ft_sync(&storage, "docs", "k", "alpha shared"));
        let v2 = put_text(&storage, "docs", "k", &v2_text);
        FAIL_APPLY_OPS_AFTER.with(|fail| fail.set(Some(crash_at)));
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ft_sync(&storage, "docs", "k", &v2_text)
        }))
        .is_err();
        let fired = FAIL_APPLY_OPS_AFTER
            .with(|fail| fail.replace(None))
            .is_none();
        drop(storage);
        let storage = open(&path);
        if crashed {
            assert!(fired);
            crashes += 1;
            let hits = ft_hits(&storage, "docs", "shared");
            assert!(
                hits == [v1] || hits.is_empty(),
                "crash at {crash_at}: {hits:?}"
            );
            assert!(
                ft_hits(&storage, "docs", "t42").is_empty(),
                "crash at {crash_at}"
            );
            let stats = ObjFile::new(&mut storage.lock())
                .fulltext_stats("docs")
                .unwrap();
            assert_eq!(stats.docs, hits.len() as u64, "crash at {crash_at}");
            assert!(ft_sync(&storage, "docs", "k", &v2_text));
        }
        assert_eq!(ft_hits(&storage, "docs", "shared"), [v2]);
        assert_eq!(ft_hits(&storage, "docs", "t1499"), [v2]);
        assert!(ft_hits(&storage, "docs", "alpha").is_empty());
        assert_eq!(
            ObjFile::new(&mut storage.lock())
                .fulltext_stats("docs")
                .unwrap(),
            truthdb_fulltext::Stats {
                docs: 1,
                total_len: 1501
            }
        );
        let version_key = |version| {
            ObjFile::new(&mut storage.lock())
                .fulltext_version_key("docs", version)
                .unwrap()
        };
        assert_eq!(version_key(v2).as_deref(), Some("k"));
        assert_eq!(version_key(v1), None);
        // A version that is no longer the latest is not written.
        let terms = truthdb_fulltext::DocTerms::from_text("alpha");
        let mut progress = truthdb_fulltext::Progress::default();
        assert_eq!(
            ft_txn(&storage, |tx| tx.fulltext_build_step(
                "docs",
                "k",
                1,
                v1,
                &terms,
                &mut progress
            )),
            None
        );
        assert_eq!(ft_hits(&storage, "docs", "shared"), [v2]);
        drop(storage);
        std::fs::remove_file(&path).ok();
        if !crashed {
            assert!(crashes > 20, "the sync crashed at only {crashes} points");
            break;
        }
        crash_at += 53;
    }

    // Deleting the bucket purges its index in batches; a bucket created again
    // under the name starts with none and cannot be enabled until the purge
    // is done.
    let path = temp_path("ft-purge");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("docs", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.enable_fulltext("docs", 1 << 20)
        })
        .unwrap()
        .unwrap();
    let mut keys = Vec::new();
    for n in 0..10 {
        let key = format!("k{n}");
        put_text(&storage, "docs", &key, "word");
        assert!(ft_sync(&storage, "docs", &key, "word"));
        keys.push(key);
    }
    assert_eq!(ft_hits(&storage, "docs", "word").len(), 10);
    for key in &keys {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_object("docs", key, None, false, now(), 0))
            .unwrap()
            .unwrap();
    }
    let state = |storage: &Storage| {
        ObjFile::new(&mut storage.lock())
            .fulltext_buckets()
            .unwrap()
            .into_iter()
            .map(|bucket| bucket.state)
            .collect::<Vec<_>>()
    };
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.delete_bucket("docs")?.unwrap();
            tx.create_bucket("docs", DEFAULT_ACCOUNT, now())
        })
        .unwrap()
        .unwrap();
    assert_eq!(state(&storage), [FtState::Purging]);
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.enable_fulltext("docs", 1 << 20))
            .unwrap(),
        Err(crate::FtError::Purging)
    );
    // Nothing more is applied to a purging bucket.
    assert_eq!(
        ft_txn(&storage, |tx| tx.fulltext_remove_step("docs", "k0", 1)),
        None
    );
    let mut passes = 0;
    while !ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.purge_fulltext("docs", 3))
        .unwrap()
    {
        passes += 1;
    }
    assert!(passes >= 6, "{passes} purge passes");
    assert!(state(&storage).is_empty());
    assert!(ft_hits(&storage, "docs", "word").is_empty());
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .fulltext_stats("docs")
            .unwrap(),
        truthdb_fulltext::Stats::default()
    );
    assert!(
        ObjFile::new(&mut storage.lock())
            .fulltext_key("docs", "k0")
            .unwrap()
            .is_none()
    );
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    for n in 0..20 {
        assert_eq!(file.fulltext_version_key("docs", n).unwrap(), None);
    }
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn the_journal_is_not_trimmed_past_the_indexer_while_a_bucket_is_indexed() {
    let path = temp_path("ft-journal");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("docs", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    put_text(&storage, "docs", "a", "x");
    // Enabling starts the cursor at the journal's end: the backfill covers
    // what came before.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.enable_fulltext("docs", 1 << 20))
        .unwrap()
        .unwrap();
    let cursor = |storage: &Storage| ObjFile::new(&mut storage.lock()).fulltext_cursor().unwrap();
    assert_eq!(cursor(&storage), 1);
    put_text(&storage, "docs", "b", "y");
    put_text(&storage, "docs", "c", "z");
    let trim = |storage: &Storage| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.trim_journal(now() + 1, 1, u64::MAX, 100))
            .unwrap()
    };
    let due = |storage: &Storage| {
        ObjFile::new(&mut storage.lock())
            .journal_due(now() + 1, 1, u64::MAX)
            .unwrap()
    };
    assert!(due(&storage));
    assert_eq!(trim(&storage), 1, "only the row the indexer has passed");
    assert!(
        !due(&storage),
        "nothing more is due: an idle sweep writes nothing"
    );
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.set_fulltext_cursor(2))
        .unwrap();
    assert_eq!(trim(&storage), 1);
    // Disabled and purged: nothing holds the journal back.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.disable_fulltext("docs"))
        .unwrap()
        .unwrap();
    assert_eq!(trim(&storage), 0, "a purging bucket still holds it");
    assert!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.purge_fulltext("docs", 100))
            .unwrap()
    );
    assert_eq!(trim(&storage), 1);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn the_full_text_index_ships_to_a_standby_and_restores_from_a_log_backup() {
    let path = temp_path("ft-ship-src");
    let bak = temp_path("ft-ship-bak");
    let log = temp_path("ft-ship-log");
    let seed = temp_path("ft-ship-seed");
    let dst = temp_path("ft-ship-dst");
    let primary = create(&path);
    primary.lock().commit_db_options(0, Some(true)).unwrap();
    let summary = primary.backup_full(&bak).unwrap();
    ObjFile::new(&mut primary.lock())
        .transaction(|tx| {
            tx.create_bucket("docs", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.enable_fulltext("docs", 1 << 20)
        })
        .unwrap()
        .unwrap();
    let a = put_text(&primary, "docs", "a", "north wind");
    put_text(&primary, "docs", "b", "south wind");
    assert!(ft_sync(&primary, "docs", "a", "north wind"));
    assert!(ft_sync(&primary, "docs", "b", "south wind"));
    let b2 = put_text(&primary, "docs", "b", "east");
    assert!(ft_sync(&primary, "docs", "b", "east"));
    let expected = (
        ft_hits(&primary, "docs", "wind"),
        ft_hits(&primary, "docs", "east"),
    );
    assert_eq!(expected, (vec![a], vec![b2]));
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
        (
            ft_hits(&standby, "docs", "wind"),
            ft_hits(&standby, "docs", "east")
        ),
        expected
    );
    drop(standby);

    Storage::restore_full_with_logs(&dst, &bak, &[log.clone()], None, engines).unwrap();
    let restored = open(&dst);
    assert_eq!(
        (
            ft_hits(&restored, "docs", "wind"),
            ft_hits(&restored, "docs", "east")
        ),
        expected
    );
    drop(restored);
    drop(primary);
    for p in [&path, &bak, &log, &seed, &dst] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn a_search_leaves_out_a_document_being_written_on_every_term() {
    // A document written a transaction at a time, terms in order: a search
    // reading its postings after the first step sees "aaa" but not yet
    // "zzz". Read then, it must not match "aaa AND NOT zzz" once the
    // document is complete (as it would if it were decided only after).
    let path = temp_path("ft-race");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("docs", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.enable_fulltext("docs", 1 << 20)
        })
        .unwrap()
        .unwrap();
    let text =
        String::from("aaa ") + &(0..1500).map(|n| format!("t{n} ")).collect::<String>() + "zzz";
    let version = put_text(&storage, "docs", "k", &text);
    let terms = truthdb_fulltext::DocTerms::from_text(&text);
    let mut progress = truthdb_fulltext::Progress::default();
    assert_eq!(
        ft_txn(&storage, |tx| tx.fulltext_build_step(
            "docs",
            "k",
            1,
            version,
            &terms,
            &mut progress
        )),
        Some(false)
    );
    // The search reads every posting now, mid-write.
    let mut search = truthdb_fulltext::Search::new("aaa AND NOT zzz").unwrap();
    let ranked = {
        let mut guard = storage.lock();
        let mut file = ObjFile::new(&mut guard);
        assert!(file.fulltext_step(&mut search, "docs", usize::MAX).unwrap());
        search.rank(file.fulltext_stats("docs").unwrap())
    };
    // The document completes before the search checks its hits.
    loop {
        match ft_txn(&storage, |tx| {
            tx.fulltext_build_step("docs", "k", 1, version, &terms, &mut progress)
        }) {
            Some(true) => break,
            Some(false) => {}
            None => panic!("not writable"),
        }
    }
    let chunks = progress.next_chunk;
    assert!(ft_txn(&storage, |tx| tx
        .fulltext_finish("docs", "k", 1, version, terms.len, chunks)));
    assert!(ranked.is_empty(), "{ranked:?}");
    assert_eq!(ft_hits(&storage, "docs", "aaa AND zzz"), [version]);
    assert!(ft_hits(&storage, "docs", "aaa AND NOT zzz").is_empty());
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn bucket_configs_and_user_policies_are_kept_replaced_and_dropped() {
    use crate::{AccountError, BUCKET_POLICY};
    let path = temp_path("documents");
    let storage = create(&path);
    let big: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
    {
        let mut guard = storage.lock();
        let mut file = ObjFile::new(&mut guard);
        file.transaction(|tx| {
            tx.create_bucket("docs", DEFAULT_ACCOUNT, 1)?.unwrap();
            tx.create_bucket("docs2", DEFAULT_ACCOUNT, 1)?.unwrap();
            tx.put_bucket_config("docs", BUCKET_POLICY, "", &big)?;
            tx.put_bucket_config("docs2", BUCKET_POLICY, "", b"other")?;
            tx.create_account("111111111111", "acme", 1)?.unwrap();
            tx.create_user("111111111111", "alice", "AIDAA", 1)?
                .unwrap();
            tx.create_user("111111111111", "alicia", "AIDAB", 1)?
                .unwrap();
            tx.put_user_policy("111111111111", "alice", "read", b"{\"r\":1}")?;
            tx.put_user_policy("111111111111", "alice", "write", &big)?;
            tx.put_user_policy("111111111111", "alicia", "x", b"{}")?;
            assert!(
                tx.put_user_policy("111111111111", "alice", "bad name", b"{}")
                    .is_err()
            );
            Ok(())
        })
        .unwrap();
    }
    drop(storage); // crash
    let storage = open(&path);
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    assert_eq!(
        file.bucket_config("docs", BUCKET_POLICY, "").unwrap(),
        Some(big.clone())
    );
    let names = |file: &mut ObjFile<'_>, user: &str| -> Vec<String> {
        file.user_policies("111111111111", user)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    };
    assert_eq!(names(&mut file, "alice"), ["read", "write"]);
    assert_eq!(
        file.user_policies("111111111111", "alice").unwrap()[1].1,
        big
    );
    assert_eq!(names(&mut file, "alicia"), ["x"]);
    // Replaced, then gone with its bucket; the other bucket's stays.
    file.transaction(|tx| tx.put_bucket_config("docs", BUCKET_POLICY, "", b"short"))
        .unwrap();
    assert_eq!(
        file.bucket_config("docs", BUCKET_POLICY, "")
            .unwrap()
            .as_deref(),
        Some(&b"short"[..])
    );
    file.transaction(|tx| tx.delete_bucket("docs"))
        .unwrap()
        .unwrap();
    assert_eq!(file.bucket_config("docs", BUCKET_POLICY, "").unwrap(), None);
    assert_eq!(
        file.bucket_config("docs2", BUCKET_POLICY, "")
            .unwrap()
            .as_deref(),
        Some(&b"other"[..])
    );
    // A user goes only once its policies have.
    assert_eq!(
        file.transaction(|tx| tx.drop_user("111111111111", "alice"))
            .unwrap(),
        Err(AccountError::NotEmpty("policies"))
    );
    for name in ["read", "write"] {
        assert!(
            file.transaction(|tx| tx.delete_user_policy("111111111111", "alice", name))
                .unwrap()
        );
    }
    assert!(
        !file
            .transaction(|tx| tx.delete_user_policy("111111111111", "alice", "read"))
            .unwrap()
    );
    assert_eq!(
        file.transaction(|tx| tx.drop_user("111111111111", "alice"))
            .unwrap(),
        Ok(())
    );
    assert_eq!(names(&mut file, "alicia"), ["x"]);
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn managed_policies_attach_to_users_and_go_only_when_detached() {
    use crate::AccountError;
    let path = temp_path("managed");
    let storage = create(&path);
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    let acct = "111111111111";
    file.transaction(|tx| {
        tx.create_account(acct, "acme", 1)?.unwrap();
        tx.create_user(acct, "alice", "AIDAA", 1)?.unwrap();
        assert_eq!(
            tx.create_user(acct, "ALICE", "AIDAB", 1)?,
            Err(AccountError::UserExists),
            "names compare without case"
        );
        tx.create_user(acct, "bob", "AIDAC", 1)?.unwrap();
        tx.create_managed_policy(acct, "read", "ANPAR", b"{\"r\":1}", 2)?
            .unwrap();
        assert_eq!(
            tx.create_managed_policy(acct, "read", "ANPAX", b"{}", 2)?,
            Err(AccountError::PolicyExists)
        );
        tx.create_managed_policy(acct, "write", "ANPAW", &vec![b'w'; 4000], 3)?
            .unwrap();
        tx.attach_user_policy(acct, "alice", "read")?.unwrap();
        tx.attach_user_policy(acct, "alice", "read")?.unwrap();
        tx.attach_user_policy(acct, "bob", "read")?.unwrap();
        assert_eq!(
            tx.attach_user_policy(acct, "alice", "none")?,
            Err(AccountError::NoSuchPolicy)
        );
        tx.create_access_key("K1", "s1", acct, Some("bob"), 4)?
            .unwrap();
        Ok(())
    })
    .unwrap();
    let read = file.managed_policy(acct, "read").unwrap().unwrap();
    assert_eq!(
        (read.policy_id.as_str(), read.created_ms, &read.document[..]),
        ("ANPAR", 2, &b"{\"r\":1}"[..])
    );
    let listed: Vec<(String, u32)> = file
        .managed_policies(acct)
        .unwrap()
        .into_iter()
        .map(|(policy, count)| (policy.name, count))
        .collect();
    assert_eq!(listed, [("read".to_string(), 2), ("write".to_string(), 0)]);
    assert_eq!(
        file.managed_policy(acct, "write")
            .unwrap()
            .unwrap()
            .document
            .len(),
        4000
    );
    assert_eq!(file.attached_policies(acct, "alice").unwrap(), ["read"]);
    // A policy goes only once detached; a user only without attachments.
    assert_eq!(
        file.transaction(|tx| tx.delete_managed_policy(acct, "read"))
            .unwrap(),
        Err(AccountError::NotEmpty("attachments"))
    );
    assert_eq!(
        file.transaction(|tx| tx.drop_user(acct, "alice")).unwrap(),
        Err(AccountError::NotEmpty("policies"))
    );
    for user in ["alice", "bob"] {
        assert!(
            file.transaction(|tx| tx.detach_user_policy(acct, user, "read"))
                .unwrap()
        );
    }
    assert!(
        !file
            .transaction(|tx| tx.detach_user_policy(acct, "bob", "read"))
            .unwrap()
    );
    assert_eq!(
        file.transaction(|tx| tx.drop_user(acct, "alice")).unwrap(),
        Ok(())
    );
    assert_eq!(
        file.transaction(|tx| tx.delete_managed_policy(acct, "read"))
            .unwrap(),
        Ok(())
    );
    // Key status.
    assert!(
        file.transaction(|tx| tx.set_access_key_status("K1", false))
            .unwrap()
    );
    assert!(!file.access_key("K1").unwrap().unwrap().0.active);
    assert!(
        !file
            .transaction(|tx| tx.set_access_key_status("none", false))
            .unwrap()
    );
    // An account with policies left is not dropped.
    file.transaction(|tx| tx.drop_access_key("K1")).unwrap();
    file.transaction(|tx| tx.drop_user(acct, "bob"))
        .unwrap()
        .unwrap();
    assert_eq!(
        file.transaction(|tx| tx.drop_account(acct)).unwrap(),
        Err(AccountError::NotEmpty("policies"))
    );
    file.transaction(|tx| tx.delete_managed_policy(acct, "write"))
        .unwrap()
        .unwrap();
    assert_eq!(
        file.transaction(|tx| tx.drop_account(acct)).unwrap(),
        Ok(())
    );
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn object_lock_keeps_protected_versions_until_their_retention_or_hold_allows() {
    use crate::{
        BucketError, ObjectError, ObjectLock, Retention, RetentionMode, VersionSel, Versioning,
    };
    let path = temp_path("object-lock");
    let storage = create(&path);
    let txn = |body: &mut dyn FnMut(&mut crate::ObjTxn<'_, '_>) -> Result<_, StorageError>| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| body(tx))
            .unwrap()
    };
    txn(&mut |tx| {
        tx.create_bucket("bkt", DEFAULT_ACCOUNT, now())?.unwrap();
        tx.create_bucket("plain", DEFAULT_ACCOUNT, now())?.unwrap();
        Ok(Ok(()))
    })
    .unwrap();
    // Object Lock needs versioning enabled, and keeps it so.
    assert!(matches!(
        txn(&mut |tx| tx.enable_object_lock("bkt")),
        Err(BucketError::InvalidBucketState(_))
    ));
    txn(&mut |tx| tx.set_versioning("bkt", Versioning::Enabled)).unwrap();
    txn(&mut |tx| tx.enable_object_lock("bkt")).unwrap();
    assert!(matches!(
        txn(&mut |tx| tx.set_versioning("bkt", Versioning::Suspended)),
        Err(BucketError::InvalidBucketState(_))
    ));
    let bucket = ObjFile::new(&mut storage.lock())
        .bucket("bkt")
        .unwrap()
        .unwrap();
    assert!(bucket.object_lock);
    assert_eq!(bucket.versioning, Versioning::Enabled);

    let v1 = put_text(&storage, "bkt", "k", "one");
    let delete = |which: Option<VersionSel>, bypass: bool, at: u64| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_object("bkt", "k", which, bypass, at, 0))
            .unwrap()
    };
    let retain = |which: VersionSel, retention: Option<Retention>, bypass: bool| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                let change = match retention {
                    Some(retention) => crate::RetentionChange::Set(retention),
                    None => crate::RetentionChange::Remove,
                };
                tx.put_retention("bkt", "k", which, change, bypass, now())
            })
            .unwrap()
    };
    let governance = |until_ms| Retention {
        mode: RetentionMode::Governance,
        until_ms,
        event: None,
    };
    let compliance = |until_ms| Retention {
        mode: RetentionMode::Compliance,
        until_ms,
        event: None,
    };
    let until = now() + 1_000;
    assert_eq!(
        retain(VersionSel::Seq(v1), Some(governance(until)), false),
        Ok((v1, false))
    );
    // A protected version stays; a delete marker may still hide it.
    assert_eq!(
        delete(Some(VersionSel::Seq(v1)), false, now()),
        Err(ObjectError::Locked)
    );
    let marker = delete(None, false, now()).unwrap().version.unwrap();
    delete(Some(VersionSel::Seq(marker)), false, now()).unwrap();
    // Governance retention: shortened, removed or ignored only with bypass.
    assert_eq!(
        retain(VersionSel::Seq(v1), Some(governance(until - 1)), false),
        Err(ObjectError::Locked)
    );
    assert_eq!(
        retain(VersionSel::Seq(v1), Some(compliance(until)), false),
        Err(ObjectError::Locked),
        "a mode change alters the lock"
    );
    assert_eq!(
        retain(VersionSel::Seq(v1), Some(governance(until + 5)), false),
        Ok((v1, false)),
        "anyone allowed to set retention may extend it"
    );
    assert_eq!(
        retain(VersionSel::Seq(v1), Some(governance(until)), true),
        Ok((v1, false))
    );
    // A legal hold protects whatever the retention, bypass or not.
    let hold = |which: VersionSel, on: bool| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_legal_hold("bkt", "k", which, on))
            .unwrap()
    };
    assert_eq!(hold(VersionSel::Latest, true), Ok((v1, false)));
    assert_eq!(
        delete(Some(VersionSel::Seq(v1)), true, now()),
        Err(ObjectError::Locked)
    );
    assert_eq!(
        delete(Some(VersionSel::Seq(v1)), true, until + 1),
        Err(ObjectError::Locked),
        "a legal hold outlives the retention"
    );
    hold(VersionSel::Latest, false).unwrap();
    assert_eq!(
        ObjFile::new(&mut storage.lock()).object_lock(v1).unwrap(),
        ObjectLock {
            retention: Some(governance(until)),
            legal_hold: Some(false),
        }
    );
    // Compliance: extended only, never bypassed.
    let v2 = put_text(&storage, "bkt", "k", "two");
    retain(VersionSel::Seq(v2), Some(compliance(until)), false).unwrap();
    for retention in [
        None,
        Some(compliance(until - 1)),
        Some(governance(until + 9)),
    ] {
        assert_eq!(
            retain(VersionSel::Seq(v2), retention, true),
            Err(ObjectError::Locked),
            "{retention:?}"
        );
    }
    assert_eq!(
        delete(Some(VersionSel::Seq(v2)), true, now()),
        Err(ObjectError::Locked)
    );
    // The locks hold across a restart.
    drop(storage);
    let storage = open(&path);
    let delete = |which: Option<VersionSel>, bypass: bool, at: u64| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_object("bkt", "k", which, bypass, at, 0))
            .unwrap()
    };
    assert_eq!(
        delete(Some(VersionSel::Seq(v2)), true, now()),
        Err(ObjectError::Locked)
    );
    // Governance is bypassed; past its date, compliance lets go too, and
    // the lock goes with the version.
    delete(Some(VersionSel::Seq(v1)), true, now()).unwrap();
    assert_eq!(
        ObjFile::new(&mut storage.lock()).object_lock(v1).unwrap(),
        ObjectLock::default()
    );
    delete(Some(VersionSel::Seq(v2)), false, until).unwrap();
    assert_eq!(
        ObjFile::new(&mut storage.lock()).object_lock(v2).unwrap(),
        ObjectLock::default()
    );
    // Lock changes need a bucket with Object Lock, and a live version.
    put_text(&storage, "plain", "k", "x");
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_legal_hold("plain", "k", VersionSel::Latest, true))
            .unwrap(),
        Err(ObjectError::ObjectLockNotEnabled)
    );
    let marker = delete(None, false, now()).unwrap().version.unwrap();
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_legal_hold("bkt", "k", VersionSel::Seq(marker), true))
            .unwrap(),
        Err(ObjectError::DeleteMarker)
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn an_uploads_lock_passes_to_its_object_or_goes_with_the_abort() {
    use crate::{ObjectAttrs, ObjectError, ObjectLock, VersionSel, Versioning};
    let path = temp_path("upload-lock");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("bkt", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.set_versioning("bkt", Versioning::Enabled)?.unwrap();
            tx.enable_object_lock("bkt")
        })
        .unwrap()
        .unwrap();
    let held = ObjectLock {
        retention: None,
        legal_hold: Some(true),
    };
    let start = || {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                let upload = tx
                    .create_upload(
                        "bkt",
                        "k",
                        &ObjectAttrs::default(),
                        crate::StorageClass::Standard,
                        now(),
                    )?
                    .unwrap();
                tx.set_upload_lock(upload, &held)?;
                Ok(upload)
            })
            .unwrap()
    };
    let aborted = start();
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.abort_upload(aborted, now(), 0))
        .unwrap()
        .unwrap();
    let upload = start();
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    writer.write(&storage, b"part", now()).unwrap();
    let sealed = writer.finish(&storage, now()).unwrap();
    let md5 = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.upload_part(upload, 1, &sealed, now(), 0))
        .unwrap()
        .unwrap()
        .md5;
    let object = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.complete_upload(upload, &[(1, md5)], now(), 0))
        .unwrap()
        .unwrap();
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .object_lock(object.version)
            .unwrap(),
        held
    );
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.delete_object(
                    "bkt",
                    "k",
                    Some(VersionSel::Seq(object.version)),
                    true,
                    now(),
                    0,
                )
            })
            .unwrap(),
        Err(ObjectError::Locked)
    );
    // Nothing of either upload's lock is left behind.
    let mut guard = storage.lock();
    let file = ObjFile::new(&mut guard);
    let locks = file.existing_tree(crate::LOCKS_TREE).unwrap().unwrap();
    let rows = locks.scan(&mut file.core.page_ctx(0)).unwrap();
    assert_eq!(rows.len(), 1, "only the object's");
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_bucket_row_has_its_object_lock_byte_only_when_enabled() {
    use crate::Versioning;
    use crate::buckets::BucketRow;
    let mut row = BucketRow {
        created_ms: 1,
        versioning: Versioning::Enabled,
        journal_from: 2,
        owner: DEFAULT_ACCOUNT.to_string(),
        object_lock: false,
    };
    assert_eq!(row.encode().len(), 29, "readable by earlier releases");
    assert!(!BucketRow::decode(&row.encode()).unwrap().object_lock);
    row.object_lock = true;
    assert_eq!(row.encode().len(), 30);
    assert!(BucketRow::decode(&row.encode()).unwrap().object_lock);
    let mut bad = row.encode();
    bad[29] = 2;
    assert!(BucketRow::decode(&bad).is_err());
}

#[test]
fn an_event_hold_keeps_a_version_until_its_duration_after_release() {
    use crate::{
        EventHold, ObjectError, Retention, RetentionChange, RetentionMode, VersionSel, Versioning,
    };
    const DAY: u64 = 86_400_000;
    let path = temp_path("event-hold");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("bkt", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.set_versioning("bkt", Versioning::Enabled)?.unwrap();
            tx.enable_object_lock("bkt")
        })
        .unwrap()
        .unwrap();
    let held = |mode, days: u32, until_ms: u64| Retention {
        mode,
        until_ms,
        event: Some(EventHold {
            on: true,
            years: false,
            count: days,
        }),
    };
    let change = |version: u64, change: RetentionChange, bypass: bool, at: u64| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_retention("bkt", "k", VersionSel::Seq(version), change, bypass, at)
            })
            .unwrap()
    };
    let retention_of = |version: u64| {
        ObjFile::new(&mut storage.lock())
            .object_lock(version)
            .unwrap()
            .retention
            .unwrap()
    };
    let delete = |version: u64, bypass: bool, at: u64| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.delete_object("bkt", "k", Some(VersionSel::Seq(version)), bypass, at, 0)
            })
            .unwrap()
    };
    let t = now();
    let v = put_text(&storage, "bkt", "k", "held");
    // Releasing what is not held is refused.
    assert!(matches!(
        change(
            v,
            RetentionChange::Release {
                mode: RetentionMode::Compliance,
                until_ms: 0,
            },
            false,
            t
        ),
        Err(ObjectError::InvalidRetention(_))
    ));
    change(
        v,
        RetentionChange::Set(held(RetentionMode::Compliance, 10, 0)),
        false,
        t,
    )
    .unwrap();
    // Held, the version stays however late, bypass or not.
    assert_eq!(delete(v, true, t + 1_000 * DAY), Err(ObjectError::Locked));
    assert_eq!(retention_of(v).effective_until(t + DAY), t + 11 * DAY);
    // A shorter duration never moves the date back.
    change(
        v,
        RetentionChange::Set(held(RetentionMode::Compliance, 1, 0)),
        false,
        t + DAY,
    )
    .unwrap();
    assert_eq!(retention_of(v).effective_until(t + DAY), t + 11 * DAY);
    assert_eq!(retention_of(v).effective_until(t + 20 * DAY), t + 21 * DAY);
    // Neither a mode change nor an earlier fixed date passes, bypass or not.
    for retention in [
        held(RetentionMode::Governance, 10, 0),
        Retention {
            mode: RetentionMode::Compliance,
            until_ms: t + 5 * DAY,
            event: None,
        },
    ] {
        assert_eq!(
            change(v, RetentionChange::Set(retention), true, t + DAY),
            Err(ObjectError::Locked),
            "{retention:?}"
        );
    }
    assert_eq!(
        change(v, RetentionChange::Remove, true, t + DAY),
        Err(ObjectError::Locked)
    );
    // Released at day 30 (a day's duration now): kept until day 31.
    change(
        v,
        RetentionChange::Release {
            mode: RetentionMode::Compliance,
            until_ms: 0,
        },
        false,
        t + 30 * DAY,
    )
    .unwrap();
    let released = retention_of(v);
    assert_eq!(released.until_ms, t + 31 * DAY);
    assert!(!released.event.unwrap().on);
    assert_eq!(released.effective_until(t + 100 * DAY), t + 31 * DAY);
    assert_eq!(delete(v, false, t + 31 * DAY - 1), Err(ObjectError::Locked));
    delete(v, false, t + 31 * DAY).unwrap();

    // A minimum date outlasts an early release; GOVERNANCE gives way to a
    // bypass, and a fixed date at least the current one releases the hold.
    let w = put_text(&storage, "bkt", "k", "minimum");
    change(
        w,
        RetentionChange::Set(held(RetentionMode::Governance, 1, t + 50 * DAY)),
        false,
        t,
    )
    .unwrap();
    change(
        w,
        RetentionChange::Release {
            mode: RetentionMode::Governance,
            until_ms: 0,
        },
        false,
        t + 2 * DAY,
    )
    .unwrap();
    assert_eq!(retention_of(w).until_ms, t + 50 * DAY);
    let fixed = Retention {
        mode: RetentionMode::Governance,
        until_ms: t + 60 * DAY,
        event: None,
    };
    change(w, RetentionChange::Set(fixed), false, t + 2 * DAY).unwrap();
    assert_eq!(retention_of(w), fixed);
    change(
        w,
        RetentionChange::Set(held(RetentionMode::Governance, 3, 0)),
        false,
        t + 2 * DAY,
    )
    .unwrap();
    assert_eq!(
        retention_of(w).until_ms,
        t + 60 * DAY,
        "a hold placed on a fixed date keeps the date as its minimum"
    );
    change(w, RetentionChange::Remove, true, t + 2 * DAY).unwrap();
    delete(w, false, t + 2 * DAY).unwrap();
    // A release naming a date keeps the later of it and the duration; on a
    // version not held, the date is a fixed retention.
    let r = put_text(&storage, "bkt", "k", "release-minimum");
    change(
        r,
        RetentionChange::Set(held(RetentionMode::Governance, 30, 0)),
        false,
        t,
    )
    .unwrap();
    let release = |until_ms| RetentionChange::Release {
        mode: RetentionMode::Governance,
        until_ms,
    };
    change(r, release(t + DAY), false, t).unwrap();
    assert_eq!(retention_of(r).until_ms, t + 30 * DAY);
    change(r, RetentionChange::Remove, true, t).unwrap();
    change(
        r,
        RetentionChange::Set(held(RetentionMode::Governance, 30, 0)),
        false,
        t,
    )
    .unwrap();
    change(r, release(t + 60 * DAY), false, t).unwrap();
    assert_eq!(retention_of(r).until_ms, t + 60 * DAY);
    change(r, RetentionChange::Remove, true, t).unwrap();
    change(r, release(t + 5 * DAY), false, t).unwrap();
    let fixed = retention_of(r);
    assert_eq!((fixed.until_ms, fixed.event), (t + 5 * DAY, None));
    change(r, RetentionChange::Remove, true, t).unwrap();
    // Held in years: 365 days each.
    let y = put_text(&storage, "bkt", "k", "years");
    let years = Retention {
        mode: RetentionMode::Governance,
        until_ms: 0,
        event: Some(EventHold {
            on: true,
            years: true,
            count: 2,
        }),
    };
    change(y, RetentionChange::Set(years), false, t).unwrap();
    assert_eq!(retention_of(y).effective_until(t), t + 730 * DAY);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_consumer_that_stops_reading_holds_the_journal_only_until_the_cap() {
    use crate::{Consumer, FULLTEXT_CONSUMER, FtState};
    let path = temp_path("journal-cap");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("docs", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    put_text(&storage, "docs", "a", "x");
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.enable_fulltext("docs", 1 << 20))
        .unwrap()
        .unwrap();
    put_text(&storage, "docs", "b", "y");
    put_text(&storage, "docs", "c", "z");
    let consumers = |storage: &Storage| {
        ObjFile::new(&mut storage.lock())
            .journal_consumers()
            .unwrap()
    };
    assert_eq!(
        consumers(&storage),
        [Consumer {
            name: FULLTEXT_CONSUMER.to_string(),
            cursor: 1,
            dropped: 0,
        }]
    );
    // The indexer's backfill finishes; then it stops reading.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.set_fulltext_backfill("docs", 1, None))
        .unwrap();
    let trim = |storage: &Storage, at: u64, cap: u64| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.trim_journal(at, 1, cap, 100))
            .unwrap()
    };
    assert_eq!(trim(&storage, now() + 1, 100), 1, "held at the cursor");
    assert!(
        !ObjFile::new(&mut storage.lock())
            .journal_due(now() + 1, 1, 100)
            .unwrap()
    );
    // Past the cap the rows go anyway: the consumer is moved past them, the
    // rows it missed counted, and the index rebuilt by backfill.
    assert!(
        ObjFile::new(&mut storage.lock())
            .journal_due(now() + 100, 1, 100)
            .unwrap()
    );
    assert_eq!(trim(&storage, now() + 100, 100), 2);
    assert_eq!(
        consumers(&storage),
        [Consumer {
            name: FULLTEXT_CONSUMER.to_string(),
            cursor: 3,
            dropped: 2,
        }]
    );
    let bucket = ObjFile::new(&mut storage.lock())
        .fulltext_buckets()
        .unwrap();
    assert_eq!(bucket[0].state, FtState::Backfilling(None));
    assert_eq!(bucket[0].generation, 2);
    // An indexer that applied rows the trim has since moved it past keeps
    // the trim's place.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.set_fulltext_cursor(2))
        .unwrap();
    assert_eq!(consumers(&storage)[0].cursor, 3);
    // The places survive a restart.
    drop(storage);
    let storage = open(&path);
    assert_eq!(consumers(&storage)[0].dropped, 2);
    // The last indexed bucket purged, nothing holds the journal.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.disable_fulltext("docs"))
        .unwrap()
        .unwrap();
    assert!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.purge_fulltext("docs", 100))
            .unwrap()
    );
    assert_eq!(consumers(&storage), []);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn an_indexer_cursor_from_before_consumers_holds_the_journal_and_moves_to_its_row() {
    use crate::{Consumer, FT_CURSOR_KEY, FULLTEXT_CONSUMER};
    let path = temp_path("journal-legacy");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("docs", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    put_text(&storage, "docs", "a", "x");
    put_text(&storage, "docs", "b", "y");
    put_text(&storage, "docs", "c", "z");
    // A file written before consumers: the indexer's cursor (1) in the
    // directory tree, no consumer row.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.enable_fulltext("docs", 1 << 20)?.unwrap();
            tx.trees.consumers().delete(
                tx.ctx,
                &mut OpMode::Txn(tx.txn),
                FULLTEXT_CONSUMER.as_bytes(),
            )?;
            tx.trees.meta().insert_unique(
                tx.ctx,
                &mut OpMode::Txn(tx.txn),
                FT_CURSOR_KEY,
                &1u64.to_le_bytes(),
            )?;
            Ok(())
        })
        .unwrap();
    let legacy = Consumer {
        name: FULLTEXT_CONSUMER.to_string(),
        cursor: 1,
        dropped: 0,
    };
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .journal_consumers()
            .unwrap(),
        [legacy]
    );
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.trim_journal(now() + 1, 1, u64::MAX, 100))
            .unwrap(),
        1,
        "held at the old cursor"
    );
    // Its next step keeps its place in a consumer row, not the directory.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.set_fulltext_cursor(2))
        .unwrap();
    let mut guard = storage.lock();
    let file = ObjFile::new(&mut guard);
    let meta = crate::meta_tree(file.eng.root.unwrap());
    assert_eq!(
        meta.get(&mut file.core.page_ctx(0), FT_CURSOR_KEY).unwrap(),
        None
    );
    drop(guard);
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .journal_consumers()
            .unwrap()[0]
            .cursor,
        2
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

/// Bytes zstd cannot shrink (a xorshift stream).
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

/// Runs the pack queue dry, as the object jobs do; returns the steps taken.
fn pack_all(storage: &Storage) -> usize {
    use crate::PackStep;
    let mut steps = 0;
    loop {
        let step = ObjFile::new(&mut storage.lock()).next_pack().unwrap();
        match step {
            None => return steps,
            Some(PackStep::Finish { body, level }) => {
                ObjFile::new(&mut storage.lock())
                    .transaction(|tx| tx.finish_pack(body, level))
                    .unwrap();
            }
            Some(PackStep::Unreadable {
                body, level, index, ..
            }) => {
                ObjFile::new(&mut storage.lock())
                    .transaction(|tx| tx.drop_pack(body, level, index))
                    .unwrap();
            }
            Some(PackStep::Segment(work)) => {
                let frame = work.compress();
                ObjFile::new(&mut storage.lock())
                    .transaction(|tx| tx.apply_pack(&work, frame))
                    .unwrap();
            }
        }
        // The log is small in these tests.
        storage.write_checkpoint(b"x", 1, 2, 1).unwrap();
        steps += 1;
    }
}

fn put_classed(
    storage: &Storage,
    bucket: &str,
    key: &str,
    data: &[u8],
    class: crate::StorageClass,
) -> crate::ObjectInfo {
    let mut writer = BodyWriter::begin(storage, now()).unwrap();
    for chunk in data.chunks(300_000) {
        writer.write(storage, chunk, now()).unwrap();
    }
    let sealed = writer.finish(storage, now()).unwrap();
    storage.write_checkpoint(b"x", 1, 2, 1).unwrap();
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.put_object(
                bucket,
                key,
                &sealed,
                &crate::ObjectAttrs::default(),
                class,
                now(),
                0,
            )
        })
        .unwrap()
        .unwrap()
}

#[test]
fn a_cold_body_is_packed_in_place_and_reads_back_whole_and_by_range() {
    use crate::{StorageClass, VersionSel};
    let path = temp_path("pack");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    // Two segments that shrink, one that does not, and a short tail.
    let seg = SEGMENT_BYTES;
    let data: Vec<u8> = [bytes(1, seg), noise(7, seg), bytes(2, seg), bytes(3, 1234)].concat();
    let warm = put_classed(&storage, "bkt", "warm", &data, StorageClass::Standard);
    assert_eq!(pack_all(&storage), 0, "STANDARD is not packed");
    let before = allocated_pages(&storage);
    let cold = put_classed(&storage, "bkt", "cold", &data, StorageClass::Glacier);
    let written = allocated_pages(&storage) - before;
    assert_eq!(pack_all(&storage), 4, "a step per segment");
    let packed = allocated_pages(&storage) - before;
    assert!(
        packed < written / 2,
        "{packed} pages packed, {written} written"
    );
    for (offset, len) in [
        (0, data.len()),
        (0, 10),
        (seg as u64 - 5, 10),
        (2 * seg as u64 - 3, seg + 6),
        (3 * seg as u64 + 1000, 234),
    ] {
        assert_eq!(
            read(&storage, cold.body, offset, len).unwrap(),
            data[offset as usize..offset as usize + len],
            "{offset}+{len}"
        );
    }
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    assert_eq!(
        file.head_object("bkt", "cold", VersionSel::Latest)
            .unwrap()
            .unwrap()
            .storage_class,
        StorageClass::Glacier
    );
    drop(guard);
    // The warm copy was never touched.
    assert_eq!(read(&storage, warm.body, 0, data.len()).unwrap(), data);
    // A packed body is not packed again at a lower level; a colder class
    // packs it again.
    copy_classed(&storage, "ia", StorageClass::StandardIa);
    assert_eq!(pack_all(&storage), 0);
    copy_classed(&storage, "deep", StorageClass::DeepArchive);
    assert_eq!(pack_all(&storage), 4);
    assert_eq!(read(&storage, cold.body, 0, data.len()).unwrap(), data);
    // The packed pages survive a restart.
    drop(storage);
    let storage = open(&path);
    assert_eq!(read(&storage, cold.body, 0, data.len()).unwrap(), data);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

/// Copies `bkt/cold` to `bkt/<key>` in `class`, sharing its body.
fn copy_classed(storage: &Storage, key: &str, class: crate::StorageClass) {
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    let source = file
        .head_object("bkt", "cold", crate::VersionSel::Latest)
        .unwrap()
        .unwrap();
    file.transaction(|tx| {
        tx.copy_object(
            "bkt",
            key,
            &source,
            &crate::ObjectAttrs::default(),
            class,
            now(),
            0,
        )
    })
    .unwrap()
    .unwrap();
}

#[test]
fn a_pack_step_left_behind_by_a_change_or_a_restart_does_no_harm() {
    use crate::{CustomerKey, ObjectAttrs, PackStep, StorageClass};
    let path = temp_path("pack-stale");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    let data = [bytes(4, SEGMENT_BYTES), bytes(5, 99)].concat();
    let cold = put_classed(&storage, "bkt", "cold", &data, StorageClass::StandardIa);
    // A step read, then the work applied twice: the second finds its segment
    // changed and does nothing.
    let Some(PackStep::Segment(work)) = ObjFile::new(&mut storage.lock()).next_pack().unwrap()
    else {
        panic!("a segment to pack");
    };
    let frame = work.compress();
    for _ in 0..2 {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.apply_pack(&work, frame.clone()))
            .unwrap();
    }
    // A restart mid-body: the queue resumes where it was.
    drop(storage);
    let storage = open(&path);
    assert_eq!(pack_all(&storage), 1, "the second segment only");
    assert_eq!(read(&storage, cold.body, 0, data.len()).unwrap(), data);
    // A body deleted while queued: its entry goes, nothing else.
    let gone = put_classed(&storage, "bkt", "gone", &data, StorageClass::OnezoneIa);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.delete_object("bkt", "gone", None, false, now(), 0))
        .unwrap()
        .unwrap();
    assert_eq!(pack_all(&storage), 1, "finished, not packed");
    let _ = gone;
    // A body under a customer-provided key is not queued.
    let mut writer = BodyWriter::begin(&storage, now()).unwrap();
    writer.write(&storage, &data, now()).unwrap();
    let sealed = writer.finish(&storage, now()).unwrap();
    let attrs = ObjectAttrs {
        customer_key: Some(CustomerKey {
            key_md5: "m".into(),
            verifier: "v".into(),
        }),
        ..ObjectAttrs::default()
    };
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.put_object(
                "bkt",
                "keyed",
                &sealed,
                &attrs,
                StorageClass::Glacier,
                now(),
                0,
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(pack_all(&storage), 0);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_cold_multipart_objects_parts_are_packed() {
    use crate::{MIN_PART_BYTES, ObjectAttrs, StorageClass};
    let path = temp_path("pack-parts");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    let upload = ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_upload(
                "bkt",
                "mp",
                &ObjectAttrs::default(),
                StorageClass::DeepArchive,
                now(),
            )
        })
        .unwrap()
        .unwrap();
    let parts = [bytes(6, MIN_PART_BYTES as usize), bytes(7, 4321)];
    let mut listed = Vec::new();
    for (number, data) in parts.iter().enumerate() {
        let mut writer = BodyWriter::begin(&storage, now()).unwrap();
        for chunk in data.chunks(300_000) {
            writer.write(&storage, chunk, now()).unwrap();
        }
        let sealed = writer.finish(&storage, now()).unwrap();
        let info = ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.upload_part(upload, number as u32 + 1, &sealed, now(), 0))
            .unwrap()
            .unwrap();
        listed.push((number as u32 + 1, info.md5));
        storage.write_checkpoint(b"x", 1, 2, 1).unwrap();
    }
    let object = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.complete_upload(upload, &listed, now(), 0))
        .unwrap()
        .unwrap();
    let before = allocated_pages(&storage);
    assert!(pack_all(&storage) > 0);
    assert!(allocated_pages(&storage) < before / 2);
    let whole = parts.concat();
    assert_eq!(read(&storage, object.body, 0, whole.len()).unwrap(), whole);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_body_packed_as_it_is_written_reads_back_and_is_not_packed_again() {
    use crate::{SealedBody, StorageClass, pack_segment};
    let path = temp_path("pack-inline");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    let seg = SEGMENT_BYTES;
    let data: Vec<u8> = [bytes(1, seg), noise(7, seg), bytes(3, 1234)].concat();
    let level = StorageClass::DeepArchive.pack_level();
    let before = allocated_pages(&storage);
    let body = ObjFile::new(&mut storage.lock()).begin_body(now()).unwrap();
    for segment in data.chunks(seg) {
        let mut guard = storage.lock();
        let mut file = ObjFile::new(&mut guard);
        match pack_segment(segment, level) {
            Some(frame) => file.append_packed_segment(body, &frame, segment.len(), now()),
            None => file.append_segment(body, segment, now()),
        }
        .unwrap();
    }
    let sealed = SealedBody {
        body,
        md5: [7; 16],
        size: None,
        pack_level: level,
    };
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.put_object(
                "bkt",
                "k",
                &sealed,
                &crate::ObjectAttrs::default(),
                StorageClass::DeepArchive,
                now(),
                0,
            )
        })
        .unwrap()
        .unwrap();
    let written = allocated_pages(&storage) - before;
    assert!(
        written < (seg + 1234) as u64 / 4096 + 10,
        "{written} pages: the noise segment whole, the others packed"
    );
    assert_eq!(pack_all(&storage), 0, "packed at its class's level already");
    drop(storage); // crash
    let storage = open(&path);
    for (offset, len) in [
        (0, data.len()),
        (seg as u64 - 5, 10),
        (2 * seg as u64 + 7, 100),
    ] {
        assert_eq!(
            read(&storage, body, offset, len).unwrap(),
            data[offset as usize..offset as usize + len],
            "{offset}+{len}"
        );
    }
    // A segment stored larger than its bytes is refused.
    let staged = ObjFile::new(&mut storage.lock()).begin_body(now()).unwrap();
    assert!(
        ObjFile::new(&mut storage.lock())
            .append_packed_segment(staged, &[0; 11], 10, now())
            .is_err()
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn an_unreadable_body_leaves_the_pack_queue_to_the_bodies_after_it() {
    use crate::StorageClass;
    use crate::codec::SegmentRow;
    let path = temp_path("pack-unreadable");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    let data = bytes(8, 5000);
    let first = put_classed(&storage, "bkt", "first", &data, StorageClass::Glacier);
    let second = put_classed(&storage, "bkt", "second", &data, StorageClass::Glacier);
    // The first body's segment names an LSN its pages do not carry.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            let segments = tx.trees.segments();
            let key = crate::codec::segment_key(first.body, 0);
            let mut row = SegmentRow::decode(&segments.get(tx.ctx, &key)?.unwrap())?;
            row.lsn += 1;
            segments.update(tx.ctx, &mut OpMode::Txn(tx.txn), &key, &row.encode())?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        pack_all(&storage),
        2,
        "the first dropped, the second packed"
    );
    assert_eq!(read(&storage, second.body, 0, data.len()).unwrap(), data);
    let packed = |body: u64| {
        let mut guard = storage.lock();
        let file = ObjFile::new(&mut guard);
        let bodies = file.existing_tree(crate::BODIES_TREE).unwrap().unwrap();
        crate::body::load_body(&mut file.core.page_ctx(0), &bodies, body)
            .unwrap()
            .pack_level
    };
    assert_eq!(packed(second.body), 9);
    assert_eq!(packed(first.body), 0, "left as it was");
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_damaged_body_queued_colder_meanwhile_keeps_its_new_entry() {
    use crate::codec::SegmentRow;
    use crate::{PackStep, StorageClass};
    let path = temp_path("pack-requeued");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    let cold = put_classed(
        &storage,
        "bkt",
        "cold",
        &bytes(8, 5000),
        StorageClass::Glacier,
    );
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            let segments = tx.trees.segments();
            let key = crate::codec::segment_key(cold.body, 0);
            let mut row = SegmentRow::decode(&segments.get(tx.ctx, &key)?.unwrap())?;
            row.lsn += 1;
            segments.update(tx.ctx, &mut OpMode::Txn(tx.txn), &key, &row.encode())?;
            Ok(())
        })
        .unwrap();
    let Some(PackStep::Unreadable {
        body, level, index, ..
    }) = ObjFile::new(&mut storage.lock()).next_pack().unwrap()
    else {
        panic!("the damaged body is unreadable");
    };
    // Between the step's read and its drop, a colder class queues the body.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.queue_pack(body, StorageClass::DeepArchive.pack_level())?;
            tx.drop_pack(body, level, index)
        })
        .unwrap();
    let mut guard = storage.lock();
    let file = ObjFile::new(&mut guard);
    let queue = file.existing_tree(crate::PACK_QUEUE_TREE).unwrap().unwrap();
    let queued = queue
        .get(&mut file.core.page_ctx(0), &crate::codec::body_key(body))
        .unwrap();
    let mut restarted = vec![19];
    restarted.extend(0u64.to_le_bytes());
    assert_eq!(queued, Some(restarted), "level 19 from segment 0");
    drop(guard);
    drop(storage);
    std::fs::remove_file(&path).ok();
}

/// A Standard-tier restore kept `days`, taking `delay_ms`.
fn ask(days: u32, delay_ms: u64) -> crate::RestoreRequest {
    crate::RestoreRequest {
        days,
        delay_ms,
        expedited: false,
    }
}

#[test]
fn an_archived_version_is_read_only_while_restored() {
    use crate::{ObjectError, Restored, StorageClass, VersionSel, Versioning, restore_expiry};
    let path = temp_path("restore");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("bkt", DEFAULT_ACCOUNT, now())?.unwrap();
            tx.set_versioning("bkt", Versioning::Enabled)?.unwrap();
            Ok(())
        })
        .unwrap();
    let cold = put_classed(&storage, "bkt", "cold", b"frozen", StorageClass::Glacier);
    let ir = put_classed(&storage, "bkt", "ir", b"instant", StorageClass::GlacierIr);
    let restore = |key: &str, which: VersionSel, days: u32, delay: u64, at: u64| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.restore_object("bkt", key, which, ask(days, delay), at))
            .unwrap()
    };
    let unreadable = |info: &crate::ObjectInfo, at: u64| {
        ObjFile::new(&mut storage.lock())
            .archived_unreadable(info.version, info.storage_class, at)
            .unwrap()
    };
    let window = |version: u64| ObjFile::new(&mut storage.lock()).restore(version).unwrap();
    assert!(unreadable(&cold, now()));
    assert!(!unreadable(&ir, now()), "GLACIER_IR is read directly");
    assert_eq!(
        restore("ir", VersionSel::Latest, 1, 0, now()),
        Err(ObjectError::NotArchived)
    );
    assert_eq!(
        restore("missing", VersionSel::Latest, 1, 0, now()),
        Err(ObjectError::NoSuchKey)
    );
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.restore_object(
                "nope",
                "cold",
                VersionSel::Latest,
                ask(1, 0),
                now()
            ))
            .unwrap(),
        Err(ObjectError::NoSuchBucket)
    );

    // Ongoing until its delay is up, then readable until it expires.
    let delay = 5_000;
    assert_eq!(
        restore("cold", VersionSel::Latest, 2, delay, now()),
        Ok((Restored::Started, cold.version, false))
    );
    // The days count from when the restore completes.
    let expiry = restore_expiry(now() + delay, 2);
    assert_eq!(
        window(cold.version),
        Some(crate::Restore {
            ready_ms: now() + delay,
            expiry_ms: expiry
        })
    );
    assert!(unreadable(&cold, now() + delay - 1));
    assert_eq!(
        restore("cold", VersionSel::Seq(cold.version), 9, 0, now() + 1),
        Err(ObjectError::RestoreInProgress)
    );
    assert!(!unreadable(&cold, now() + delay));
    // Asked again while restored, only the expiry moves, from the new time.
    let later = now() + delay + 1;
    assert_eq!(
        restore("cold", VersionSel::Latest, 7, 0, later),
        Ok((Restored::Extended, cold.version, false))
    );
    let extended = restore_expiry(later, 7);
    assert_eq!(window(cold.version).unwrap().expiry_ms, extended);
    assert!(!unreadable(&cold, extended - 1));
    assert!(unreadable(&cold, extended));

    // A restore that takes longer than the days it keeps the copy is still
    // readable for those days once it completes; DEEP_ARCHIVE has no
    // Expedited tier.
    let deep = put_classed(
        &storage,
        "bkt",
        "deep",
        b"deeper",
        StorageClass::DeepArchive,
    );
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.restore_object(
                "bkt",
                "deep",
                VersionSel::Latest,
                crate::RestoreRequest {
                    expedited: true,
                    ..ask(1, 0)
                },
                now()
            ))
            .unwrap(),
        Err(ObjectError::TierUnavailable)
    );
    let slow = 3 * 86_400_000;
    assert_eq!(
        restore("deep", VersionSel::Latest, 1, slow, now())
            .unwrap()
            .0,
        Restored::Started
    );
    assert!(unreadable(&deep, now() + slow - 1));
    assert!(!unreadable(&deep, now() + slow));
    assert!(!unreadable(&deep, now() + slow + 86_400_000 - 1));
    // Extending it retrieves nothing, so any tier is accepted.
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.restore_object(
                "bkt",
                "deep",
                VersionSel::Latest,
                crate::RestoreRequest {
                    expedited: true,
                    ..ask(2, 0)
                },
                now() + slow
            ))
            .unwrap()
            .unwrap()
            .0,
        Restored::Extended
    );
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.delete_object(
                "bkt",
                "deep",
                Some(VersionSel::Seq(deep.version)),
                false,
                now(),
                0,
            )
        })
        .unwrap()
        .unwrap();

    // The window survives a restart; once expired it is removed, and an
    // idle sweep has nothing to do.
    drop(storage);
    let storage = open(&path);
    let file = |storage: &Storage, at: u64| ObjFile::new(&mut storage.lock()).restores_due(at);
    assert!(!file(&storage, extended - 1).unwrap());
    assert!(file(&storage, extended).unwrap());
    let expired = ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.expire_restores(extended, 10))
        .unwrap();
    assert_eq!(expired, 1);
    assert!(!file(&storage, u64::MAX).unwrap());
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .restore(cold.version)
            .unwrap(),
        None
    );
    // An expired restore is restored anew (202), not extended.
    let restore = |which: VersionSel, at: u64| {
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.restore_object("bkt", "cold", which, ask(1, 0), at))
            .unwrap()
    };
    assert_eq!(
        restore(VersionSel::Latest, extended),
        Ok((Restored::Started, cold.version, false))
    );
    // Removing the version removes its restore and timer.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.delete_object(
                "bkt",
                "cold",
                Some(VersionSel::Seq(cold.version)),
                false,
                now(),
                0,
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        ObjFile::new(&mut storage.lock())
            .restore(cold.version)
            .unwrap(),
        None
    );
    assert!(!file(&storage, u64::MAX).unwrap());
    assert_eq!(
        restore(VersionSel::Seq(cold.version), extended),
        Err(ObjectError::NoSuchVersion)
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_restore_is_whole_after_a_crash_at_any_op_and_ships_to_a_standby() {
    use crate::{Restore, StorageClass, VersionSel, restore_expiry};
    let first = restore_expiry(now(), 1);
    let second = restore_expiry(now() + 1, 3);
    let mut crash_at = 0;
    let mut crashes = 0;
    loop {
        let path = temp_path("restore-crash");
        let storage = create(&path);
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
            .unwrap()
            .unwrap();
        let cold = put_classed(&storage, "bkt", "k", b"deep", StorageClass::DeepArchive);
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.restore_object("bkt", "k", VersionSel::Latest, ask(1, 0), now()))
            .unwrap()
            .unwrap();
        // Extending replaces the window and its timer: crash anywhere in it.
        FAIL_APPLY_OPS_AFTER.with(|fail| fail.set(Some(crash_at)));
        let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ft_txn(&storage, |tx| {
                tx.restore_object("bkt", "k", VersionSel::Latest, ask(3, 0), now() + 1)
            })
        }))
        .is_err();
        let fired = FAIL_APPLY_OPS_AFTER
            .with(|fail| fail.replace(None))
            .is_none();
        drop(storage);
        let storage = open(&path);
        let window = ObjFile::new(&mut storage.lock())
            .restore(cold.version)
            .unwrap()
            .unwrap();
        let expected = if crashed {
            assert!(fired);
            crashes += 1;
            first
        } else {
            second
        };
        assert_eq!(
            window,
            Restore {
                ready_ms: now(),
                expiry_ms: expected
            },
            "crash at {crash_at}"
        );
        // The timer agrees with the window: due exactly at its expiry, and
        // expiring it leaves nothing behind.
        let due = |at: u64| ObjFile::new(&mut storage.lock()).restores_due(at).unwrap();
        assert!(!due(expected - 1), "crash at {crash_at}");
        assert!(due(expected), "crash at {crash_at}");
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.expire_restores(u64::MAX, 10))
            .unwrap();
        assert!(!due(u64::MAX), "crash at {crash_at}");
        drop(storage);
        std::fs::remove_file(&path).ok();
        if !crashed {
            assert!(crashes >= 4, "the restore crashed at only {crashes} points");
            break;
        }
        crash_at += 1;
    }

    // A restore reaches a standby and a restore from a log backup.
    let path = temp_path("restore-ship-src");
    let bak = temp_path("restore-ship-bak");
    let log = temp_path("restore-ship-log");
    let seed = temp_path("restore-ship-seed");
    let dst = temp_path("restore-ship-dst");
    let primary = create(&path);
    primary.lock().commit_db_options(0, Some(true)).unwrap();
    let summary = primary.backup_full(&bak).unwrap();
    ObjFile::new(&mut primary.lock())
        .transaction(|tx| tx.create_bucket("bkt", DEFAULT_ACCOUNT, now()))
        .unwrap()
        .unwrap();
    let cold = put_classed(&primary, "bkt", "k", b"deep", StorageClass::DeepArchive);
    ObjFile::new(&mut primary.lock())
        .transaction(|tx| tx.restore_object("bkt", "k", VersionSel::Latest, ask(1, 0), now()))
        .unwrap()
        .unwrap();
    let window = |storage: &Storage| {
        ObjFile::new(&mut storage.lock())
            .restore(cold.version)
            .unwrap()
    };
    let expected = window(&primary);
    assert!(expected.is_some());
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, primary.wal_flushed_lsn())
        .unwrap();
    primary.backup_log(&log, true, false).unwrap();
    Storage::restore_full_standby(&seed, &bak, &[], engines).unwrap();
    let standby = open(&seed);
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .unwrap();
    assert_eq!(window(&standby), expected);
    drop(standby);
    Storage::restore_full_with_logs(&dst, &bak, &[log.clone()], None, engines).unwrap();
    let restored = open(&dst);
    assert_eq!(window(&restored), expected);
    drop(restored);
    drop(primary);
    for p in [&path, &bak, &log, &seed, &dst] {
        std::fs::remove_file(p).ok();
    }
}

/// Puts `size` bytes as `key` in `bucket`, in `class`, written at `at`.
fn put_at(
    storage: &Storage,
    bucket: &str,
    key: &str,
    size: usize,
    class: crate::StorageClass,
    at: u64,
) -> u64 {
    let mut writer = BodyWriter::begin(storage, at).unwrap();
    writer.write(storage, &bytes(3, size), at).unwrap();
    let sealed = writer.finish(storage, at).unwrap();
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.put_object(
                bucket,
                key,
                &sealed,
                &crate::ObjectAttrs::default(),
                class,
                at,
                0,
            )
        })
        .unwrap()
        .unwrap()
        .version
}

/// Every version of every bucket (key, version, delete marker, class) and
/// every open upload's key.
fn lifecycle_snapshot(storage: &Storage) -> (Vec<(String, u64, bool, String)>, Vec<String>) {
    let mut guard = storage.lock();
    let mut file = ObjFile::new(&mut guard);
    let mut versions = Vec::new();
    let mut uploads = Vec::new();
    for bucket in ["ver", "unv", "lck"] {
        // A bucket deleted meanwhile has nothing to list.
        let Ok(listing) = file.list_versions(bucket, "", None, None, 1000).unwrap() else {
            continue;
        };
        versions.extend(listing.objects.into_iter().map(|listed| {
            (
                format!("{bucket}/{}", listed.key),
                listed.info.version,
                listed.info.delete_marker,
                listed.info.storage_class.as_str().to_string(),
            )
        }));
        uploads.extend(
            file.list_uploads(bucket, "", None, 1000)
                .unwrap()
                .0
                .into_iter()
                .map(|upload| format!("{bucket}/{}", upload.key)),
        );
    }
    (versions, uploads)
}

/// Runs the lifecycle passes due at `at` to the end, `budget` a step.
fn lifecycle_passes(storage: &Storage, at: u64, budget: crate::StepBudget) {
    for _ in 0..10_000 {
        if !ObjFile::new(&mut storage.lock())
            .lifecycle_step(at, 0, budget)
            .unwrap()
        {
            return;
        }
    }
    panic!("the lifecycle passes never ended");
}

const DAY: u64 = 86_400_000;
const HOUR: u64 = 3_600_000;
/// Day 10, 01:00 UTC.
const T0: u64 = 10 * DAY + HOUR;

/// The buckets of the lifecycle tests: `ver` versioned, `unv` unversioned and
/// `lck` with Object Lock, their objects written on day 10 and their rules.
fn lifecycle_scene(storage: &Storage) {
    use crate::{
        Expiration, Filter, LifecycleConfig, MinTransitionSize, NoncurrentExpiration, ObjectLock,
        Retention, RetentionMode, Rule, StorageClass, Transition, Versioning, When,
    };
    let rule = |id: &str, filter: Filter| Rule {
        id: id.to_string(),
        enabled: true,
        filter,
        expiration: None,
        transitions: Vec::new(),
        noncurrent_expiration: None,
        noncurrent_transitions: Vec::new(),
        abort_upload_days: None,
    };
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            for bucket in ["ver", "unv", "lck"] {
                tx.create_bucket(bucket, DEFAULT_ACCOUNT, T0)?.unwrap();
            }
            tx.set_versioning("ver", Versioning::Enabled)?.unwrap();
            tx.set_versioning("lck", Versioning::Enabled)?.unwrap();
            tx.enable_object_lock("lck")?.unwrap();
            tx.put_lifecycle(
                "ver",
                &LifecycleConfig {
                    min_size: MinTransitionSize::AllClasses128K,
                    rules: vec![
                        Rule {
                            transitions: vec![Transition {
                                when: When::Days(1),
                                class: StorageClass::Glacier,
                            }],
                            expiration: Some(Expiration::At(When::Days(3))),
                            noncurrent_expiration: Some(NoncurrentExpiration {
                                days: 1,
                                newer_versions: Some(1),
                            }),
                            ..rule("logs", Filter::Prefix("logs/".into()))
                        },
                        Rule {
                            transitions: vec![Transition {
                                when: When::Days(0),
                                class: StorageClass::StandardIa,
                            }],
                            ..rule("tiny", Filter::Prefix("tiny/".into()))
                        },
                        Rule {
                            expiration: Some(Expiration::ExpiredObjectDeleteMarker(true)),
                            ..rule("gone", Filter::Prefix("gone/".into()))
                        },
                        Rule {
                            abort_upload_days: Some(2),
                            ..rule("uploads", Filter::Prefix("up/".into()))
                        },
                        Rule {
                            enabled: false,
                            expiration: Some(Expiration::At(When::Days(1))),
                            ..rule("off", Filter::All)
                        },
                    ],
                },
            )?;
            tx.put_lifecycle(
                "unv",
                &LifecycleConfig {
                    min_size: MinTransitionSize::AllClasses128K,
                    rules: vec![Rule {
                        transitions: vec![Transition {
                            when: When::Days(0),
                            class: StorageClass::Glacier,
                        }],
                        expiration: Some(Expiration::At(When::Days(1))),
                        ..rule("all", Filter::All)
                    }],
                },
            )?;
            tx.put_lifecycle(
                "lck",
                &LifecycleConfig {
                    min_size: MinTransitionSize::AllClasses128K,
                    rules: vec![Rule {
                        noncurrent_expiration: Some(NoncurrentExpiration {
                            days: 1,
                            newer_versions: None,
                        }),
                        ..rule("old", Filter::All)
                    }],
                },
            )
        })
        .unwrap();
    let big = 200 * 1024;
    put_at(storage, "ver", "logs/a", big, StorageClass::Standard, T0);
    put_at(
        storage,
        "ver",
        "logs/a",
        big,
        StorageClass::Standard,
        T0 + HOUR,
    );
    put_at(storage, "ver", "logs/b", big, StorageClass::Standard, T0);
    put_at(storage, "ver", "tiny/x", 10, StorageClass::Standard, T0);
    put_at(storage, "ver", "other/z", big, StorageClass::Standard, T0);
    let gone = put_at(storage, "ver", "gone/k", 10, StorageClass::Standard, T0);
    put_at(storage, "unv", "k", big, StorageClass::Standard, T0);
    let locked = put_at(storage, "lck", "k", 10, StorageClass::Standard, T0);
    put_at(storage, "lck", "k", 10, StorageClass::Standard, T0 + HOUR);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            // gone/k: a delete marker left alone.
            tx.delete_object("ver", "gone/k", None, false, T0, 0)?
                .unwrap();
            tx.delete_object(
                "ver",
                "gone/k",
                Some(crate::VersionSel::Seq(gone)),
                false,
                T0,
                0,
            )?
            .unwrap();
            tx.set_lock(
                locked,
                &ObjectLock {
                    retention: Some(Retention {
                        mode: RetentionMode::Compliance,
                        until_ms: u64::MAX,
                        event: None,
                    }),
                    legal_hold: None,
                },
            )?;
            for key in ["up/u", "other/u"] {
                tx.create_upload(
                    "ver",
                    key,
                    &crate::ObjectAttrs::default(),
                    StorageClass::Standard,
                    T0,
                )?
                .unwrap();
            }
            Ok(())
        })
        .unwrap();
}

/// What the scene holds after the passes of each day, as `(key, marker,
/// class)` per version (newest first) and the open uploads.
fn lifecycle_days() -> Vec<(
    u64,
    Vec<(&'static str, bool, &'static str)>,
    Vec<&'static str>,
)> {
    let both = vec!["ver/other/u", "ver/up/u"];
    let mut days = Vec::new();
    // Day 10: only the lone delete marker is due (at once).
    days.push((
        T0 + HOUR,
        vec![
            ("lck/k", false, "STANDARD"),
            ("lck/k", false, "STANDARD"),
            ("unv/k", false, "STANDARD"),
            ("ver/logs/a", false, "STANDARD"),
            ("ver/logs/a", false, "STANDARD"),
            ("ver/logs/b", false, "STANDARD"),
            ("ver/other/z", false, "STANDARD"),
            ("ver/tiny/x", false, "STANDARD"),
        ],
        both.clone(),
    ));
    // Day 12: a day old, the logs go cold; the unversioned object is removed (before a transition); the locked
    // noncurrent version stays; the small object stays STANDARD; the
    // noncurrent logs/a has no newer noncurrent version to keep.
    days.push((
        12 * DAY + HOUR,
        vec![
            ("lck/k", false, "STANDARD"),
            ("lck/k", false, "STANDARD"),
            ("ver/logs/a", false, "GLACIER"),
            ("ver/logs/a", false, "STANDARD"),
            ("ver/logs/b", false, "GLACIER"),
            ("ver/other/z", false, "STANDARD"),
            ("ver/tiny/x", false, "STANDARD"),
        ],
        both.clone(),
    ));
    // Day 13: the upload under up/ is aborted.
    let mut day13 = days[1].clone();
    day13.0 = 13 * DAY + HOUR;
    day13.2 = vec!["ver/other/u"];
    days.push(day13);
    // Day 14: three days old, the logs expire: delete markers.
    days.push((
        14 * DAY + HOUR,
        vec![
            ("lck/k", false, "STANDARD"),
            ("lck/k", false, "STANDARD"),
            ("ver/logs/a", true, "STANDARD"),
            ("ver/logs/a", false, "GLACIER"),
            ("ver/logs/a", false, "STANDARD"),
            ("ver/logs/b", true, "STANDARD"),
            ("ver/logs/b", false, "GLACIER"),
            ("ver/other/z", false, "STANDARD"),
            ("ver/tiny/x", false, "STANDARD"),
        ],
        vec!["ver/other/u"],
    ));
    // Day 15: logs/a's oldest version now has a newer noncurrent one, and
    // its successor is days old: removed.
    days.push((
        15 * DAY + HOUR,
        vec![
            ("lck/k", false, "STANDARD"),
            ("lck/k", false, "STANDARD"),
            ("ver/logs/a", true, "STANDARD"),
            ("ver/logs/a", false, "GLACIER"),
            ("ver/logs/b", true, "STANDARD"),
            ("ver/logs/b", false, "GLACIER"),
            ("ver/other/z", false, "STANDARD"),
            ("ver/tiny/x", false, "STANDARD"),
        ],
        vec!["ver/other/u"],
    ));
    days
}

fn lifecycle_view(storage: &Storage) -> (Vec<(String, bool, String)>, Vec<String>) {
    let (versions, uploads) = lifecycle_snapshot(storage);
    (
        versions
            .into_iter()
            .map(|(key, _, marker, class)| (key, marker, class))
            .collect(),
        uploads,
    )
}

fn expected_view(
    day: &(
        u64,
        Vec<(&'static str, bool, &'static str)>,
        Vec<&'static str>,
    ),
) -> (Vec<(String, bool, String)>, Vec<String>) {
    let mut versions: Vec<_> = day
        .1
        .iter()
        .map(|(key, marker, class)| (key.to_string(), *marker, class.to_string()))
        .collect();
    // Listed bucket by bucket: v, u, lk.
    versions.sort_by_key(|(key, _, _)| match &key[..4] {
        "ver/" => 0,
        "unv/" => 1,
        _ => 2,
    });
    (versions, day.2.iter().map(|key| key.to_string()).collect())
}

#[test]
fn lifecycle_rules_act_as_the_days_pass_once_a_day() {
    let path = temp_path("lifecycle");
    let storage = create(&path);
    lifecycle_scene(&storage);
    let budget = crate::StepBudget {
        rows: 1000,
        actions: 1000,
    };
    let packs_queued = || {
        ObjFile::new(&mut storage.lock())
            .next_pack()
            .unwrap()
            .is_some()
    };
    for day in lifecycle_days() {
        // Only the transitions of day 12 queue bodies for packing.
        assert_eq!(packs_queued(), day.0 >= 13 * DAY, "at {}", day.0);
        lifecycle_passes(&storage, day.0, budget);
        assert_eq!(packs_queued(), day.0 >= 12 * DAY, "at {}", day.0);
        assert_eq!(
            lifecycle_view(&storage),
            expected_view(&day),
            "at {}",
            day.0
        );
        // A second run the same day changes nothing.
        let journal = ObjFile::new(&mut storage.lock())
            .journal(0, 10_000)
            .unwrap();
        lifecycle_passes(&storage, day.0 + HOUR, budget);
        assert_eq!(
            lifecycle_view(&storage),
            expected_view(&day),
            "at {}",
            day.0
        );
        assert_eq!(
            ObjFile::new(&mut storage.lock())
                .journal(0, 10_000)
                .unwrap(),
            journal
        );
    }
    // The pass's changes are in the journal, named as lifecycle's.
    let journal = ObjFile::new(&mut storage.lock())
        .journal(0, 10_000)
        .unwrap();
    let by = |operation: &str| {
        journal
            .iter()
            .filter(|row| {
                row.origin
                    .as_ref()
                    .is_some_and(|o| o.operation == operation)
            })
            .count()
    };
    assert_eq!(by("LifecycleTransition"), 2);
    // The marker, the unversioned removal, two expirations, one noncurrent
    // removal.
    assert_eq!(by("LifecycleExpiration"), 5);
    // A deleted configuration stops the passes; a deleted bucket takes its
    // state along: with neither left, no pass is due.
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            assert!(tx.delete_lifecycle("ver")?);
            assert!(!tx.delete_lifecycle("ver")?);
            assert!(tx.delete_lifecycle("lck")?);
            tx.delete_object("unv", "k", None, false, T0, 0)?.unwrap();
            tx.delete_bucket("unv").map(|deleted| deleted.unwrap())
        })
        .unwrap();
    assert!(
        ObjFile::new(&mut storage.lock())
            .lifecycle("ver")
            .unwrap()
            .is_none()
    );
    assert!(
        !ObjFile::new(&mut storage.lock())
            .lifecycle_step(40 * DAY, 0, budget)
            .unwrap()
    );
    let after = lifecycle_view(&storage);
    assert!(after.0.iter().any(|(key, _, _)| key == "ver/other/z"));
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_lifecycle_pass_resumes_across_restarts_and_failed_steps() {
    let days = lifecycle_days();
    // A step at a time, the store reopened after each: the same days.
    let path = temp_path("lifecycle-resume");
    let mut storage = create(&path);
    lifecycle_scene(&storage);
    let tiny = crate::StepBudget {
        rows: 1,
        actions: 1,
    };
    for day in &days {
        loop {
            let more = ObjFile::new(&mut storage.lock())
                .lifecycle_step(day.0, 0, tiny)
                .unwrap();
            drop(storage);
            storage = open(&path);
            if !more {
                break;
            }
        }
        assert_eq!(lifecycle_view(&storage), expected_view(day), "at {}", day.0);
    }
    drop(storage);
    std::fs::remove_file(&path).ok();

    // A step that fails at any of its operations changes nothing it would
    // not redo; the passes then end where they would have.
    let day = &days[3];
    let mut fail_at = 0;
    loop {
        let path = temp_path("lifecycle-fail");
        let storage = create(&path);
        lifecycle_scene(&storage);
        lifecycle_passes(&storage, days[1].0, tiny);
        lifecycle_passes(&storage, days[2].0, tiny);
        FAIL_APPLY_OPS_AFTER.with(|fail| fail.set(Some(fail_at)));
        let failed = ObjFile::new(&mut storage.lock())
            .lifecycle_step(
                day.0,
                0,
                crate::StepBudget {
                    rows: 1000,
                    actions: 1000,
                },
            )
            .is_err();
        let fired = FAIL_APPLY_OPS_AFTER
            .with(|fail| fail.replace(None))
            .is_none();
        drop(storage);
        let storage = open(&path);
        lifecycle_passes(&storage, day.0, tiny);
        assert_eq!(
            lifecycle_view(&storage),
            expected_view(day),
            "failed at {fail_at}"
        );
        drop(storage);
        std::fs::remove_file(&path).ok();
        if !failed {
            assert!(!fired);
            // Each expiration's operations and the pass's own write.
            assert!(fail_at >= 3, "the step failed at only {fail_at} points");
            break;
        }
        fail_at += 1;
    }
}

#[test]
fn a_lifecycle_pass_ships_to_a_standby_and_restores_from_a_log_backup() {
    let path = temp_path("lifecycle-ship-src");
    let bak = temp_path("lifecycle-ship-bak");
    let log = temp_path("lifecycle-ship-log");
    let seed = temp_path("lifecycle-ship-seed");
    let dst = temp_path("lifecycle-ship-dst");
    let primary = create(&path);
    primary.lock().commit_db_options(0, Some(true)).unwrap();
    let summary = primary.backup_full(&bak).unwrap();
    lifecycle_scene(&primary);
    let days = lifecycle_days();
    for day in &days {
        lifecycle_passes(
            &primary,
            day.0,
            crate::StepBudget {
                rows: 3,
                actions: 2,
            },
        );
    }
    let expected = lifecycle_snapshot(&primary);
    assert_eq!(
        lifecycle_view(&primary),
        expected_view(days.last().unwrap())
    );
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, primary.wal_flushed_lsn())
        .unwrap();
    primary.backup_log(&log, true, false).unwrap();
    Storage::restore_full_standby(&seed, &bak, &[], engines).unwrap();
    let standby = open(&seed);
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .unwrap();
    assert_eq!(lifecycle_snapshot(&standby), expected);
    drop(standby);
    Storage::restore_full_with_logs(&dst, &bak, &[log.clone()], None, engines).unwrap();
    let restored = open(&dst);
    assert_eq!(lifecycle_snapshot(&restored), expected);
    // The restored store's passes are done for the day.
    assert!(
        !ObjFile::new(&mut restored.lock())
            .lifecycle_step(
                days.last().unwrap().0,
                0,
                crate::StepBudget {
                    rows: 1000,
                    actions: 1000,
                }
            )
            .unwrap()
    );
    drop(restored);
    drop(primary);
    for p in [&path, &bak, &log, &seed, &dst] {
        std::fs::remove_file(p).ok();
    }
}

fn lifecycle_rule(id: &str, filter: crate::Filter) -> crate::Rule {
    crate::Rule {
        id: id.to_string(),
        enabled: true,
        filter,
        expiration: None,
        transitions: Vec::new(),
        noncurrent_expiration: None,
        noncurrent_transitions: Vec::new(),
        abort_upload_days: None,
    }
}

fn put_rules(storage: &Storage, bucket: &str, rules: Vec<crate::Rule>) {
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.put_lifecycle(
                bucket,
                &crate::LifecycleConfig {
                    min_size: crate::MinTransitionSize::AllClasses128K,
                    rules,
                },
            )
        })
        .unwrap();
}

/// The classes of `bucket`'s versions (`MARKER` for a delete marker),
/// newest first per key.
fn classes(storage: &Storage, bucket: &str) -> Vec<(String, String)> {
    ObjFile::new(&mut storage.lock())
        .list_versions(bucket, "", None, None, 1000)
        .unwrap()
        .unwrap()
        .objects
        .into_iter()
        .map(|listed| {
            (
                listed.key,
                if listed.info.delete_marker {
                    "MARKER".to_string()
                } else {
                    listed.info.storage_class.as_str().to_string()
                },
            )
        })
        .collect()
}

#[test]
fn a_new_configuration_or_a_pass_past_midnight_acts_once_a_day() {
    use crate::{Expiration, Filter, StorageClass, Transition, Versioning, When};
    let path = temp_path("lifecycle-once");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.create_bucket("bkt", DEFAULT_ACCOUNT, T0)?.unwrap();
            tx.set_versioning("bkt", Versioning::Enabled)
        })
        .unwrap()
        .unwrap();
    for key in ["a", "b", "c"] {
        put_at(&storage, "bkt", key, 200 * 1024, StorageClass::Standard, T0);
    }
    // Long due on day 20: a transition and an expiration; the transition
    // comes first, the marker the next day, even with the rules just set.
    let day = 20 * DAY + HOUR;
    put_rules(
        &storage,
        "bkt",
        vec![crate::Rule {
            transitions: vec![Transition {
                when: When::Days(1),
                class: StorageClass::Glacier,
            }],
            expiration: Some(Expiration::At(When::Days(3))),
            ..lifecycle_rule("r", Filter::All)
        }],
    );
    let all_cold = vec![
        ("a".to_string(), "GLACIER".to_string()),
        ("b".to_string(), "GLACIER".to_string()),
        ("c".to_string(), "GLACIER".to_string()),
    ];
    let one_row = crate::StepBudget {
        rows: 1,
        actions: 1,
    };
    // One version on day 20, the rest after midnight.
    assert!(
        ObjFile::new(&mut storage.lock())
            .lifecycle_step(day, 0, one_row)
            .unwrap()
    );
    lifecycle_passes(&storage, day + DAY, one_row);
    assert_eq!(classes(&storage, "bkt"), all_cold);
    // The pass finished on day 21: nothing more that day.
    assert!(
        !ObjFile::new(&mut storage.lock())
            .lifecycle_step(day + DAY + HOUR, 0, one_row)
            .unwrap()
    );
    lifecycle_passes(&storage, day + 2 * DAY, one_row);
    let marked: Vec<_> = classes(&storage, "bkt");
    assert_eq!(
        marked.iter().filter(|(_, class)| class == "MARKER").count(),
        3
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn lifecycle_passes_go_by_tags_suspended_versioning_and_noncurrent_rules() {
    use crate::{
        Expiration, Filter, NoncurrentExpiration, NoncurrentTransition, ObjectLock, Retention,
        RetentionMode, StorageClass, Transition, Versioning, When,
    };
    let path = temp_path("lifecycle-kinds");
    let storage = create(&path);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            for bucket in ["tag", "sus", "non"] {
                tx.create_bucket(bucket, DEFAULT_ACCOUNT, T0)?.unwrap();
            }
            tx.set_versioning("non", Versioning::Enabled)?.unwrap();
            tx.enable_object_lock("non")?.unwrap();
            tx.set_versioning("sus", Versioning::Enabled)?.unwrap();
            Ok(())
        })
        .unwrap();
    let big = 200 * 1024;
    // tag: only the tagged object transitions, by a date.
    put_at(&storage, "tag", "plain", big, StorageClass::Standard, T0);
    let tagged = put_at(&storage, "tag", "tagged", big, StorageClass::Standard, T0);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.set_tags(
                "tag",
                "tagged",
                crate::VersionSel::Seq(tagged),
                vec![("tier".to_string(), "cold".to_string())],
                T0,
            )
        })
        .unwrap()
        .unwrap();
    put_rules(
        &storage,
        "tag",
        vec![crate::Rule {
            transitions: vec![Transition {
                when: When::Date(12 * DAY),
                class: StorageClass::DeepArchive,
            }],
            ..lifecycle_rule("t", Filter::Tag("tier".into(), "cold".into()))
        }],
    );
    // sus: a version from while versioning was on, then a null version;
    // expiring the null current version leaves a null delete marker.
    put_at(&storage, "sus", "k", 10, StorageClass::Standard, T0);
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| tx.set_versioning("sus", Versioning::Suspended))
        .unwrap()
        .unwrap();
    put_at(&storage, "sus", "k", 10, StorageClass::Standard, T0);
    put_rules(
        &storage,
        "sus",
        vec![crate::Rule {
            expiration: Some(Expiration::At(When::Days(1))),
            ..lifecycle_rule("e", Filter::All)
        }],
    );
    // non: five versions; the two newest noncurrent are kept, the others
    // go colder, and the oldest (locked) stays where it is.
    let mut versions = Vec::new();
    for hour in 0..5 {
        versions.push(put_at(
            &storage,
            "non",
            "k",
            big,
            StorageClass::Standard,
            T0 + hour * HOUR,
        ));
    }
    ObjFile::new(&mut storage.lock())
        .transaction(|tx| {
            tx.set_lock(
                versions[0],
                &ObjectLock {
                    retention: Some(Retention {
                        mode: RetentionMode::Governance,
                        until_ms: u64::MAX,
                        event: None,
                    }),
                    legal_hold: None,
                },
            )
        })
        .unwrap();
    put_rules(
        &storage,
        "non",
        vec![crate::Rule {
            noncurrent_transitions: vec![NoncurrentTransition {
                days: 1,
                newer_versions: Some(2),
                class: StorageClass::Glacier,
            }],
            noncurrent_expiration: Some(NoncurrentExpiration {
                days: 30,
                newer_versions: None,
            }),
            ..lifecycle_rule("n", Filter::Prefix("k".into()))
        }],
    );
    lifecycle_passes(
        &storage,
        12 * DAY + HOUR,
        crate::StepBudget {
            rows: 2,
            actions: 1,
        },
    );
    let pair = |key: &str, class: &str| (key.to_string(), class.to_string());
    assert_eq!(
        classes(&storage, "tag"),
        [pair("plain", "STANDARD"), pair("tagged", "DEEP_ARCHIVE")]
    );
    // The null version became a null marker; the older numbered one stays.
    let sus = ObjFile::new(&mut storage.lock())
        .list_versions("sus", "", None, None, 10)
        .unwrap()
        .unwrap()
        .objects;
    assert_eq!(sus.len(), 2);
    assert!(sus[0].info.delete_marker && sus[0].info.null_version);
    assert!(!sus[1].info.delete_marker && !sus[1].info.null_version);
    assert_eq!(
        classes(&storage, "non"),
        [
            pair("k", "STANDARD"),
            pair("k", "STANDARD"),
            pair("k", "STANDARD"),
            pair("k", "GLACIER"),
            pair("k", "STANDARD"),
        ]
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}
