//! Several engines on the shared log: a second engine's root survives a crash,
//! a checkpoint, a backup and restore, a standby apply, a standby restartpoint
//! and a promotion, and restart recovery hands each tree undo to the engine
//! that owns the tree.

use std::sync::Mutex;

use super::*;
use crate::relstore::ctx::OpMode;
use crate::wal::records::PageOpUndo;

const SECOND_ENGINE_ID: u32 = 1;

/// Object ids whose undo restart recovery handed to [`SecondEngine`].
static UNDONE: Mutex<Vec<u32>> = Mutex::new(Vec::new());

struct SecondEngine {
    root: Option<u64>,
}

struct SecondUndo;

impl LogicalUndo for SecondUndo {
    fn undo(
        &self,
        _ctx: &mut RelCtx<'_>,
        _mode: &mut OpMode<'_>,
        object_id: u32,
        _undo: &PageOpUndo,
    ) -> Result<(), StorageError> {
        UNDONE.lock().unwrap().push(object_id);
        Ok(())
    }
}

impl EngineParticipant for SecondEngine {
    fn engine_id(&self) -> u32 {
        SECOND_ENGINE_ID
    }
    fn owns_object(&self, object_id: u32) -> bool {
        object_id >= FIRST_ENGINE_OBJECT_ID
    }
    fn open(&mut self, metadata_root: Option<u64>, _db_options: u8) {
        self.root = metadata_root;
    }
    fn metadata_root(&self) -> Option<u64> {
        self.root
    }
    fn set_metadata_root(&mut self, page: u64) {
        self.root = Some(page);
    }
    fn options_byte(&self) -> u8 {
        0
    }
    fn reload(&mut self, _core: &mut StorageFile) -> Result<(), StorageError> {
        Ok(())
    }
    fn recovery_undo(&self) -> Box<dyn LogicalUndo> {
        Box::new(SecondUndo)
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

fn two_engines() -> Vec<Box<dyn EngineParticipant>> {
    vec![Box::new(NoEngine), Box::new(SecondEngine { root: None })]
}

fn create_two(path: &Path) -> Storage {
    Storage::create_with_wal_bounds(
        path.to_path_buf(),
        test_storage_options(),
        TEST_WAL_BYTES,
        TEST_WAL_BYTES,
        two_engines,
    )
    .expect("create storage")
}

fn second_root(storage: &Storage) -> Option<u64> {
    let mut guard = storage.lock();
    guard.engine_parts::<SecondEngine>().1.root
}

/// Allocates a page and logs it as the second engine's root, durably.
fn set_second_root(storage: &Storage) -> u64 {
    let mut guard = storage.lock();
    let (core, eng) = guard.engine_parts::<SecondEngine>();
    let mut ctx = core.page_ctx(0);
    let page = ctx.allocate_page(0).expect("allocate root");
    ctx.append(&RelRecord::set_engine_root(SECOND_ENGINE_ID, page), true)
        .expect("log root");
    eng.root = Some(page);
    page
}

#[test]
fn second_engine_root_survives_crash_and_checkpoint() {
    let path = unique_temp_path("engines-root");
    let storage = create_two(&path);
    let page = set_second_root(&storage);
    drop(storage); // crash: only the log knows the root

    let storage = Storage::open(path.clone(), two_engines).expect("reopen after crash");
    assert_eq!(second_root(&storage), Some(page), "redo found the root");
    storage
        .write_checkpoint(b"state", 1, 2, 1)
        .expect("checkpoint");
    drop(storage);

    let storage = Storage::open(path.clone(), two_engines).expect("reopen after checkpoint");
    assert_eq!(
        second_root(&storage),
        Some(page),
        "the superblock carried the root past the truncated log"
    );
    drop(storage);

    let err = Storage::open(path.clone(), no_engine)
        .err()
        .expect("a build without the second engine refuses the file");
    assert!(
        err.to_string().contains("holds engine 1"),
        "unexpected error: {err}"
    );
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_root_logged_for_an_unknown_engine_is_refused() {
    let path = unique_temp_path("engines-unknown");
    let storage = create_two(&path);
    set_second_root(&storage);
    drop(storage); // crash: the root is only in the log

    let err = Storage::open(path.clone(), no_engine)
        .err()
        .expect("restart recovery refuses a root it cannot place");
    assert!(
        err.to_string().contains("holds engine 1"),
        "unexpected error: {err}"
    );
    std::fs::remove_file(&path).ok();
}

#[test]
fn second_engine_root_survives_backup_and_restore() {
    let path = unique_temp_path("engines-backup-src");
    let bak = unique_temp_path("engines-backup-bak");
    let dst = unique_temp_path("engines-backup-dst");
    let storage = create_two(&path);
    let page = set_second_root(&storage);
    storage
        .write_checkpoint(b"state", 1, 2, 1)
        .expect("checkpoint moves the root into the superblock");
    storage.backup_full(&bak).expect("backup");
    drop(storage);

    Storage::restore_full(&dst, &bak, two_engines).expect("restore");
    let restored = Storage::open(dst.clone(), two_engines).expect("open restored");
    assert_eq!(second_root(&restored), Some(page));
    drop(restored);
    for p in [&path, &bak, &dst] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn second_engine_root_ships_to_a_standby_and_survives_restartpoint_and_promotion() {
    let path = unique_temp_path("engines-standby-src");
    let bak = unique_temp_path("engines-standby-bak");
    let seed = unique_temp_path("engines-standby-seed");
    let primary = create_two(&path);
    let summary = primary.backup_full(&bak).expect("backup");
    let page = set_second_root(&primary);
    let flushed = primary.wal_flushed_lsn();
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, flushed)
        .expect("ship");

    Storage::restore_full_standby(&seed, &bak, &[], two_engines).expect("standby seed");
    let standby = Storage::open(seed.clone(), two_engines).expect("open standby");
    assert_eq!(second_root(&standby), None);
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .expect("apply");
    assert_eq!(
        second_root(&standby),
        Some(page),
        "the stream moved the root"
    );
    assert!(
        standby.standby_restartpoint().expect("restartpoint"),
        "the restartpoint moved the head past the root record"
    );
    {
        let guard = standby.lock();
        let offset = guard.layout.data_offset + page * PAGE_SIZE as u64;
        assert_eq!(
            guard.active_sb().engine_roots(),
            vec![(SECOND_ENGINE_ID, offset)],
            "the restartpoint stamped the root before truncating its record"
        );
    }
    drop(standby);

    Storage::promote(&seed, two_engines).expect("promote");
    let promoted = Storage::open(seed.clone(), two_engines).expect("open promoted");
    assert_eq!(second_root(&promoted), Some(page));
    drop(promoted);
    drop(primary);
    for p in [&path, &bak, &seed] {
        std::fs::remove_file(p).ok();
    }
}

#[test]
fn restart_undo_goes_to_the_engine_that_owns_the_tree() {
    let path = unique_temp_path("engines-undo");
    let storage = create_two(&path);
    let object_id = FIRST_ENGINE_OBJECT_ID + 7;
    {
        let mut guard = storage.lock();
        let mut ctx = guard.page_ctx(0);
        let page = ctx.allocate_page(0).expect("allocate");
        let begin = ctx.append(&RelRecord::txn_begin(5), false).expect("begin");
        let image = vec![0u8; PAGE_SIZE];
        let undo = PageOpUndo::TreeDeleteKey {
            object_id,
            key: b"k".to_vec(),
        };
        ctx.append(&RelRecord::page_image(5, begin, page, &image, &undo), true)
            .expect("loser op");
    }
    drop(storage); // crash with transaction 5 open

    let storage = Storage::open(path.clone(), two_engines).expect("reopen");
    assert!(
        UNDONE.lock().unwrap().contains(&object_id),
        "the second engine undid its own tree"
    );
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_factory_without_one_catalog_engine_or_with_shared_ids_is_refused() {
    fn only_second() -> Vec<Box<dyn EngineParticipant>> {
        vec![Box::new(SecondEngine { root: None })]
    }
    fn shared_id() -> Vec<Box<dyn EngineParticipant>> {
        vec![
            Box::new(NoEngine),
            Box::new(SecondEngine { root: None }),
            Box::new(SecondEngine { root: None }),
        ]
    }
    let path = unique_temp_path("engines-factory");
    for (factory, expected) in [
        (only_second as EngineFactory, "exactly one catalog engine"),
        (shared_id as EngineFactory, "share id 1"),
    ] {
        let err = Storage::create_with_wal_bounds(
            path.clone(),
            test_storage_options(),
            TEST_WAL_BYTES,
            TEST_WAL_BYTES,
            factory,
        )
        .err()
        .expect("create refuses the factory");
        assert!(
            err.to_string().contains(expected),
            "unexpected error: {err}"
        );
    }
    drop(create_two(&path));
    let err = Storage::open(path.clone(), shared_id)
        .err()
        .expect("open refuses the factory");
    assert!(
        err.to_string().contains("share id 1"),
        "unexpected error: {err}"
    );
    std::fs::remove_file(&path).ok();
}
