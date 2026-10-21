//! An encrypted database: no page of its data region holds plaintext (tree
//! pages, blob pages, the search snapshot, spill pages), it reopens with its
//! master key only, and a changed byte or a page moved elsewhere is refused.

use super::*;
use crate::encryption::KeySource;
use crate::relstore::btree::{BTree, TreeInsert};
use crate::relstore::ctx::OpMode;

const OBJECT: u32 = 0xE000_0002;
const MARKER: &[u8] = b"PLAINTEXT-CANARY";

fn key_file(label: &str, body: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = unique_temp_path(label).with_extension("keys");
    std::fs::write(&path, body).expect("write key file");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    path
}

fn row(n: u64) -> Vec<u8> {
    [MARKER, &n.to_le_bytes(), &[7u8; 64]].concat()
}

/// A tree of `rows` rows, a blob and a checkpoint (pages and the search
/// snapshot on disk), all carrying the marker. Returns the tree's root and
/// the blob's first page.
fn fill(storage: &Storage, rows: u64) -> (u64, u64) {
    let mut guard = storage.lock();
    let tree = BTree::create(&mut guard.page_ctx(0), OBJECT).expect("create tree");
    let txn_id = guard.alloc_txn_id();
    let blob = {
        let mut ctx = guard.page_ctx(0);
        let mut txn = ctx.begin(txn_id).expect("begin");
        for n in 0..rows {
            let inserted = tree
                .insert_unique(
                    &mut ctx,
                    &mut OpMode::Txn(&mut txn),
                    &n.to_be_bytes(),
                    &row(n),
                )
                .expect("insert");
            assert_eq!(inserted, TreeInsert::Inserted);
        }
        let start = ctx
            .allocate_extent_in_txn(&mut txn, super::txn_extents::PAGES)
            .expect("allocate");
        ctx.write_blob_pages(&mut txn, OBJECT + 1, start, &MARKER.repeat(300))
            .expect("blob");
        ctx.commit(txn).expect("commit");
        start
    };
    guard.wal_sync_all().expect("sync");
    drop(guard);
    let snapshot = MARKER.repeat(1000);
    storage
        .write_checkpoint(&snapshot, 1, 2, 1)
        .expect("checkpoint");
    (tree.root, blob)
}

fn rows_of(storage: &Storage, root: u64) -> Result<Vec<Vec<u8>>, StorageError> {
    let tree = BTree {
        object_id: OBJECT,
        root,
    };
    let mut guard = storage.lock();
    let rows = tree.scan(&mut guard.page_ctx(0))?;
    Ok(rows.into_iter().map(|(_, row)| row).collect())
}

/// The data region's bytes.
fn data_region(path: &Path, storage: &Storage) -> Vec<u8> {
    use std::os::unix::fs::FileExt;
    let (offset, size) = {
        let guard = storage.lock();
        (guard.layout.data_offset, guard.layout.data_size)
    };
    let file = std::fs::File::open(path).expect("open");
    let mut bytes = vec![0u8; size as usize];
    file.read_exact_at(&mut bytes, offset)
        .expect("read data region");
    bytes
}

fn holds_marker(bytes: &[u8]) -> bool {
    bytes.windows(MARKER.len()).any(|window| window == MARKER)
}

#[test]
fn an_encrypted_database_keeps_its_data_region_unreadable_and_opens_with_its_key_only() {
    let keys = key_file("enc-keys", &format!("main {}\n", "5a".repeat(32)));
    let source = KeySource::File(keys.clone());
    let path = unique_temp_path("enc-canary");
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    let (root, blob) = fill(&storage, 400);
    // A spill page reads back within the process that wrote it.
    let spill = storage.allocate_extent(true).expect("temp extent");
    let mut page = MARKER.repeat(PAGE_SIZE / MARKER.len());
    page.resize(PAGE_SIZE, 0);
    storage.spill_write_page(spill, &page).expect("spill write");
    let mut back = vec![0u8; PAGE_SIZE];
    storage
        .spill_read_page(spill, &mut back)
        .expect("spill read");
    assert_eq!(back, page);
    // Everything is on disk (the checkpoint flushed the pool).
    assert!(!holds_marker(&data_region(&path, &storage)));
    drop(storage);

    let mut magic = [0u8; 8];
    std::fs::File::open(&path)
        .and_then(|mut file| file.read_exact(&mut magic))
        .expect("read magic");
    assert_eq!(&magic, b"TRUTHDBE");

    // With its key: the rows, the blob and the snapshot read back.
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("reopen");
    let rows = rows_of(&storage, root).expect("rows");
    assert_eq!(rows, (0..400).map(row).collect::<Vec<_>>());
    let mut body = vec![0u8; crate::relstore::page::BLOB_PAGE_DATA];
    storage
        .lock()
        .page_ctx(0)
        .read_blob_page(blob, &mut body)
        .expect("blob page");
    assert!(body.starts_with(MARKER));
    let snapshot = storage.lock().load_snapshot().expect("snapshot");
    assert_eq!(snapshot.expect("a snapshot").data, MARKER.repeat(1000));
    drop(storage);

    // Without a key, or with another key under the same id: refused.
    let other = key_file("enc-other", &format!("main {}\n", "a5".repeat(32)));
    let refused = Storage::open_with_keys(
        path.clone(),
        no_engine,
        Some(&KeySource::File(other.clone())),
    );
    assert!(refused.is_err());
    if crate::encryption::test_key_source().is_none() {
        let refused = Storage::open(path.clone(), no_engine)
            .err()
            .expect("no key");
        assert!(refused.to_string().contains("encrypted"), "{refused}");
    }
    for file in [&path, &keys, &other] {
        std::fs::remove_file(file).ok();
    }
}

#[test]
fn a_plain_database_shows_the_marker_the_encrypted_one_hides() {
    // The scan above finds the marker when it is there.
    if crate::encryption::test_key_source().is_some() {
        return;
    }
    let path = unique_temp_path("enc-control");
    let storage = Storage::create(path.clone(), test_storage_options(), no_engine).expect("create");
    fill(&storage, 400);
    assert!(holds_marker(&data_region(&path, &storage)));
    drop(storage);
    std::fs::remove_file(&path).ok();
}

#[test]
fn a_changed_byte_or_a_misplaced_page_is_refused() {
    use std::os::unix::fs::FileExt;
    let keys = key_file("enc-tamper-keys", &format!("main {}\n", "3c".repeat(32)));
    let source = KeySource::File(keys.clone());
    let path = unique_temp_path("enc-tamper");
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    let (root, blob) = fill(&storage, 400);
    let root_offset = storage.lock().data_page_offset(root);
    let blob_offset = storage.lock().data_page_offset(blob);
    drop(storage);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open");
    let mut original = vec![0u8; PAGE_SIZE];
    file.read_exact_at(&mut original, root_offset)
        .expect("read root");

    // One byte of the root page's ciphertext changed.
    let mut changed = original.clone();
    changed[1000] ^= 1;
    file.write_all_at(&changed, root_offset).expect("tamper");
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open");
    let err = rows_of(&storage, root)
        .err()
        .expect("a changed byte is refused");
    assert!(err.to_string().contains("checksum mismatch"), "{err}");
    drop(storage);

    // Another page's (valid) image written where the root belongs.
    let mut elsewhere = vec![0u8; PAGE_SIZE];
    file.read_exact_at(&mut elsewhere, blob_offset)
        .expect("read blob page");
    file.write_all_at(&elsewhere, root_offset)
        .expect("misplace");
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open");
    assert!(
        rows_of(&storage, root).is_err(),
        "a misplaced page is refused"
    );
    drop(storage);

    // The original image restored: readable again.
    file.write_all_at(&original, root_offset).expect("restore");
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open");
    assert_eq!(rows_of(&storage, root).expect("rows").len(), 400);
    drop(storage);
    for file in [&path, &keys] {
        std::fs::remove_file(file).ok();
    }
}

/// Overwrites the header page with garbage.
fn damage_header(path: &Path) {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open");
    file.write_all_at(&[0x5Au8; PAGE_SIZE], 0)
        .expect("damage header");
}

#[test]
fn a_damaged_header_page_is_restored_from_its_copy_in_the_last_page() {
    let keys = key_file("enc-mirror-keys", &format!("main {}\n", "6b".repeat(32)));
    let source = KeySource::File(keys.clone());
    let path = unique_temp_path("enc-mirror");
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    let (root, _) = fill(&storage, 50);
    drop(storage);

    damage_header(&path);
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source))
        .expect("opens from the copy");
    assert_eq!(rows_of(&storage, root).expect("rows").len(), 50);
    drop(storage);
    let header = std::fs::read(&path).expect("read")[..8].to_vec();
    assert_eq!(header, b"TRUTHDBE", "the header page was written back");

    // After a grow, the copy follows the file's new end.
    let pages = Storage::grow_data_region(&path, 1).expect("grow");
    damage_header(&path);
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source))
        .expect("opens from the grown copy");
    assert_eq!(storage.lock().data_pages(), pages);
    assert_eq!(rows_of(&storage, root).expect("rows").len(), 50);
    drop(storage);
    for file in [&path, &keys] {
        std::fs::remove_file(file).ok();
    }
}

#[test]
fn a_plain_file_with_a_damaged_header_still_does_not_open() {
    if crate::encryption::test_key_source().is_some() {
        return;
    }
    let path = unique_temp_path("plain-damaged-header");
    let storage = Storage::create(path.clone(), test_storage_options(), no_engine).expect("create");
    drop(storage);
    damage_header(&path);
    let err = Storage::open(path.clone(), no_engine)
        .err()
        .expect("refused");
    assert!(err.to_string().contains("bad magic"), "{err}");
    std::fs::remove_file(&path).ok();
}

#[test]
fn an_interrupted_grow_leaves_no_copy_that_outlives_the_header() {
    use std::os::unix::fs::FileExt;
    let keys = key_file("enc-grow-keys", &format!("main {}\n", "4d".repeat(32)));
    let source = KeySource::File(keys.clone());
    let path = unique_temp_path("enc-grow");
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    fill(&storage, 50);
    let pages = storage.lock().data_pages();
    drop(storage);
    let mut header = vec![0u8; PAGE_SIZE];
    std::fs::File::open(&path)
        .and_then(|file| file.read_exact_at(&mut header, 0))
        .expect("read header");

    // A grow that stops after its copy, before its header: the database
    // goes on under the old layout.
    Storage::grow_data_region(&path, 1).expect("grow");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .and_then(|file| file.write_all_at(&header, 0))
        .expect("put the old header back");
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open");
    assert_eq!(storage.lock().data_pages(), pages);
    let (root, _) = fill(&storage, 30);
    storage
        .write_checkpoint(b"SECOND", 1, 2, 1)
        .expect("checkpoint");
    drop(storage);

    // A damaged header now comes back as the layout the database runs on.
    damage_header(&path);
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source))
        .expect("opens from the copy");
    assert_eq!(storage.lock().data_pages(), pages);
    assert_eq!(rows_of(&storage, root).expect("rows").len(), 30);
    let snapshot = storage.lock().load_snapshot().expect("snapshot");
    assert_eq!(snapshot.expect("a snapshot").data, b"SECOND");
    drop(storage);
    for file in [&path, &keys] {
        std::fs::remove_file(file).ok();
    }
}

#[test]
fn a_damaged_copy_is_rewritten_and_a_grow_cut_short_still_recovers() {
    use std::os::unix::fs::FileExt;
    let keys = key_file("enc-copy-keys", &format!("main {}\n", "2e".repeat(32)));
    let source = KeySource::File(keys.clone());
    let path = unique_temp_path("enc-copy");
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    let (root, _) = fill(&storage, 40);
    let pages = storage.lock().data_pages();
    drop(storage);
    let read_page = |offset: u64| {
        let mut page = vec![0u8; PAGE_SIZE];
        std::fs::File::open(&path)
            .and_then(|file| file.read_exact_at(&mut page, offset))
            .expect("read page");
        page
    };
    let write_page = |offset: u64, page: &[u8]| {
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .and_then(|file| file.write_all_at(page, offset))
            .expect("write page");
    };
    let len = std::fs::metadata(&path).expect("metadata").len();
    let header = read_page(0);

    // A damaged copy under a sound header is written again on open.
    write_page(len - PAGE_SIZE as u64, &[0x33; PAGE_SIZE]);
    drop(Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open"));
    assert_eq!(read_page(len - PAGE_SIZE as u64), header);

    // A grow that stopped after extending the file (with the current
    // layout's copy at the new end) and before the grown copy and header;
    // then the header is damaged: the copy at the end is of the current
    // layout, and the unused extension goes.
    Storage::grow_data_region(&path, 1).expect("grow");
    let grown = std::fs::metadata(&path).expect("metadata").len();
    write_page(grown - PAGE_SIZE as u64, &header);
    write_page(0, &header);
    damage_header(&path);
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source))
        .expect("opens from the copy at the end");
    assert_eq!(storage.lock().data_pages(), pages);
    assert_eq!(rows_of(&storage, root).expect("rows").len(), 40);
    drop(storage);
    assert_eq!(std::fs::metadata(&path).expect("metadata").len(), len);
    for file in [&path, &keys] {
        std::fs::remove_file(file).ok();
    }
}

#[test]
fn an_older_layouts_copy_is_never_taken_for_the_current_one() {
    use std::os::unix::fs::FileExt;
    let keys = key_file("enc-stale-keys", &format!("main {}\n", "1f".repeat(32)));
    let source = KeySource::File(keys.clone());
    let path = unique_temp_path("enc-stale");
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    fill(&storage, 20);
    drop(storage);
    // A completed grow leaves the old layout's copy inside the grown data
    // region; rows go into the grown database.
    Storage::grow_data_region(&path, 1).expect("grow");
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open");
    let (root, _) = fill(&storage, 30);
    drop(storage);
    let len = std::fs::metadata(&path).expect("metadata").len();
    let mut copy = vec![0u8; PAGE_SIZE];
    std::fs::File::open(&path)
        .and_then(|file| file.read_exact_at(&mut copy, len - PAGE_SIZE as u64))
        .expect("read copy");

    // The header and the current copy both damaged: refused, untouched.
    damage_header(&path);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .and_then(|file| file.write_all_at(&[0x77; PAGE_SIZE], len - PAGE_SIZE as u64))
        .expect("damage copy");
    assert!(Storage::open_with_keys(path.clone(), no_engine, Some(&source)).is_err());
    assert_eq!(std::fs::metadata(&path).expect("metadata").len(), len);

    // With the current copy back, everything reads.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .and_then(|file| file.write_all_at(&copy, len - PAGE_SIZE as u64))
        .expect("restore copy");
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open");
    assert_eq!(rows_of(&storage, root).expect("rows").len(), 30);
    drop(storage);
    for file in [&path, &keys] {
        std::fs::remove_file(file).ok();
    }
}

#[test]
fn a_grow_stopped_with_an_empty_last_page_is_cut_back_or_refused() {
    use std::os::unix::fs::FileExt;
    let keys = key_file("enc-empty-end-keys", &format!("main {}\n", "8c".repeat(32)));
    let source = KeySource::File(keys.clone());
    let path = unique_temp_path("enc-empty-end");
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    let (root, _) = fill(&storage, 25);
    drop(storage);
    let len = std::fs::metadata(&path).expect("metadata").len();
    let header = std::fs::read(&path).expect("read")[..PAGE_SIZE].to_vec();
    // The size grew but the extending write never landed: a zero last page.
    let extend = |path: &Path| {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open");
        file.set_len(len + (1 << 30)).expect("extend");
    };

    // With a damaged header: refused, nothing written, by open and by grow.
    extend(&path);
    damage_header(&path);
    assert!(Storage::open_with_keys(path.clone(), no_engine, Some(&source)).is_err());
    assert!(Storage::grow_data_region(&path, 1).is_err());
    assert_eq!(
        std::fs::metadata(&path).expect("metadata").len(),
        len + (1 << 30)
    );

    // With a sound header: the extension is cut off.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .and_then(|file| file.write_all_at(&header, 0))
        .expect("restore header");
    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open");
    assert_eq!(rows_of(&storage, root).expect("rows").len(), 25);
    drop(storage);
    assert_eq!(std::fs::metadata(&path).expect("metadata").len(), len);
    for file in [&path, &keys] {
        std::fs::remove_file(file).ok();
    }
}

#[test]
fn grow_does_not_restore_a_damaged_header_without_the_key() {
    let keys = key_file(
        "enc-grow-damaged-keys",
        &format!("main {}\n", "9d".repeat(32)),
    );
    let source = KeySource::File(keys.clone());
    let path = unique_temp_path("enc-grow-damaged");
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    fill(&storage, 10);
    drop(storage);
    let len = std::fs::metadata(&path).expect("metadata").len();
    damage_header(&path);
    let err = Storage::grow_data_region(&path, 1).err().expect("refused");
    assert!(err.to_string().contains("with its key"), "{err}");
    assert_eq!(std::fs::metadata(&path).expect("metadata").len(), len);
    let damaged = std::fs::read(&path).expect("read")[..8].to_vec();
    assert_eq!(damaged, vec![0x5A; 8], "the header page was not written");
    // A keyed open restores it; then grow works.
    drop(Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open"));
    Storage::grow_data_region(&path, 1).expect("grow");
    for file in [&path, &keys] {
        std::fs::remove_file(file).ok();
    }
}

fn holds_marker_in(path: &Path) -> bool {
    holds_marker(&std::fs::read(path).expect("read file"))
}

#[test]
fn the_log_backups_and_restores_of_an_encrypted_database_hold_no_plaintext() {
    let keys = key_file("enc-log-keys", &format!("main {}\n", "c4".repeat(32)));
    let source = KeySource::File(keys.clone());
    let [path, bak, log, bak2, dst] = [
        "enc-log",
        "enc-log-bak",
        "enc-log-arch",
        "enc-log-bak2",
        "enc-log-dst",
    ]
    .map(unique_temp_path);
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    super::txn_extents::set_full(&storage);
    storage.backup_full(&bak).expect("full backup");
    let (root, _) = fill(&storage, 400);
    storage.backup_log(&log, true, false).expect("log backup");
    storage.backup_full(&bak2).expect("second full backup");
    drop(storage);
    // The whole file (its log included), the backups and the log archive.
    for file in [&path, &bak, &log, &bak2] {
        assert!(!holds_marker_in(file), "{}", file.display());
    }

    // Restored elsewhere with its key: the full backup plus the log archive.
    Storage::restore_with_keys(
        &dst,
        &bak,
        &[log.clone()],
        None,
        false,
        no_engine,
        Some(&source),
    )
    .expect("restore");
    assert!(!holds_marker_in(&dst));
    let restored = Storage::open_with_keys(dst.clone(), no_engine, Some(&source)).expect("open");
    assert_eq!(rows_of(&restored, root).expect("rows").len(), 400);
    drop(restored);

    // Without its key, or with another key under the same id: refused.
    let other = key_file("enc-log-other", &format!("main {}\n", "4c".repeat(32)));
    let refused = unique_temp_path("enc-log-refused");
    let without = Storage::restore_with_keys(&refused, &bak2, &[], None, false, no_engine, None);
    let with_other = Storage::restore_with_keys(
        &refused,
        &bak2,
        &[],
        None,
        false,
        no_engine,
        Some(&KeySource::File(other.clone())),
    );
    assert!(without.is_err() && with_other.is_err());
    assert!(!refused.exists(), "a refused restore leaves no file");

    // A log archive of another database (another key) does not extend this
    // chain.
    let [path_b, bak_b, dst_b] =
        ["enc-log-b", "enc-log-b-bak", "enc-log-b-dst"].map(unique_temp_path);
    let b = Storage::create_with_keys(
        path_b.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create another");
    super::txn_extents::set_full(&b);
    b.backup_full(&bak_b).expect("backup of the other");
    drop(b);
    let err = Storage::restore_with_keys(
        &dst_b,
        &bak_b,
        &[log.clone()],
        None,
        false,
        no_engine,
        Some(&source),
    )
    .err()
    .expect("another database's archive is refused");
    assert!(err.to_string().contains("another encryption key"), "{err}");
    for file in [
        &path, &bak, &log, &bak2, &dst, &path_b, &bak_b, &keys, &other,
    ] {
        std::fs::remove_file(file).ok();
    }
}

#[test]
fn a_plain_databases_log_and_backup_show_the_marker() {
    // The scans above find the marker when it is there.
    if crate::encryption::test_key_source().is_some() {
        return;
    }
    let [path, bak] = ["plain-log", "plain-log-bak"].map(unique_temp_path);
    let storage = Storage::create(path.clone(), test_storage_options(), no_engine).expect("create");
    fill(&storage, 400);
    storage.backup_full(&bak).expect("backup");
    drop(storage);
    assert!(holds_marker_in(&path) && holds_marker_in(&bak));
    for file in [&path, &bak] {
        std::fs::remove_file(file).ok();
    }
}

#[test]
fn a_plaintext_log_entry_in_an_encrypted_database_ends_the_log() {
    use crate::storage_layout::{
        WAL_ENTRY_TYPE_RECORD, WalEntryFooter, WalEntryHeader, wal_entry_padded_len,
        wal_payload_crc,
    };
    let keys = key_file("enc-forged-keys", &format!("main {}\n", "d5".repeat(32)));
    let source = KeySource::File(keys.clone());
    let path = unique_temp_path("enc-forged");
    let storage = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    let first = b"the first entry".to_vec();
    storage
        .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, 1, &first)
        .expect("append");
    let second = storage
        .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, 2, b"the second entry")
        .expect("append");
    let (wal_offset, wal_size) = {
        let guard = storage.lock();
        (guard.layout.wal_offset, guard.layout.wal_size)
    };
    drop(storage);

    // A well-formed plaintext entry written over the second one.
    let forged = b"a forged entry".to_vec();
    let crc = wal_payload_crc(&forged);
    let header = WalEntryHeader::new(
        WAL_ENTRY_TYPE_RECORD,
        1,
        forged.len() as u32,
        2,
        second,
        crc,
    );
    let footer = WalEntryFooter {
        payload_len: forged.len() as u32,
        payload_crc: crc,
    };
    let mut entry = [
        header.to_le_bytes().as_slice(),
        &forged,
        &footer.to_le_bytes(),
    ]
    .concat();
    entry.resize(wal_entry_padded_len(forged.len()), 0);
    {
        use std::os::unix::fs::FileExt;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .and_then(|file| file.write_all_at(&entry, wal_offset + second % wal_size))
            .expect("forge");
    }

    let storage = Storage::open_with_keys(path.clone(), no_engine, Some(&source)).expect("open");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 1, "the forged entry is not accepted");
    assert_eq!(records[0].payload, first);
    drop(storage);
    for file in [&path, &keys] {
        std::fs::remove_file(file).ok();
    }
}

#[test]
fn a_standby_of_an_encrypted_database_applies_its_log_and_holds_no_plaintext() {
    let keys = key_file("enc-standby-keys", &format!("main {}\n", "e6".repeat(32)));
    let source = KeySource::File(keys.clone());
    let [path, bak, seed] =
        ["enc-standby", "enc-standby-bak", "enc-standby-seed"].map(unique_temp_path);
    let primary = Storage::create_with_keys(
        path.clone(),
        test_storage_options(),
        no_engine,
        Some(&source),
    )
    .expect("create encrypted");
    let summary = primary.backup_full(&bak).expect("backup");
    let (root, _) = fill(&primary, 300);
    let delta = primary
        .read_wal_range(summary.backup_end_lsn, primary.wal_flushed_lsn())
        .expect("ship");
    assert!(!holds_marker(&delta), "the shipped log is sealed");
    let fingerprint = primary.key_fingerprint();
    drop(primary);

    Storage::restore_with_keys(&seed, &bak, &[], None, true, no_engine, Some(&source))
        .expect("standby seed");
    let standby = Storage::open_with_keys(seed.clone(), no_engine, Some(&source)).expect("open");
    assert_eq!(standby.key_fingerprint(), fingerprint, "same database key");
    standby
        .apply_wal_stream(summary.backup_end_lsn, &delta)
        .expect("apply");
    assert!(standby.standby_restartpoint().expect("restartpoint"));
    drop(standby);
    assert!(!holds_marker_in(&seed));

    // Promoted with its key, it reads the rows.
    Storage::promote_with_keys(&seed, no_engine, Some(&source)).expect("promote");
    let promoted = Storage::open_with_keys(seed.clone(), no_engine, Some(&source)).expect("open");
    assert_eq!(rows_of(&promoted, root).expect("rows").len(), 300);
    drop(promoted);
    for file in [&path, &bak, &seed, &keys] {
        std::fs::remove_file(file).ok();
    }
}
