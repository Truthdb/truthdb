use super::*;

/// Buffered write into the file (test-side corruption / fixtures). The
/// sync makes it visible to subsequent O_DIRECT reads.
fn overwrite_bytes(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open for corruption");
    file.seek(SeekFrom::Start(offset)).expect("seek");
    file.write_all(bytes).expect("write");
    file.sync_all().expect("sync");
}

fn read_bytes(path: &Path, offset: u64, len: usize) -> Vec<u8> {
    let mut file = std::fs::File::open(path).expect("open for read");
    file.seek(SeekFrom::Start(offset)).expect("seek");
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf).expect("read");
    buf
}

fn append_search_entry(storage: &mut Storage, seq_no: u64, payload: &[u8]) -> u64 {
    storage
        .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, seq_no, payload)
        .expect("append wal entry")
}

/// Reads both superblocks from disk and returns the active (highest
/// valid generation) one.
fn read_active_superblock(path: &Path, layout: &StorageLayout) -> Superblock {
    let read_sb = |offset: u64| -> Option<Superblock> {
        let bytes = read_bytes(path, offset, SUPERBLOCK_SIZE);
        let sb = Superblock::from_le_bytes(bytes.as_slice().try_into().unwrap());
        (sb.checksum == sb.compute_checksum()).then_some(sb)
    };
    let a = read_sb(layout.superblock_a_offset);
    let b = read_sb(layout.superblock_b_offset);
    match (a, b) {
        (Some(a), Some(b)) => {
            if b.generation > a.generation {
                b
            } else {
                a
            }
        }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => panic!("no valid superblock"),
    }
}

fn test_layout() -> StorageLayout {
    compute_layout(test_storage_options(), TEST_WAL_BYTES, TEST_WAL_BYTES).expect("layout")
}

/// Writes a v1-format file: v1 header, superblocks, optional snapshot in
/// half-slot 0, WAL entries with v1 stamping (`logical_ts = seq_no`) and
/// garbage in the allocator bitmap (v1 half-slot bookkeeping the upgrade
/// must discard).
fn write_v1_fixture(
    path: &Path,
    wal_events: &[(u64, Vec<u8>)],
    snapshot: Option<(&[u8], u64, u64, u64)>,
) -> StorageLayout {
    let layout = test_layout();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .expect("create fixture");
    file.set_len(layout.total_size).expect("set_len");
    drop(file);

    let mut header = FileHeader::default();
    header.version = FILE_VERSION_V1;
    header.superblock_a_offset = layout.superblock_a_offset;
    header.superblock_b_offset = layout.superblock_b_offset;
    header.wal_offset = layout.wal_offset;
    header.wal_size = layout.wal_size;
    header.data_offset = layout.data_offset;
    header.data_size = layout.data_size;
    header.metadata_offset = layout.metadata_offset;
    header.metadata_size = layout.metadata_size;
    header.allocator_offset = layout.allocator_offset;
    header.allocator_size = layout.allocator_size;
    header.snapshot_offset = layout.snapshot_offset;
    header.snapshot_size = layout.snapshot_size;
    header.reserved_offset = layout.reserved_offset;
    header.reserved_size = layout.reserved_size;
    header.header_checksum = header.compute_checksum();
    overwrite_bytes(path, 0, &header.to_le_bytes_with_checksum());

    // WAL entries, sequentially from ring position 0, v1 stamping.
    let mut tail = 0u64;
    let mut last_seq = 0u64;
    for (seq_no, payload) in wal_events {
        let padded = wal_entry_padded_len(payload.len());
        let crc = wal_payload_crc(payload);
        let entry_header = WalEntryHeader::new(
            WAL_ENTRY_TYPE_RECORD,
            1,
            payload.len() as u32,
            *seq_no,
            *seq_no, // v1: logical_ts carried the engine seq
            crc,
        );
        let footer = WalEntryFooter {
            payload_len: payload.len() as u32,
            payload_crc: crc,
        };
        let mut bytes = Vec::with_capacity(padded);
        bytes.extend_from_slice(&entry_header.to_le_bytes());
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(&footer.to_le_bytes());
        bytes.resize(padded, 0);
        overwrite_bytes(path, layout.wal_offset + tail, &bytes);
        tail += padded as u64;
        last_seq = *seq_no;
    }

    // Snapshot in v1 half-slot 0 (start of the data region).
    if let Some((data, checkpoint_seq, next_seq_no, next_doc_id)) = snapshot {
        overwrite_bytes(path, layout.data_offset, data);
        let mut desc = SnapshotDescriptor::default();
        desc.generation = 1;
        desc.slot = 0;
        desc.checkpoint_seq = checkpoint_seq;
        desc.data_offset = layout.data_offset;
        desc.data_len = data.len() as u64;
        desc.data_checksum = xxh64(data, 0);
        desc.next_seq_no = next_seq_no;
        desc.next_doc_id = next_doc_id;
        desc.checksum = desc.compute_checksum();
        overwrite_bytes(
            path,
            layout.snapshot_offset,
            &desc.to_le_bytes_with_checksum(),
        );
    }

    // v1 half-slot allocator garbage the upgrade must wipe.
    overwrite_bytes(path, layout.allocator_offset, &[0xFF; 512]);

    let mut sb_a = Superblock::default();
    sb_a.generation = 2;
    sb_a.active = SUPERBLOCK_ACTIVE_A;
    sb_a.wal_tail = tail;
    sb_a.last_committed_seq = last_seq;
    sb_a.checksum = sb_a.compute_checksum();
    overwrite_bytes(
        path,
        layout.superblock_a_offset,
        &sb_a.to_le_bytes_with_checksum(),
    );
    let mut sb_b = Superblock::default();
    sb_b.active = SUPERBLOCK_ACTIVE_B;
    sb_b.checksum = sb_b.compute_checksum();
    overwrite_bytes(
        path,
        layout.superblock_b_offset,
        &sb_b.to_le_bytes_with_checksum(),
    );
    layout
}

#[test]
fn v1_fixture_upgrades_in_place_and_preserves_state() {
    let path = unique_temp_path("v1-upgrade");
    let snapshot_data = b"v1-snapshot-payload".as_slice();
    let events = vec![(6u64, vec![1u8; 100]), (7u64, vec![2u8; 50])];
    write_v1_fixture(&path, &events, Some((snapshot_data, 5, 8, 3)));

    let mut storage = Storage::open(path.clone()).expect("open v1 file");

    // Snapshot survives the upgrade.
    let snapshot = storage
        .load_snapshot()
        .expect("load snapshot")
        .expect("snapshot present");
    assert_eq!(snapshot.data, snapshot_data);
    assert_eq!(snapshot.checkpoint_seq, 5);
    assert_eq!(snapshot.next_seq_no, 8);
    assert_eq!(snapshot.next_doc_id, 3);

    // WAL entries survive and replay in order.
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].seq_no, 6);
    assert_eq!(records[0].payload, vec![1u8; 100]);
    assert_eq!(records[1].seq_no, 7);
    assert_eq!(records[1].payload, vec![2u8; 50]);

    // The allocator was rebuilt: only the live snapshot extent is
    // allocated; the v1 half-slot garbage is gone.
    assert!(storage.is_page_allocated(0), "snapshot extent must be live");
    assert!(
        !storage.is_page_allocated(1),
        "pages past the snapshot extent must be free"
    );
    assert!(
        !storage.is_page_allocated(100),
        "v1 bitmap garbage must have been wiped"
    );

    // New work post-upgrade: append + checkpoint + reopen.
    append_search_entry(&mut storage, 8, b"post-upgrade");
    storage
        .write_checkpoint(b"v2-snapshot", 8, 9, 4)
        .expect("checkpoint");
    drop(storage);

    // On-disk version is now v2; upgraded file opens cleanly.
    let header_bytes = read_bytes(&path, 0, FILE_HEADER_SIZE);
    let header = FileHeader::from_le_bytes(header_bytes.as_slice().try_into().unwrap());
    assert_eq!(header.version, FILE_VERSION);
    assert_eq!(header.header_checksum, header.compute_checksum());

    let mut storage = Storage::open(path.clone()).expect("reopen upgraded");
    let snapshot = storage
        .load_snapshot()
        .expect("load")
        .expect("second snapshot");
    assert_eq!(snapshot.data, b"v2-snapshot");
    assert!(
        storage.replay_wal_entries().expect("replay").is_empty(),
        "checkpoint reclaimed the wal"
    );
    // The upgraded snapshot's extent was freed once the v2 one became
    // durable; page 0 belonged to the v1 snapshot.
    assert!(!storage.is_page_allocated(0));
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn torn_tail_is_stopped_at_and_healed_by_whole_page_flush() {
    let path = unique_temp_path("torn-tail");
    let layout = test_layout();
    let mut storage = create_small(&path);
    let payload_a = vec![0xAAu8; 100];
    append_search_entry(&mut storage, 1, &payload_a);
    drop(storage); // crash: superblock still says tail = 0

    // Simulate a torn write of a follow-up entry: garbage on the tail
    // page right after entry A.
    let entry_a_len = wal_entry_padded_len(payload_a.len()) as u64;
    overwrite_bytes(&path, layout.wal_offset + entry_a_len, &[0x5Au8; 200]);

    // Recovery must stop at the garbage and keep A.
    let mut storage = Storage::open(path.clone()).expect("reopen after tear");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 1, "only entry A must survive the torn tail");
    assert_eq!(records[0].payload, payload_a);

    // The next append rewrites the whole tail page from memory, healing
    // the torn bytes: B lands exactly where the garbage was.
    let payload_b = vec![0xBBu8; 60];
    append_search_entry(&mut storage, 2, &payload_b);
    drop(storage);

    let mut storage = Storage::open(path.clone()).expect("reopen after heal");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, payload_a);
    assert_eq!(records[1].payload, payload_b);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn superblock_is_lazy_and_forward_scan_recovers_past_it() {
    let path = unique_temp_path("lazy-superblock");
    let layout = test_layout();
    let mut storage = create_small(&path);
    storage.lock().wal.set_superblock_interval(400);

    // 152 bytes per entry: the cadence write fires once, on entry 3
    // (456 bytes appended), and entry 4 stays past the recorded tail.
    let payloads: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i + 1; 100]).collect();
    let mut lsns = Vec::new();
    for (i, payload) in payloads.iter().enumerate() {
        lsns.push(append_search_entry(&mut storage, i as u64 + 1, payload));
    }
    drop(storage); // crash without checkpoint

    // The on-disk superblock lags the true tail (laziness) but is not 0
    // (the cadence rewrite fired).
    let sb = read_active_superblock(&path, &layout);
    let true_tail = lsns[3] + wal_entry_padded_len(payloads[3].len()) as u64;
    assert!(sb.wal_tail > 0, "cadence superblock write must have fired");
    assert!(
        sb.wal_tail < true_tail,
        "superblock tail {} must lag the true tail {true_tail}",
        sb.wal_tail
    );

    // Recovery scans forward past the stale superblock tail and finds
    // every entry.
    let mut storage = Storage::open(path.clone()).expect("reopen");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 4, "forward scan must recover all entries");
    for (record, payload) in records.iter().zip(&payloads) {
        assert_eq!(&record.payload, payload);
    }
    // LSN self-identity stamping.
    for (record, lsn) in records.iter().zip(&lsns) {
        assert_eq!(record.logical_ts, *lsn);
    }
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn wal_wraps_after_checkpoint_and_recovers_across_the_lap() {
    let path = unique_temp_path("wal-wrap");
    let mut storage = create_small(&path);

    // Fill ~60% of the 64 KiB ring, then reclaim it via checkpoint.
    for seq in 1..=5u64 {
        append_search_entry(&mut storage, seq, &vec![seq as u8; 8000]);
    }
    storage
        .write_checkpoint(b"state-at-5", 5, 6, 1)
        .expect("checkpoint");

    // These cross the lap boundary (one entry forces a wrap gap).
    let post: Vec<Vec<u8>> = (6..=9u64).map(|seq| vec![seq as u8; 8000]).collect();
    for (i, payload) in post.iter().enumerate() {
        append_search_entry(&mut storage, 6 + i as u64, payload);
    }
    drop(storage); // crash

    let mut storage = Storage::open(path.clone()).expect("reopen");
    let snapshot = storage
        .load_snapshot()
        .expect("load")
        .expect("snapshot present");
    assert_eq!(snapshot.data, b"state-at-5");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(
        records.len(),
        4,
        "exactly the post-checkpoint entries replay"
    );
    for (record, payload) in records.iter().zip(&post) {
        assert_eq!(&record.payload, payload);
    }
    // The ring stays usable after recovery on the wrapped lap.
    append_search_entry(&mut storage, 10, b"after-recovery");
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn wal_full_errors_and_checkpoint_reclaims() {
    let path = unique_temp_path("wal-full");
    let mut storage = create_small(&path);
    let payload = vec![7u8; 8000];
    let mut appended = 0;
    let err = loop {
        match storage.append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, appended + 1, &payload) {
            Ok(_) => appended += 1,
            Err(err) => break err,
        }
        assert!(appended < 100, "ring must fill up");
    };
    assert!(matches!(err, StorageError::WalFull(_)), "got: {err}");

    storage
        .write_checkpoint(b"reclaim", appended, appended + 1, 1)
        .expect("checkpoint");
    append_search_entry(&mut storage, appended + 1, &payload);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn extent_alloc_free_replays_from_wal() {
    let path = unique_temp_path("extent-replay");
    let mut storage = create_small(&path);
    let durable = storage.allocate_extent(false).expect("durable extent");
    let temp = storage.allocate_extent(true).expect("temp extent");
    assert_ne!(durable, temp);
    drop(storage); // crash: bitmap never persisted, only the WAL knows

    let mut storage = Storage::open(path.clone()).expect("reopen");
    assert!(
        storage.is_page_allocated(durable),
        "logged alloc must replay"
    );
    assert!(
        !storage.is_page_allocated(temp),
        "temp extents must vanish on restart"
    );

    storage.free_extent(durable).expect("free extent");
    drop(storage); // crash again

    let mut storage = Storage::open(path.clone()).expect("reopen after free");
    assert!(
        !storage.is_page_allocated(durable),
        "logged free must replay"
    );

    // Alloc + checkpoint: the bitmap carries the state once the WAL is
    // reclaimed.
    let kept = storage.allocate_extent(false).expect("extent");
    storage
        .write_checkpoint(b"with-extent", 1, 2, 1)
        .expect("checkpoint");
    drop(storage);

    let mut storage = Storage::open(path.clone()).expect("reopen after checkpoint");
    assert!(
        storage.replay_wal_entries().expect("replay").is_empty(),
        "wal reclaimed"
    );
    assert!(
        storage.is_page_allocated(kept),
        "bitmap must carry extents across checkpoints"
    );
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Regression (review finding): the whole-tail-page flush must never
/// let the page's zero suffix alias onto live entries at the head. With
/// a mid-page head and a nearly-full ring, the append that would have
/// clobbered the head must instead report WalFull, and every previously
/// acknowledged entry must survive a crash.
#[test]
fn tail_page_flush_never_overwrites_live_head_entries() {
    let path = unique_temp_path("tail-page-alias");
    let mut storage = create_small(&path);

    // Head becomes mid-page: one 5056-byte entry, then checkpoint.
    append_search_entry(&mut storage, 1, &vec![1u8; 5000]);
    storage
        .write_checkpoint(b"cp", 1, 2, 1)
        .expect("checkpoint");

    // Fill the ring almost entirely (the 15th entry wraps).
    let mut acked = Vec::new();
    for i in 0..15u64 {
        let payload = vec![(i + 2) as u8; 4000];
        append_search_entry(&mut storage, i + 2, &payload);
        acked.push(payload);
    }

    // This append fits the naive byte count but its tail-page zero
    // suffix would overwrite the oldest live entries; it must be
    // rejected.
    let err = storage
        .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, 17, &vec![9u8; 900])
        .expect_err("append aliasing the head must fail");
    assert!(matches!(err, StorageError::WalFull(_)), "got: {err}");
    drop(storage); // crash

    let mut storage = Storage::open(path.clone()).expect("reopen");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 15, "every acked entry must survive");
    for (record, payload) in records.iter().zip(&acked) {
        assert_eq!(&record.payload, payload);
    }
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Regression (review finding): a crash between descriptor fsync and
/// superblock publish leaves descriptor generations ahead of the
/// superblocks. The next checkpoint must mint a strictly higher
/// generation (no duplicates), and later opens must agree on the live
/// snapshot.
#[test]
fn checkpoint_after_superblock_publish_crash_mints_higher_generation() {
    let path = unique_temp_path("gen-minting");
    let layout = test_layout();
    let mut storage = create_small(&path);
    storage.write_checkpoint(b"one", 1, 2, 1).expect("cp 1");
    drop(storage);

    // Save the checkpoint-1-era superblocks.
    let sb_a = read_bytes(&path, layout.superblock_a_offset, SUPERBLOCK_SIZE);
    let sb_b = read_bytes(&path, layout.superblock_b_offset, SUPERBLOCK_SIZE);

    let mut storage = Storage::open(path.clone()).expect("reopen");
    storage.write_checkpoint(b"two", 2, 3, 1).expect("cp 2");
    drop(storage);

    // Simulate the crash window: descriptor of checkpoint 2 durable,
    // superblocks rolled back to checkpoint 1.
    overwrite_bytes(&path, layout.superblock_a_offset, &sb_a);
    overwrite_bytes(&path, layout.superblock_b_offset, &sb_b);

    let mut storage = Storage::open(path.clone()).expect("reopen in crash window");
    let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
    assert_eq!(snapshot.data, b"two", "newest descriptor must win");

    // The next checkpoint must not duplicate checkpoint 2's generation.
    storage.write_checkpoint(b"three", 3, 4, 1).expect("cp 3");
    drop(storage);

    let mut storage = Storage::open(path.clone()).expect("final reopen");
    let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
    assert_eq!(snapshot.data, b"three");
    // Allocator agrees with the snapshot choice: the live extent is
    // allocated and loadable, and further checkpoints keep working.
    storage.write_checkpoint(b"four", 4, 5, 1).expect("cp 4");
    let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
    assert_eq!(snapshot.data, b"four");
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Review finding: a durable wrap gap whose next-lap entry never made it
/// to disk must rewind the recovered tail to the gap start.
#[test]
fn wrap_gap_without_next_lap_entry_rewinds_tail() {
    let path = unique_temp_path("gap-rewind");
    let layout = test_layout();
    let mut storage = create_small(&path);
    for seq in 1..=5u64 {
        append_search_entry(&mut storage, seq, &vec![seq as u8; 8000]);
    }
    storage
        .write_checkpoint(b"cp", 5, 6, 1)
        .expect("checkpoint");
    let post: Vec<Vec<u8>> = (6..=9u64).map(|seq| vec![seq as u8; 8000]).collect();
    for (i, payload) in post.iter().enumerate() {
        append_search_entry(&mut storage, 6 + i as u64, payload);
    }
    drop(storage);

    // Entry 9 wrapped to the ring start. Erase its lap-2 pages as if the
    // gap reached disk but the entry itself never did.
    let entry_len = wal_entry_padded_len(8000);
    overwrite_bytes(&path, layout.wal_offset, &vec![0u8; entry_len]);

    let mut storage = Storage::open(path.clone()).expect("reopen");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 3, "the lost wrap entry must not replay");
    for (record, payload) in records.iter().zip(&post[..3]) {
        assert_eq!(&record.payload, payload);
    }

    // The rewound tail must be usable: a new append re-wraps and
    // survives another crash.
    append_search_entry(&mut storage, 9, b"after-rewind");
    drop(storage);
    let mut storage = Storage::open(path.clone()).expect("second reopen");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 4);
    assert_eq!(records[3].payload, b"after-rewind");
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Review finding: the in-place lazy superblock rewrite stakes its
/// safety on falling back to the other superblock plus the forward
/// scan. Exercise exactly that: corrupt the active superblock and
/// recover everything through the stale one.
#[test]
fn torn_active_superblock_falls_back_and_forward_scan_recovers() {
    let path = unique_temp_path("torn-superblock");
    let layout = test_layout();
    let mut storage = create_small(&path);
    storage.lock().wal.set_superblock_interval(400);
    let payloads: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i + 1; 100]).collect();
    for (i, payload) in payloads.iter().enumerate() {
        append_search_entry(&mut storage, i as u64 + 1, payload);
    }
    drop(storage);

    // The lazy writes all went to the active superblock (A). Tear it.
    overwrite_bytes(
        &path,
        layout.superblock_a_offset,
        &[0xEEu8; SUPERBLOCK_SIZE],
    );

    let mut storage = Storage::open(path.clone()).expect("reopen on backup superblock");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 4, "forward scan from the stale superblock");
    for (record, payload) in records.iter().zip(&payloads) {
        assert_eq!(&record.payload, payload);
    }
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Corruption inside the trusted region truncates the log there; the
/// corrected tail must be persisted at open so stale entries beyond it
/// can never re-enter a future trusted region.
#[test]
fn trusted_region_corruption_truncates_and_persists_corrected_tail() {
    let path = unique_temp_path("trusted-corruption");
    let layout = test_layout();
    let mut storage = create_small(&path);
    storage.lock().wal.set_superblock_interval(400);
    for seq in 1..=4u64 {
        append_search_entry(&mut storage, seq, &[seq as u8; 100]);
    }
    drop(storage);
    let entry_len = wal_entry_padded_len(100) as u64;

    // Corrupt entry 2, inside the superblock-trusted region.
    overwrite_bytes(&path, layout.wal_offset + entry_len + 8, &[0xDDu8; 32]);

    let mut storage = Storage::open(path.clone()).expect("reopen after corruption");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 1, "log truncates at the corrupt entry");
    assert_eq!(records[0].payload, vec![1u8; 100]);
    drop(storage);

    // The corrected (smaller) tail must now be on disk.
    let sb = read_active_superblock(&path, &layout);
    assert_eq!(
        sb.wal_tail, entry_len,
        "open must persist the corrected tail"
    );

    // New history: append a differently-sized entry and crash. Recovery
    // must see [entry 1, new entry] and never resurrect old entries 3/4.
    let mut storage = Storage::open(path.clone()).expect("reopen");
    storage.replay_wal_entries().expect("drain");
    append_search_entry(&mut storage, 2, &[9u8; 60]);
    drop(storage);
    let mut storage = Storage::open(path.clone()).expect("final reopen");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, vec![1u8; 100]);
    assert_eq!(records[1].payload, vec![9u8; 60]);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Review finding: allocate_extent's rollback when the WAL append fails
/// must leave the allocator exactly as before.
#[test]
fn allocate_extent_rolls_back_when_wal_is_full() {
    let path = unique_temp_path("extent-rollback");
    let mut storage = create_small(&path);
    // Fill the ring for large and small entries alike.
    let mut seq = 1u64;
    for payload_len in [8000usize, 1000, 100, 8] {
        while storage
            .append_wal_entry(WAL_ENTRY_TYPE_RECORD, 1, seq, &vec![7u8; payload_len])
            .is_ok()
        {
            seq += 1;
            assert!(seq < 1000, "ring must fill up");
        }
    }

    let err = storage
        .allocate_extent(false)
        .expect_err("extent alloc must fail when its record cannot be logged");
    assert!(matches!(err, StorageError::WalFull(_)), "got: {err}");
    for page in 0..EXTENT_PAGES {
        assert!(
            !storage.is_page_allocated(page),
            "rolled-back extent must leave page {page} free"
        );
    }

    // After reclaiming the ring, extent allocation works again and the
    // rolled-back range stays free (the next-fit cursor moved past it,
    // so it is not the range reused here).
    storage
        .write_checkpoint(b"reclaim", seq, seq + 1, 1)
        .expect("checkpoint");
    let start = storage.allocate_extent(false).expect("extent");
    for page in 0..EXTENT_PAGES {
        assert!(!storage.is_page_allocated(page));
    }
    assert!(storage.is_page_allocated(start));
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn empty_checkpoint_data_is_rejected() {
    let path = unique_temp_path("empty-checkpoint");
    let mut storage = create_small(&path);
    storage.write_checkpoint(b"valid", 1, 2, 1).expect("cp");
    let err = storage
        .write_checkpoint(b"", 2, 3, 1)
        .expect_err("empty checkpoint must be rejected");
    assert!(matches!(err, StorageError::InvalidConfig(_)), "got: {err}");
    // The previous snapshot is untouched.
    let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
    assert_eq!(snapshot.data, b"valid");
    drop(storage);
    let _ = std::fs::remove_file(path);
}

/// Review finding: the v1 upgrade must refresh BOTH superblocks — a v1
/// backup superblock is arbitrarily stale, and a later fallback to it
/// would lose every v1 WAL entry (they cannot pass the v2 forward scan).
#[test]
fn upgraded_v1_file_survives_active_superblock_loss() {
    let path = unique_temp_path("v1-superblock-refresh");
    let events = vec![(1u64, vec![5u8; 80]), (2u64, vec![6u8; 40])];
    let layout = write_v1_fixture(&path, &events, None);

    // First open performs the upgrade.
    let mut storage = Storage::open(path.clone()).expect("upgrade open");
    assert_eq!(storage.replay_wal_entries().expect("replay").len(), 2);
    drop(storage);

    // Lose the active superblock; the refreshed backup must carry the
    // v1 tail so the trusted scan still finds the v1-stamped entries.
    overwrite_bytes(
        &path,
        layout.superblock_a_offset,
        &[0xEEu8; SUPERBLOCK_SIZE],
    );
    let mut storage = Storage::open(path.clone()).expect("reopen on backup");
    let records = storage.replay_wal_entries().expect("replay");
    assert_eq!(records.len(), 2, "v1 entries must survive the fallback");
    assert_eq!(records[0].payload, vec![5u8; 80]);
    drop(storage);
    let _ = std::fs::remove_file(path);
}

#[test]
fn successive_checkpoints_recycle_snapshot_extents() {
    let path = unique_temp_path("snapshot-recycle");
    let mut storage = create_small(&path);
    storage.write_checkpoint(b"first", 1, 2, 1).expect("cp 1");
    let first_desc = storage
        .lock()
        .load_active_snapshot_descriptor()
        .expect("desc")
        .expect("present");
    let (first_start, first_pages) = storage
        .lock()
        .descriptor_page_range(&first_desc)
        .expect("range");
    assert!(storage.is_page_allocated(first_start));

    storage
        .write_checkpoint(b"second-snapshot", 2, 3, 1)
        .expect("cp 2");
    for page in first_start..first_start + first_pages {
        assert!(
            !storage.is_page_allocated(page),
            "first snapshot extent must be freed after the second checkpoint"
        );
    }
    drop(storage);

    let mut storage = Storage::open(path.clone()).expect("reopen");
    let snapshot = storage.load_snapshot().expect("load").expect("snapshot");
    assert_eq!(snapshot.data, b"second-snapshot");
    assert!(!storage.is_page_allocated(first_start));
    drop(storage);
    let _ = std::fs::remove_file(path);
}
