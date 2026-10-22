/// In-place v1 -> v2 upgrade:
/// 1. zero the allocator bitmap (v1's half-slot bookkeeping is meaningless
///    in v2 — `recover_allocator` re-marks the live snapshot extent);
/// 2. rewrite BOTH superblocks from the active one — v1's backup superblock
///    may be arbitrarily stale, and any later fallback to it would silently
///    drop fsync-acknowledged v1 WAL entries (v1 entries beyond a stale tail
///    cannot pass the v2 forward scan);
/// 3. stamp the new version.
///
/// Each step is fsync-fenced; a crash before step 3 leaves a v1 file and the
/// upgrade re-runs idempotently.
pub(super) fn upgrade_v1_to_v2(
    file: &mut DirectFile,
    mut header: FileHeader,
    layout: &StorageLayout,
    active_sb: &Superblock,
) -> Result<(Superblock, Superblock), StorageError> {
    let zero_page = AlignedPageBuf::new();
    let mut offset = header.allocator_offset;
    let end = header.allocator_offset + header.allocator_size;
    while offset < end {
        file.write_page_from(offset, &zero_page)?;
        offset += PAGE_SIZE as u64;
    }
    file.sync_data()?;

    let generation = active_sb.generation.saturating_add(1);
    let new_sb = |active_flag: u8| -> Superblock {
        let mut sb = *active_sb;
        sb.generation = generation;
        sb.active = active_flag;
        sb.checksum = sb.compute_checksum();
        sb
    };
    let superblock_a = new_sb(SUPERBLOCK_ACTIVE_A);
    let superblock_b = new_sb(SUPERBLOCK_ACTIVE_B);
    file.write_all_at(
        layout.superblock_a_offset,
        &superblock_a.to_le_bytes_with_checksum(),
    )?;
    file.sync_data()?;
    file.write_all_at(
        layout.superblock_b_offset,
        &superblock_b.to_le_bytes_with_checksum(),
    )?;
    file.sync_data()?;

    header.version = crate::storage_layout::FILE_VERSION;
    header.header_checksum = header.compute_checksum();
    file.write_all_at(0, &header.to_le_bytes_with_checksum())?;
    file.sync_data()?;
    Ok((superblock_a, superblock_b))
}

/// Picks the live snapshot descriptor slot by the highest
/// `(generation, slot index)` — one total order shared by every consumer
/// (allocator recovery and snapshot loading), so they can never disagree on
/// which descriptor is authoritative, even in the face of legacy duplicate
/// generations.
pub(super) fn live_descriptor_slot(descriptors: &[Option<SnapshotDescriptor>; 2]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (slot, desc) in descriptors.iter().enumerate() {
        let Some(desc) = desc else { continue };
        let better = match best {
            None => true,
            Some(b) => {
                let current = descriptors[b].as_ref().expect("best slot is valid");
                (desc.generation, slot) > (current.generation, b)
            }
        };
        if better {
            best = Some(slot);
        }
    }
    best
}

/// The layout a header describes. Its size is where its last region ends,
/// not the file's length: a grow that stopped before its header leaves the
/// file longer than the layout it runs on.
pub(super) fn layout_from_header(header: &FileHeader) -> StorageLayout {
    StorageLayout {
        total_size: header.reserved_offset.saturating_add(header.reserved_size),
        header_offset: 0,
        superblock_a_offset: header.superblock_a_offset,
        superblock_b_offset: header.superblock_b_offset,
        wal_offset: header.wal_offset,
        wal_size: header.wal_size,
        data_offset: header.data_offset,
        data_size: header.data_size,
        metadata_offset: header.metadata_offset,
        metadata_size: header.metadata_size,
        allocator_offset: header.allocator_offset,
        allocator_size: header.allocator_size,
        snapshot_offset: header.snapshot_offset,
        snapshot_size: header.snapshot_size,
        reserved_offset: header.reserved_offset,
        reserved_size: header.reserved_size,
    }
}

pub(super) fn validate_allocator_region(layout: &StorageLayout) -> Result<(), StorageError> {
    let bitmap_len = (layout.data_size / PAGE_SIZE as u64).div_ceil(8);
    if bitmap_len > layout.allocator_size {
        return Err(StorageError::InvalidConfig(format!(
            "allocator region ({} bytes) too small for data-region bitmap ({bitmap_len} bytes)",
            layout.allocator_size
        )));
    }
    Ok(())
}

pub(super) fn compute_layout(
    opts: StorageOptions,
    wal_min_bytes: u64,
    wal_max_bytes: u64,
) -> Result<StorageLayout, StorageError> {
    let total_size = opts
        .size_gib
        .checked_mul(crate::storage_layout::GIB)
        .ok_or_else(|| StorageError::InvalidConfig("storage.size_gib overflow".to_string()))?;

    let page = crate::storage_layout::PAGE_SIZE as u64;
    let fixed_size = page * 3;
    if total_size <= fixed_size {
        return Err(StorageError::InvalidConfig(
            "storage.size_gib too small for header/superblocks".to_string(),
        ));
    }

    let wal_raw = (total_size as f64 * opts.wal_ratio) as u64;
    let wal_clamped = wal_raw.clamp(wal_min_bytes, wal_max_bytes);
    let wal_size = align_down(wal_clamped, page);

    let metadata_size = align_down((total_size as f64 * opts.metadata_ratio) as u64, page);
    let snapshot_size = align_down((total_size as f64 * opts.snapshot_ratio) as u64, page);
    let allocator_size = align_down((total_size as f64 * opts.allocator_ratio) as u64, page);
    let reserved_target = align_down((total_size as f64 * opts.reserved_ratio) as u64, page);

    let mut remaining = total_size
        .saturating_sub(fixed_size)
        .saturating_sub(wal_size)
        .saturating_sub(metadata_size)
        .saturating_sub(snapshot_size)
        .saturating_sub(allocator_size)
        .saturating_sub(reserved_target);

    remaining = align_down(remaining, page);

    if remaining == 0 {
        return Err(StorageError::InvalidConfig(
            "storage ratios leave no space for data region".to_string(),
        ));
    }

    let reserved_size = total_size
        .saturating_sub(fixed_size)
        .saturating_sub(wal_size)
        .saturating_sub(metadata_size)
        .saturating_sub(snapshot_size)
        .saturating_sub(allocator_size)
        .saturating_sub(remaining);

    let header_offset = 0;
    let superblock_a_offset = page;
    let superblock_b_offset = page * 2;
    let wal_offset = page * 3;
    let data_offset = wal_offset + wal_size;
    let metadata_offset = data_offset + remaining;
    let allocator_offset = metadata_offset + metadata_size;
    let snapshot_offset = allocator_offset + allocator_size;
    let reserved_offset = snapshot_offset + snapshot_size;

    Ok(StorageLayout {
        total_size,
        header_offset,
        superblock_a_offset,
        superblock_b_offset,
        wal_offset,
        wal_size,
        data_offset,
        data_size: remaining,
        metadata_offset,
        metadata_size,
        allocator_offset,
        allocator_size,
        snapshot_offset,
        snapshot_size,
        reserved_offset,
        reserved_size,
    })
}

use super::*;

impl Storage {
    pub fn lock(&self) -> std::sync::MutexGuard<'_, Locked> {
        self.inner.lock().expect("storage mutex poisoned")
    }

    /// Wraps an opened/created [`StorageFile`] in a `Storage`, spawning the
    /// group-commit log-writer over a duplicated WAL fd.
    pub(super) fn with_log_writer(path: PathBuf, file: Locked) -> Result<Self, StorageError> {
        let wal_fd = file.try_clone_wal_fd()?;
        let encrypted = std::sync::atomic::AtomicBool::new(file.codec.is_encrypted());
        let (gc, log_writer) = GroupCommit::start(wal_fd, file.wal.sync_gate());
        Ok(Storage {
            path,
            inner: std::sync::Mutex::new(file),
            gc,
            sync_commit: SyncCommitState::default(),
            repl_connected: std::sync::Mutex::new(std::collections::HashSet::new()),
            log_writer: Some(log_writer),
            encrypted,
        })
    }

    pub fn open(path: PathBuf, engine: EngineFactory) -> Result<Self, StorageError> {
        Self::open_with_keys(path, engine, None)
    }

    /// Opens a database whose file may be encrypted: `keys` is where its
    /// master key comes from (an encrypted file does not open without it).
    pub fn open_with_keys(
        path: PathBuf,
        engine: EngineFactory,
        keys: Option<&crate::encryption::KeySource>,
    ) -> Result<Self, StorageError> {
        assert_layout_invariants();
        #[cfg(any(test, feature = "test-hooks"))]
        let test_keys = crate::encryption::test_key_source();
        #[cfg(any(test, feature = "test-hooks"))]
        let keys = keys.or(test_keys.as_ref());
        let file = StorageFile::open_existing(path.clone(), engine(), keys)?;
        Self::with_log_writer(path, file)
    }

    /// OFFLINE file growth (Stage 14): extends the data region of a CLOSED
    /// store by `add_gib` GiB. The server must not have the file open.
    ///
    /// The data region ends where the tail regions (metadata, allocator,
    /// snapshot, reserved) begin, so growth shifts the tail right by the
    /// delta. Crash safety comes from a size floor, not ordering tricks: the
    /// delta must be at least the tail's whole span, which puts every
    /// relocated region entirely in the fresh extension — nothing the OLD
    /// header points at is touched until the new header is stamped last
    /// (fsync-fenced, the v1→v2 upgrade's commit-point pattern). A crash
    /// before the stamp leaves a longer file under the old, fully valid
    /// layout; re-running the grow completes it.
    ///
    /// Everything inside the regions survives untouched: page numbers, RIDs
    /// and catalog roots are data-region-relative or absolute below the tail,
    /// the WAL sits before the data region, and the allocator bitmap is
    /// copied with zero (= free) extension bits — recovery replays pending
    /// WAL allocations on top exactly as it would have.
    pub fn grow_data_region(path: &Path, add_gib: u64) -> Result<u64, StorageError> {
        if add_gib == 0 {
            return Err(StorageError::InvalidConfig(
                "growth must be at least 1 GiB".to_string(),
            ));
        }
        let delta = add_gib.checked_mul(1024 * 1024 * 1024).ok_or_else(|| {
            StorageError::InvalidConfig(format!("growth of {add_gib} GiB overflows"))
        })?;
        let mut file = DirectFile::open_existing(path.to_path_buf())?;
        let (mut header, repair) = read_file_header(&mut file)?;
        // Offline, with no key: a header restored from its copy is not known
        // to be this file's until a keyed open has checked it. The other
        // repairs follow from a sound header (an unused extension cut off,
        // the copy put in step).
        if repair.header.is_some() {
            return Err(StorageError::InvalidFile(
                "the file header is damaged: open the database once (with its key) to \
                 restore it from its copy, then grow"
                    .to_string(),
            ));
        }
        repair.apply(&mut file)?;
        if header.version != crate::storage_layout::FILE_VERSION {
            return Err(StorageError::InvalidFile(format!(
                "grow requires a v{} file, found v{}",
                crate::storage_layout::FILE_VERSION,
                header.version
            )));
        }
        let tail_span = header.metadata_size
            + header.allocator_size
            + header.snapshot_size
            + header.reserved_size;
        if delta < tail_span {
            return Err(StorageError::InvalidConfig(format!(
                "growth of {add_gib} GiB is below the safe minimum of {} GiB for this file \
                 (the relocated regions must clear the old layout entirely)",
                tail_span.div_ceil(1024 * 1024 * 1024)
            )));
        }
        let new_data_size = header.data_size + delta;
        let new_data_pages = new_data_size / PAGE_SIZE as u64;
        let new_bitmap_len = new_data_pages.div_ceil(8);
        if new_bitmap_len > header.allocator_size {
            return Err(StorageError::InvalidConfig(format!(
                "the allocator region ({} bytes) cannot hold the bitmap for {} data pages",
                header.allocator_size, new_data_pages
            )));
        }

        // Read the payloads that move BEFORE any write: the allocator bitmap
        // (as much of it as the old data region used) and both snapshot
        // descriptor pages, verbatim.
        let old_bitmap_len = (header.data_size / PAGE_SIZE as u64).div_ceil(8) as usize;
        let mut bitmap = vec![0u8; old_bitmap_len];
        file.read_exact_at(header.allocator_offset, &mut bitmap)?;
        let mut descriptors = vec![0u8; 2 * PAGE_SIZE];
        file.read_exact_at(header.snapshot_offset, &mut descriptors)?;

        // Extend the file, fsynced before anything lands in the extension.
        // An encrypted file is extended by writing its copy (still of the
        // current layout) at the new end, so its last page is a copy at every
        // point of the grow.
        let old_total = header.reserved_offset + header.reserved_size;
        if header.is_encrypted() {
            file.write_all_at(
                old_total + delta - PAGE_SIZE as u64,
                &header.to_le_bytes_with_checksum(),
            )?;
            file.sync_data()?;
        } else {
            // A separate buffered handle: O_DIRECT is for page I/O, not
            // metadata.
            let plain = std::fs::OpenOptions::new().write(true).open(path)?;
            plain.set_len(old_total + delta)?;
            plain.sync_all()?;
            drop(plain);
        }

        // Write the relocated payloads into the extension (all beyond the
        // old file end by the size floor above). The bitmap's new bytes stay
        // zero — the grown pages are free.
        file.write_all_at(header.allocator_offset + delta, &bitmap)?;
        file.write_all_at(header.snapshot_offset + delta, &descriptors)?;
        file.sync_data()?;

        // Commit point: the header flips to the new layout.
        header.data_size = new_data_size;
        header.metadata_offset += delta;
        header.allocator_offset += delta;
        header.snapshot_offset += delta;
        header.reserved_offset += delta;
        header.header_checksum = header.compute_checksum();
        // An encrypted file's copy of the grown layout goes first (at the new
        // end): if the header write tears, the copy describes the grown
        // layout, already in place.
        if header.is_encrypted() {
            file.write_all_at(
                old_total + delta - PAGE_SIZE as u64,
                &header.to_le_bytes_with_checksum(),
            )?;
            file.sync_data()?;
        }
        file.write_all_at(0, &header.to_le_bytes_with_checksum())?;
        file.sync_data()?;
        Ok(new_data_pages)
    }

    pub fn create(
        path: PathBuf,
        opts: StorageOptions,
        engine: EngineFactory,
    ) -> Result<Self, StorageError> {
        Self::create_with_keys(path, opts, engine, None)
    }

    /// Creates a database, encrypted under a new database key wrapped by
    /// `keys`' master key when given.
    pub fn create_with_keys(
        path: PathBuf,
        opts: StorageOptions,
        engine: EngineFactory,
        keys: Option<&crate::encryption::KeySource>,
    ) -> Result<Self, StorageError> {
        Self::create_inner(path, opts, WAL_MIN_BYTES, WAL_MAX_BYTES, engine, keys)
    }

    /// Test hook: create with custom WAL ring bounds so ring-wrap paths can
    /// be exercised without writing hundreds of MiB.
    pub fn create_with_wal_bounds(
        path: PathBuf,
        opts: StorageOptions,
        wal_min_bytes: u64,
        wal_max_bytes: u64,
        engine: EngineFactory,
    ) -> Result<Self, StorageError> {
        Self::create_inner(path, opts, wal_min_bytes, wal_max_bytes, engine, None)
    }

    fn create_inner(
        path: PathBuf,
        opts: StorageOptions,
        wal_min_bytes: u64,
        wal_max_bytes: u64,
        engine: EngineFactory,
        keys: Option<&crate::encryption::KeySource>,
    ) -> Result<Self, StorageError> {
        assert_layout_invariants();
        opts.validate()?;
        #[cfg(any(test, feature = "test-hooks"))]
        let test_keys = crate::encryption::test_key_source();
        #[cfg(any(test, feature = "test-hooks"))]
        let keys = keys.or(test_keys.as_ref());
        let engines = engine();
        validate_engines(&engines)?;
        let core = StorageFile::create_new(path.clone(), opts, wal_min_bytes, wal_max_bytes, keys)?;
        let file = Locked { core, engines };
        Self::with_log_writer(path, file)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether the database's pages are encrypted (without the storage
    /// lock).
    pub fn is_encrypted(&self) -> bool {
        self.encrypted.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Rewraps the database key under the master key now current (the first
    /// key of the key file, or the key command's `wrap`), keeping the key
    /// itself: the pages, log, backups and standbys are unaffected. The key
    /// file may then drop the key it replaced.
    ///
    /// The rewrap (which may run a key command, a KMS round trip) happens
    /// without the storage lock; the write checks the key block is still the
    /// one rewrapped.
    pub fn rotate_master_key(&self) -> Result<(), StorageError> {
        let (header, source) = self.lock().header_to_rotate()?;
        let block =
            crate::encryption::rewrap_key_block(header.key_block(), &source, &header.file_uuid)?;
        self.lock().write_key_block(header.key_block(), &block)
    }

    /// The database's encryption at rest, as `encryption status` and
    /// `sys.dm_database_encryption_keys` report it.
    pub fn encryption_status(&self) -> EncryptionStatus {
        let locked = self.lock();
        match &locked.key_record {
            Some(record) => {
                let (encryptor_type, key_id) =
                    crate::encryption::key_block_encryptor(&record.key_block);
                let rekeying = locked
                    .conversion
                    .as_ref()
                    .is_some_and(|conversion| conversion.is_rekey());
                EncryptionStatus {
                    encrypted: true,
                    converting: locked.conversion.is_some() && !rekeying,
                    rekeying,
                    percent_complete: locked.conversion_percent(),
                    encryptor_type,
                    key_id,
                    fingerprint: Some(record.fingerprint),
                }
            }
            None => EncryptionStatus {
                encrypted: false,
                converting: false,
                rekeying: false,
                percent_complete: None,
                encryptor_type: "NONE",
                key_id: None,
                fingerprint: None,
            },
        }
    }

    /// The database key's fingerprint (zeros when not encrypted): equal for
    /// a database, its restores and its standbys.
    pub fn key_fingerprint(&self) -> [u8; 16] {
        self.lock()
            .codec
            .wal_keys()
            .map_or([0; 16], |keys| keys.fingerprint())
    }
}

/// Repairs an encrypted file's header reading found, applied by the caller
/// only once the file is known to be its own (its key block unlocked).
#[derive(Default)]
struct HeaderRepair {
    /// The header page, damaged: write this copy over it.
    header: Option<[u8; crate::storage_layout::FILE_HEADER_SIZE]>,
    /// A grow that stopped before its header left an unused extension.
    truncate_to: Option<u64>,
    /// The copy in the layout's last page, missing or damaged: write it.
    copy: Option<(u64, [u8; crate::storage_layout::FILE_HEADER_SIZE])>,
}

impl HeaderRepair {
    fn apply(self, file: &mut DirectFile) -> Result<(), StorageError> {
        if let Some(header) = self.header {
            file.write_all_at(0, &header)?;
            file.sync_data()?;
        }
        if let Some(len) = self.truncate_to {
            file.set_len(len)?;
        }
        if let Some((offset, copy)) = self.copy {
            file.write_all_at(offset, &copy)?;
            file.sync_data()?;
        }
        Ok(())
    }
}

/// Reads and checks the file header. The wrapped key of an encrypted file
/// lives only in its header, so the file also keeps a copy in its last
/// page: when the header page is damaged, that copy is used.
///
/// The file's last page always holds a copy of the layout the database runs
/// on. A grow extends the file by writing the current header's copy at the
/// new end, then writes the grown layout's copy there, then the header. So a
/// copy at the end whose layout ends before the file does is a grow cut
/// short: its layout is the current one, and the extension is unused. No
/// other page is ever taken for a copy (an older layout's copy stays behind
/// inside the grown data region).
fn read_file_header(file: &mut DirectFile) -> Result<(FileHeader, HeaderRepair), StorageError> {
    let page = PAGE_SIZE as u64;
    let mut bytes = [0u8; crate::storage_layout::FILE_HEADER_SIZE];
    file.read_exact_at(0, &mut bytes)?;
    let header = FileHeader::from_le_bytes(&bytes);
    let sound = |header: &FileHeader| {
        header.has_known_magic() && header.header_checksum == header.compute_checksum()
    };
    let len = file.len();
    if sound(&header) {
        let mut repair = HeaderRepair::default();
        if header.is_encrypted() {
            let end = layout_end(&header)
                .filter(|end| *end <= len)
                .ok_or_else(|| {
                    StorageError::InvalidFile(
                        "the file header's regions are inconsistent".to_string(),
                    )
                })?;
            repair.truncate_to = (len > end).then_some(end);
            let mut copy = [0u8; crate::storage_layout::FILE_HEADER_SIZE];
            file.read_exact_at(end - page, &mut copy)?;
            if copy != bytes {
                repair.copy = Some((end - page, bytes));
            }
        }
        return Ok((header, repair));
    }
    if len >= 2 * page {
        let mut copy_bytes = [0u8; crate::storage_layout::FILE_HEADER_SIZE];
        file.read_exact_at(len - page, &mut copy_bytes)?;
        let copy = FileHeader::from_le_bytes(&copy_bytes);
        if let Some(end) = layout_end(&copy).filter(|end| *end <= len)
            && sound(&copy)
            && copy.is_encrypted()
        {
            let mut repair = HeaderRepair {
                header: Some(copy_bytes),
                ..HeaderRepair::default()
            };
            if end < len {
                repair.truncate_to = Some(end);
                let mut own = [0u8; crate::storage_layout::FILE_HEADER_SIZE];
                file.read_exact_at(end - page, &mut own)?;
                if own != copy_bytes {
                    repair.copy = Some((end - page, copy_bytes));
                }
            }
            return Ok((copy, repair));
        }
    }
    if !header.has_known_magic() {
        return Err(StorageError::InvalidFile("bad magic".to_string()));
    }
    Err(StorageError::InvalidFile(
        "header checksum mismatch".to_string(),
    ))
}

/// An encrypted file's key record (which its backups carry).
fn key_record_of(
    header: &FileHeader,
    codec: &crate::encryption::PageCodec,
) -> Option<crate::backup::KeyRecord> {
    let keys = codec.wal_keys()?;
    Some(crate::backup::KeyRecord {
        file_uuid: header.file_uuid,
        key_block: header.key_block().to_vec(),
        fingerprint: keys.fingerprint(),
    })
}

/// The end of the layout a header describes (where its copy ends), when
/// every region it names lies below its reserved region and that region can
/// hold the copy.
fn layout_end(header: &FileHeader) -> Option<u64> {
    let end = header.reserved_offset.checked_add(header.reserved_size)?;
    let page = PAGE_SIZE as u64;
    for (offset, size) in [
        (header.superblock_a_offset, page),
        (header.superblock_b_offset, page),
        (header.wal_offset, header.wal_size),
        (header.data_offset, header.data_size),
        (header.metadata_offset, header.metadata_size),
        (header.allocator_offset, header.allocator_size),
        (header.snapshot_offset, header.snapshot_size),
    ] {
        if offset.checked_add(size)? > header.reserved_offset {
            return None;
        }
    }
    (header.reserved_size >= page).then_some(end)
}

/// What [`Storage::encryption_status`] reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionStatus {
    pub encrypted: bool,
    /// `encryption enable` is still converting the pages written before it.
    pub converting: bool,
    /// `encryption rotate database key` is still rewriting what the old key
    /// sealed.
    pub rekeying: bool,
    /// How far the conversion or rekeying is (`None`: neither under way).
    pub percent_complete: Option<u32>,
    /// `KEY FILE`, `KEY COMMAND` or `NONE`.
    pub encryptor_type: &'static str,
    /// The master key's id, for a key file.
    pub key_id: Option<String>,
    pub fingerprint: Option<[u8; 16]>,
}

#[cfg(test)]
thread_local! {
    /// Test fault injection: the next master key rotation stops after writing
    /// the header copy, before the header, as a crash there would.
    pub(crate) static CRASH_ROTATION_AFTER_COPY: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

impl StorageFile {
    /// The file header as it is on disk, checked.
    pub(super) fn header_on_disk(&mut self) -> Result<FileHeader, StorageError> {
        let mut bytes = [0u8; crate::storage_layout::FILE_HEADER_SIZE];
        self.file.read_exact_at(0, &mut bytes)?;
        let header = FileHeader::from_le_bytes(&bytes);
        if header.header_checksum != header.compute_checksum() {
            return Err(StorageError::InvalidFile(
                "the file header is damaged (its copy restores it at the next open)".to_string(),
            ));
        }
        Ok(header)
    }

    /// The header of an encrypted database and its master key source, for a
    /// rotation.
    fn header_to_rotate(
        &mut self,
    ) -> Result<(FileHeader, crate::encryption::KeySource), StorageError> {
        let header = self.header_on_disk()?;
        if !header.is_encrypted() {
            return Err(StorageError::InvalidConfig(
                "this database is not encrypted: it has no master key to rotate".to_string(),
            ));
        }
        let source = self.key_source.clone().ok_or_else(|| {
            StorageError::InvalidConfig("no master key is configured".to_string())
        })?;
        Ok((header, source))
    }

    /// Replaces the key block `old` by `block`. The new header goes to its
    /// copy in the last page first, then to the header page, each made
    /// durable: a crash leaves the old block or the new, and both unwrap the
    /// same key. Refused if the block is no longer `old` (another rotation
    /// ran meanwhile).
    pub(super) fn write_key_block(&mut self, old: &[u8], block: &[u8]) -> Result<(), StorageError> {
        let mut header = self.header_on_disk()?;
        if header.key_block() != old {
            return Err(StorageError::InvalidConfig(
                "the key block changed during the rotation (another rotation ran): retry"
                    .to_string(),
            ));
        }
        header.set_key_block(block);
        header.header_checksum = header.compute_checksum();
        let out = header.to_le_bytes_with_checksum();
        let end = layout_end(&header).ok_or_else(|| {
            StorageError::InvalidFile("the file header's regions are inconsistent".to_string())
        })?;
        self.file.write_all_at(end - PAGE_SIZE as u64, &out)?;
        self.file.sync_data()?;
        #[cfg(test)]
        if CRASH_ROTATION_AFTER_COPY.with(|crash| crash.replace(false)) {
            return Ok(());
        }
        self.file.write_all_at(0, &out)?;
        self.file.sync_data()?;
        if let Some(record) = &mut self.key_record {
            record.key_block = block.to_vec();
        }
        Ok(())
    }

    /// The current WAL tail (append position).
    pub(super) fn wal_tail(&self) -> u64 {
        self.wal.tail()
    }

    /// Duplicates the WAL file descriptor for the group-commit log-writer.
    pub(super) fn try_clone_wal_fd(&self) -> Result<std::fs::File, StorageError> {
        Ok(self.wal.try_clone_file()?)
    }

    pub(super) fn open_existing(
        path: PathBuf,
        mut engines: Vec<Box<dyn EngineParticipant>>,
        keys: Option<&crate::encryption::KeySource>,
    ) -> Result<Locked, StorageError> {
        validate_engines(&engines)?;
        let mut file = DirectFile::open_existing(path.clone())?;
        let (header, repair) = read_file_header(&mut file)?;
        if header.page_size as usize != crate::storage_layout::PAGE_SIZE {
            return Err(StorageError::InvalidFile("page size mismatch".to_string()));
        }
        let codec = if header.is_encrypted() {
            let source = keys.ok_or_else(|| {
                StorageError::InvalidConfig(
                    "this database is encrypted: configure [encryption] with its master key"
                        .to_string(),
                )
            })?;
            crate::encryption::codec_for_key_block(header.key_block(), source, &header.file_uuid)?
        } else {
            crate::encryption::PageCodec::Plain
        };
        // Validate the layout before any destructive step (the v1 upgrade
        // mutates the file; a file we cannot operate must be rejected while
        // it is still untouched v1).
        let layout = layout_from_header(&header);
        validate_allocator_region(&layout)?;
        // The key block unlocked and the layout sound: the header (or its
        // copy) is this file's.
        repair.apply(&mut file)?;

        let mut sb_a_bytes = [0u8; crate::storage_layout::SUPERBLOCK_SIZE];
        file.read_exact_at(header.superblock_a_offset, &mut sb_a_bytes)?;
        let mut superblock_a = Superblock::from_le_bytes(&sb_a_bytes);
        let sb_a_valid = superblock_a.checksum == superblock_a.compute_checksum();

        let mut sb_b_bytes = [0u8; crate::storage_layout::SUPERBLOCK_SIZE];
        file.read_exact_at(header.superblock_b_offset, &mut sb_b_bytes)?;
        let mut superblock_b = Superblock::from_le_bytes(&sb_b_bytes);
        let sb_b_valid = superblock_b.checksum == superblock_b.compute_checksum();

        if !sb_a_valid && !sb_b_valid {
            return Err(StorageError::InvalidFile(
                "both superblocks have checksum mismatch".to_string(),
            ));
        }

        let mut active_superblock = ActiveSuperblock::from_superblocks(
            &superblock_a,
            &superblock_b,
            sb_a_valid,
            sb_b_valid,
        );

        if header.version == crate::storage_layout::FILE_VERSION_V1 {
            let active_sb = match active_superblock {
                ActiveSuperblock::A => superblock_a,
                ActiveSuperblock::B => superblock_b,
            };
            (superblock_a, superblock_b) =
                upgrade_v1_to_v2(&mut file, header, &layout, &active_sb)?;
            active_superblock = ActiveSuperblock::A;
        }
        let active_sb = match active_superblock {
            ActiveSuperblock::A => &superblock_a,
            ActiveSuperblock::B => &superblock_b,
        };

        // Recover the true WAL tail: trust the superblock's tail as a lower
        // bound and scan forward (CRC + LSN self-identity) past it.
        let mut log_file = DirectFile::open_existing_unlocked(path.clone())?;
        let wal_keys = codec.wal_keys();
        let scan = scan_ring(
            &mut log_file,
            layout.wal_offset,
            layout.wal_size,
            active_sb.wal_head,
            active_sb.wal_tail,
            wal_keys.as_deref(),
        )?;
        let wal = WalWriter::open(
            log_file,
            layout.wal_offset,
            layout.wal_size,
            active_sb.wal_head,
            scan.tail,
            wal_keys.as_deref(),
        )?;
        file.share_sync_gate(wal.sync_gate());
        let recorded_tail = active_sb.wal_tail;

        // Every root is stored as an absolute file offset (0 = none): the
        // catalog engine's in `metadata_root`, every other engine's in the
        // engine-roots table.
        let root_page = |offset: u64| -> Result<Option<u64>, StorageError> {
            if offset == 0 {
                return Ok(None);
            }
            if offset < layout.data_offset || offset >= layout.data_offset + layout.data_size {
                return Err(StorageError::InvalidFile(
                    "engine root outside the data region".to_string(),
                ));
            }
            Ok(Some((offset - layout.data_offset) / PAGE_SIZE as u64))
        };
        let mut roots = std::collections::HashMap::new();
        roots.insert(CATALOG_ENGINE_ID, root_page(active_sb.metadata_root)?);
        for (engine_id, offset) in active_sb.engine_roots() {
            if engine_id == CATALOG_ENGINE_ID
                || !engines.iter().any(|eng| eng.engine_id() == engine_id)
            {
                return Err(StorageError::InvalidFile(format!(
                    "the file holds engine {engine_id}, which this build does not run"
                )));
            }
            roots.insert(engine_id, root_page(offset)?);
        }
        for eng in &mut engines {
            let root = roots.get(&eng.engine_id()).copied().flatten();
            eng.open(root, active_sb.db_options());
        }
        let last_log_backup_lsn = active_sb.last_log_backup_lsn();
        let repl_slots = active_sb.repl_slots();
        let recovery_full = active_sb.db_options() & 4 != 0;
        let key_record = key_record_of(&header, &codec);
        let key_source = keys.cloned();
        // A conversion or rekeying under way resumes; the entries scanned may
        // be plain or under the old key, the ones appended from here on are
        // sealed under the current one.
        let conversion = super::conversion::Conversion::resumed(&codec, scan.tail);
        let core = StorageFile {
            default_collation: header.default_collation(),
            file,
            wal,
            truncation_gate: LogTruncationGate::default(),
            max_slot_retain_bytes: u64::MAX,
            standby_att: std::collections::HashMap::new(),
            standby_search_floor: None,
            snapshot_next_seq_no: 0,
            last_log_backup_lsn,
            log_backup_in_progress: false,
            recovery_full,
            layout,
            superblock_a,
            superblock_b,
            active_superblock,
            allocator: PageAllocator::new(layout.data_size),
            pages: PageState::new(DEFAULT_CAPACITY_BYTES),
            replay_cache: scan.records,
            standby_pending_frees: Default::default(),
            codec,
            key_record,
            key_source,
            spill: crate::encryption::SpillCodec::new()?,
            conversion,
        };
        let mut storage = Locked { core, engines };
        // Re-establish the FULL-model log-backup hold so a checkpoint cannot
        // reclaim un-backed-up log. The floor is >= the persisted wal_head
        // (checkpoints were already clamped to it), so it never moves the head
        // backward.
        // A standby skips the hold: its seeded marker is the PRIMARY's log
        // chain (frozen at the backup point), and holding there would cap
        // every restartpoint at the seed forever, running the ring full. The
        // hold re-arms at promotion (a full reopen as a non-standby).
        if recovery_full && !active_sb.is_standby() {
            storage.register_log_backup_hold(last_log_backup_lsn);
        }
        // Re-seed the replication slots so their truncation hold survives the
        // restart (the persisted LSN is <= the live one — conservative, holds
        // more log, safe: redo is idempotent).
        for (id, lsn) in repl_slots {
            storage.truncation_gate.repl_slots.insert(id, lsn);
        }
        // A standby re-derives its restartpoint floors from the same records
        // recovery is about to scan: the active-transaction table (unresolved
        // BEGINs) and the first search record the seed snapshot does not cover.
        if active_sb.is_standby() {
            let descriptors = storage.read_snapshot_descriptors()?;
            storage.snapshot_next_seq_no = live_descriptor_slot(&descriptors)
                .and_then(|slot| descriptors[slot])
                .map(|desc| desc.next_seq_no)
                .unwrap_or(0);
            let rel_records: Vec<(u64, RelRecord)> = storage
                .replay_cache
                .iter()
                .filter(|record| record.entry_type == WAL_ENTRY_TYPE_REL)
                .map(|record| Ok((record.logical_ts, RelRecord::decode(&record.payload)?)))
                .collect::<Result<_, StorageError>>()?;
            for (lsn, record) in &rel_records {
                storage.standby_track_rel_record(*lsn, record);
            }
            storage.standby_search_floor = storage
                .replay_cache
                .iter()
                .find(|record| {
                    record.entry_type != WAL_ENTRY_TYPE_REL
                        && record.seq_no >= storage.snapshot_next_seq_no
                })
                .map(|record| record.logical_ts);
        }
        storage.recover_allocator()?;

        // A tail below the superblock's recorded one means part of the
        // trusted region was lost (media corruption, or a v1 file whose
        // superblock ran ahead of durability). Persist the corrected tail
        // now: otherwise entries appended at it could crash-recover with the
        // old superblock and stale entries beyond them would be replayed as
        // trusted.
        if scan.tail < recorded_tail {
            let last_seq = storage
                .replay_cache
                .iter()
                .map(|r| r.seq_no)
                .max()
                .unwrap_or(0);
            storage.write_active_superblock(last_seq)?;
            storage.file.sync_data()?;
        }

        // ARIES restart for the relational store: analysis + redo, catalog
        // reload, undo of losers with compensation logging. A standby (one that
        // has applied a live WAL stream) recovers REDO-ONLY — repeat history but
        // do not undo in-flight transactions, which the primary will commit and
        // whose continuation resumes above this standby's applied point.
        let redo_only = storage.active_sb().is_standby();
        // A standby is read-only until promotion: it appends nothing to its own
        // WAL (only the primary's log, via apply_wal_stream). Set before
        // recovery — which for a standby is redo-only and never appends, so
        // this blocks only later local writes.
        storage.wal.set_read_only(redo_only);
        storage.recover(redo_only)?;
        Ok(storage)
    }

    pub(super) fn create_new(
        path: PathBuf,
        opts: StorageOptions,
        wal_min_bytes: u64,
        wal_max_bytes: u64,
        keys: Option<&crate::encryption::KeySource>,
    ) -> Result<Self, StorageError> {
        let layout = compute_layout(opts.clone(), wal_min_bytes, wal_max_bytes)?;
        let keys = match keys {
            Some(source) => super::FileKeys::New(source),
            None => super::FileKeys::Plain,
        };
        Self::create_from_layout(path, layout, opts.default_collation, true, keys)
    }

    /// Creates a fresh file with an explicit layout. The restore path
    /// reconstructs the source's exact region sizes from the backup header
    /// rather than recomputing them from ratios, so it lays regions back
    /// byte-for-byte. Mirrors [`Self::create_new`] from
    /// `validate_allocator_region` onward.
    /// Creates a fresh file. With `openable` false (a restore in progress) its
    /// superblocks are written as zeros, failing their checksums, so the file
    /// does not open until the restore writes the restored ones: a crash
    /// mid-restore never leaves a database that opens.
    pub(super) fn create_from_layout(
        path: PathBuf,
        layout: StorageLayout,
        default_collation: Option<String>,
        openable: bool,
        keys: super::FileKeys<'_>,
    ) -> Result<Self, StorageError> {
        validate_allocator_region(&layout)?;
        let mut header = FileHeader::default();
        // Stamp the database's default collation into the file. Every character
        // column declared without an explicit COLLATE is keyed under it, so it
        // belongs to the data, not to whatever the config says at the next boot.
        if let Some(name) = default_collation.as_deref() {
            header
                .set_default_collation(name)
                .map_err(StorageError::InvalidConfig)?;
        }
        // An encrypted database: a fresh key, wrapped by the master key, in
        // the header; the file's uuid salts every key derived from it.
        let key_source = match &keys {
            super::FileKeys::New(source) | super::FileKeys::Existing(_, source) => {
                Some((*source).clone())
            }
            super::FileKeys::Plain => None,
        };
        let codec =
            match keys {
                super::FileKeys::New(source) => {
                    header.file_uuid = crate::encryption::random_uuid()?;
                    let (block, dek) = crate::encryption::new_key_block(source, &header.file_uuid)?;
                    header.set_key_block(&block);
                    crate::encryption::PageCodec::Encrypted(Box::new(
                        crate::encryption::PageKeys::new(&dek, &header.file_uuid, false)?,
                    ))
                }
                super::FileKeys::Existing(record, source) => {
                    header.file_uuid = record.file_uuid;
                    let codec = crate::encryption::codec_for_key_block(
                        &record.key_block,
                        source,
                        &record.file_uuid,
                    )?;
                    header.set_key_block(&record.key_block);
                    codec
                }
                super::FileKeys::Plain => crate::encryption::PageCodec::Plain,
            };
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

        let mut superblock_a = Superblock::default();
        superblock_a.checksum = superblock_a.compute_checksum();
        let mut superblock_b = Superblock::default();
        superblock_b.active = SUPERBLOCK_ACTIVE_B;
        superblock_b.checksum = superblock_b.compute_checksum();
        let superblock_bytes = |sb: Superblock| {
            if openable {
                sb.to_le_bytes_with_checksum()
            } else {
                [0u8; crate::storage_layout::SUPERBLOCK_SIZE]
            }
        };

        if header.is_encrypted() && layout.reserved_size < PAGE_SIZE as u64 {
            return Err(StorageError::InvalidConfig(
                "an encrypted database needs a reserved region of at least one page for the \
                 copy of its header (storage.reserved_ratio)"
                    .to_string(),
            ));
        }
        let mut file = DirectFile::create_new(path.clone(), layout.total_size)?;
        if header.is_encrypted() {
            file.write_all_at(
                layout.total_size - PAGE_SIZE as u64,
                &header.to_le_bytes_with_checksum(),
            )?;
        }
        file.write_all_at(layout.header_offset, &header.to_le_bytes_with_checksum())?;
        file.write_all_at(layout.superblock_a_offset, &superblock_bytes(superblock_a))?;
        file.write_all_at(layout.superblock_b_offset, &superblock_bytes(superblock_b))?;
        file.sync_data()?;

        let log_file = DirectFile::open_existing_unlocked(path.clone())?;
        let wal = WalWriter::open(
            log_file,
            layout.wal_offset,
            layout.wal_size,
            0,
            0,
            codec.wal_keys().as_deref(),
        )?;
        file.share_sync_gate(wal.sync_gate());

        let key_record = key_record_of(&header, &codec);
        // A restored file whose backup was taken mid-conversion (or
        // mid-rekeying) resumes it.
        let conversion = super::conversion::Conversion::resumed(&codec, 0);
        Ok(StorageFile {
            default_collation: header.default_collation(),
            file,
            wal,
            truncation_gate: LogTruncationGate::default(),
            max_slot_retain_bytes: u64::MAX,
            standby_att: std::collections::HashMap::new(),
            standby_search_floor: None,
            snapshot_next_seq_no: 0,
            last_log_backup_lsn: 0,
            log_backup_in_progress: false,
            recovery_full: false,
            layout,
            superblock_a,
            superblock_b,
            active_superblock: ActiveSuperblock::A,
            allocator: PageAllocator::new(layout.data_size),
            pages: PageState::new(DEFAULT_CAPACITY_BYTES),
            replay_cache: Vec::new(),
            standby_pending_frees: Default::default(),
            codec,
            key_record,
            key_source,
            spill: crate::encryption::SpillCodec::new()?,
            conversion,
        })
    }

    /// The active superblock (the authoritative in-memory copy).
    pub(super) fn active_sb(&self) -> &Superblock {
        match self.active_superblock {
            ActiveSuperblock::A => &self.superblock_a,
            ActiveSuperblock::B => &self.superblock_b,
        }
    }

    /// Fsyncs the restored file's data handle.
    pub(super) fn sync_file(&mut self) -> Result<(), StorageError> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Rebuilds both superblocks from the active slot, applies `mutate` to each,
    /// bumps the generation, and dual-writes them durably (active slot first,
    /// fsync between — a torn first write falls back to the other slot with the
    /// old state). Commits the new superblocks to memory only after both are
    /// durable. The single discipline behind every reserved-field update.
    pub(super) fn commit_superblock(
        &mut self,
        mutate: impl Fn(&mut Superblock),
    ) -> Result<(), StorageError> {
        let generation = self
            .superblock_a
            .generation
            .max(self.superblock_b.generation)
            .saturating_add(1);
        let (active, backup_flag) = match self.active_superblock {
            ActiveSuperblock::A => (self.superblock_a, SUPERBLOCK_ACTIVE_B),
            ActiveSuperblock::B => (self.superblock_b, SUPERBLOCK_ACTIVE_A),
        };
        let mut primary = active;
        let mut backup = active;
        backup.active = backup_flag;
        for sb in [&mut primary, &mut backup] {
            mutate(sb);
            sb.generation = generation;
            sb.checksum = sb.compute_checksum();
        }
        let (primary_offset, backup_offset) = match self.active_superblock {
            ActiveSuperblock::A => (
                self.layout.superblock_a_offset,
                self.layout.superblock_b_offset,
            ),
            ActiveSuperblock::B => (
                self.layout.superblock_b_offset,
                self.layout.superblock_a_offset,
            ),
        };
        self.file
            .write_all_at(primary_offset, &primary.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;
        self.file
            .write_all_at(backup_offset, &backup.to_le_bytes_with_checksum())?;
        self.file.sync_data()?;
        match self.active_superblock {
            ActiveSuperblock::A => {
                self.superblock_a = primary;
                self.superblock_b = backup;
            }
            ActiveSuperblock::B => {
                self.superblock_b = primary;
                self.superblock_a = backup;
            }
        }
        Ok(())
    }

    /// Lazily rewrites the active superblock in place (no fsync: it is a
    /// recovery-scan optimization, not a durability point; a torn write
    /// falls back to the other superblock).
    pub(super) fn write_active_superblock(
        &mut self,
        last_committed_seq: u64,
    ) -> Result<(), StorageError> {
        let generation = self
            .superblock_a
            .generation
            .max(self.superblock_b.generation)
            .saturating_add(1);

        let (head, tail) = (self.wal.head(), self.wal.tail());
        let (sb, offset) = match self.active_superblock {
            ActiveSuperblock::A => (&mut self.superblock_a, self.layout.superblock_a_offset),
            ActiveSuperblock::B => (&mut self.superblock_b, self.layout.superblock_b_offset),
        };
        sb.generation = generation;
        sb.active = match self.active_superblock {
            ActiveSuperblock::A => SUPERBLOCK_ACTIVE_A,
            ActiveSuperblock::B => SUPERBLOCK_ACTIVE_B,
        };
        sb.wal_head = head;
        sb.wal_tail = tail;
        sb.last_committed_seq = last_committed_seq;
        sb.checksum = sb.compute_checksum();
        let bytes = sb.to_le_bytes_with_checksum();
        self.wal.file_mut().write_all_at(offset, &bytes)?;
        Ok(())
    }

    pub(super) fn read_snapshot_descriptors(
        &mut self,
    ) -> Result<[Option<SnapshotDescriptor>; 2], StorageError> {
        let mut out = [None, None];
        for (slot, entry) in out.iter_mut().enumerate() {
            let desc_offset =
                self.layout.snapshot_offset + slot as u64 * SNAPSHOT_DESCRIPTOR_SIZE as u64;
            if desc_offset + SNAPSHOT_DESCRIPTOR_SIZE as u64
                > self.layout.snapshot_offset + self.layout.snapshot_size
            {
                continue;
            }
            let mut desc_bytes = [0u8; SNAPSHOT_DESCRIPTOR_SIZE];
            self.file.read_exact_at(desc_offset, &mut desc_bytes)?;
            let desc = SnapshotDescriptor::from_le_bytes(&desc_bytes);
            if desc.is_valid() {
                *entry = Some(desc);
            }
        }
        Ok(out)
    }

    pub(super) fn load_active_snapshot_descriptor(
        &mut self,
    ) -> Result<Option<SnapshotDescriptor>, StorageError> {
        let descriptors = self.read_snapshot_descriptors()?;
        Ok(live_descriptor_slot(&descriptors).and_then(|slot| descriptors[slot]))
    }

    pub(super) fn load_snapshot(&mut self) -> Result<Option<SnapshotData>, StorageError> {
        let desc = match self.load_active_snapshot_descriptor()? {
            Some(d) => d,
            None => return Ok(None),
        };

        let first_page =
            desc.data_offset.saturating_sub(self.layout.data_offset) / PAGE_SIZE as u64;
        // Sealed when its descriptor says so, or (a file created encrypted)
        // whenever the database is encrypted and not being converted.
        let sealed = desc.sealed() || self.codec.wal_keys().is_some_and(|keys| !keys.converting());
        // While the key is being replaced, a snapshot written before holds
        // the old key's ciphertext; raw pages carry no MAC, so the data
        // checksum tells which key opens it.
        let mut data = Vec::new();
        for previous in [false, true] {
            // Whole pages, as they were sealed; then the padding goes.
            data = vec![0u8; (desc.data_len as usize).div_ceil(PAGE_SIZE) * PAGE_SIZE];
            self.file.read_exact_at(desc.data_offset, &mut data)?;
            let mut opened = true;
            for (index, page) in data.chunks_mut(PAGE_SIZE).enumerate() {
                if !sealed {
                    break;
                }
                opened = self.codec.open_raw(
                    crate::encryption::RawPage::Snapshot,
                    first_page + index as u64,
                    desc.generation,
                    page,
                    previous,
                );
            }
            data.truncate(desc.data_len as usize);
            if opened && xxh64(&data, 0) == desc.data_checksum {
                break;
            }
            if previous || !sealed || !self.codec.rekeying() {
                return Err(StorageError::InvalidFile(
                    "snapshot data checksum mismatch".to_string(),
                ));
            }
        }

        Ok(Some(SnapshotData {
            data,
            checkpoint_seq: desc.checkpoint_seq,
            next_seq_no: desc.next_seq_no,
            next_doc_id: desc.next_doc_id,
        }))
    }
}
