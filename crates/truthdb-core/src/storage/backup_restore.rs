/// Pages copied per storage-lock acquisition during a backup (1 MiB at 4 KiB
/// pages): bounds both the lock hold and the in-flight copy buffer.
const BACKUP_CHUNK_PAGES: u64 = 256;

/// Releases the WAL truncation hold registered by `begin_backup` when the
/// backup ends — on every path, including an early error return or a panic
/// unwind. A leaked hold freezes WAL truncation for the life of the process.
struct BackupHoldGuard<'a> {
    storage: &'a Storage,
}

impl Drop for BackupHoldGuard<'_> {
    fn drop(&mut self) {
        self.storage.lock().release_backup_hold();
    }
}

/// Releases the `BACKUP LOG` single-flight guard when the operation ends — on
/// every path, including an early error return or a panic during the unlocked
/// archive write. A leaked guard would reject every later `BACKUP LOG`.
struct LogBackupGuard<'a> {
    storage: &'a Storage,
}

impl Drop for LogBackupGuard<'_> {
    fn drop(&mut self) {
        self.storage.lock().cancel_log_backup();
    }
}

/// Everything captured under the storage lock at the start of a backup, so the
/// bulk page copy can proceed while releasing the lock between chunks.
struct BackupPlan {
    layout: StorageLayout,
    default_collation: Option<String>,
    redo_start: u64,
    metadata_root: u64,
    last_committed_seq: u64,
    db_options: u8,
    epoch: u64,
    runs: Vec<(u64, u64)>,
    checksum: bool,
    copy_only: bool,
    finished_at_millis: u64,
}

impl BackupPlan {
    fn header(&self) -> crate::backup::BackupHeader {
        crate::backup::BackupHeader {
            format_version: crate::backup::FORMAT_VERSION,
            page_size: PAGE_SIZE as u32,
            total_size: self.layout.total_size,
            wal_size: self.layout.wal_size,
            data_size: self.layout.data_size,
            metadata_size: self.layout.metadata_size,
            allocator_size: self.layout.allocator_size,
            snapshot_size: self.layout.snapshot_size,
            reserved_size: self.layout.reserved_size,
            default_collation: self.default_collation.clone(),
            redo_start_lsn: self.redo_start,
            metadata_root: self.metadata_root,
            last_committed_seq: self.last_committed_seq,
            db_options: self.db_options,
            epoch: self.epoch,
            finished_at_millis: self.finished_at_millis,
            flags: crate::backup::BackupFlags {
                checksum: self.checksum,
                copy_only: self.copy_only,
                log_backup: false,
            },
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Writes a log-only `TDBBAK1` archive (one `LogChunk`) at `dst` and makes it
/// FULLY durable — data, then the parent directory's entry — before returning.
/// Called with the storage lock RELEASED. Directory durability matters because
/// the caller then advances the log-backup marker, which makes the shipped log
/// range reclaimable; if the archive's directory entry were not durable, a crash
/// could reclaim the log while the only archive copy lost its name.
fn write_log_archive(
    dst: &Path,
    header: &crate::backup::BackupHeader,
    start: u64,
    log: &[u8],
) -> Result<(), StorageError> {
    use crate::backup::{BackupWriter, BlockType, encode_log_chunk};
    let file = std::fs::File::create(dst)?;
    let mut writer = BackupWriter::new(file, header)?;
    writer.write_block(BlockType::LogChunk, &encode_log_chunk(start, log))?;
    writer.finish()?.sync_all()?;
    fsync_parent_dir(dst)?;
    Ok(())
}

/// Fsyncs the parent directory of `path` so a newly created file's name is
/// durable (POSIX: `fsync` on a file does not persist its directory entry).
fn fsync_parent_dir(path: &Path) -> Result<(), StorageError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

/// Applies one `BACKUP LOG` archive to a restore in progress: reads its log
/// range, verifies it continues from the current coverage (no gap — error 4305)
/// and that the whole recoverable range still fits the ring, then seeds it and
/// extends `tail`. Overlap with already-seeded log is harmless (same bytes).
/// Applies a raw shipped WAL ring range `[from_lsn, from_lsn + bytes.len())` to a
/// restore/standby file, extending the seeded ring. The sibling of
/// [`apply_log_archive`] for physical replication: the bytes are the primary's
/// own ring bytes (from `read_ring_range` at a flushed watermark), not a
/// `TDBBAK1` archive, so there is no per-block framing to decode — the range
/// starts and ends on WAL entry boundaries and recovery's forward scan validates
/// it (self-identity + CRC) on the next open.
///
/// `from_lsn` may be `<= tail` (a re-shipped, overlapping range overwrites
/// identical bytes — idempotent); a `from_lsn > tail` is a chain gap (4305). The
/// covered range must fit the ring's usable size, leaving the CLR reserve free
/// for recovery's undo, exactly as [`apply_log_archive`] caps a log chain.
fn apply_wal_range(
    file: &mut StorageFile,
    from_lsn: u64,
    bytes: &[u8],
    head: u64,
    tail: &mut u64,
) -> Result<(), StorageError> {
    if bytes.is_empty() {
        return Ok(());
    }
    if from_lsn > *tail {
        return Err(StorageError::InvalidFile(format!(
            "WAL range gap (4305): range begins at LSN {from_lsn} but the standby has reached {tail}"
        )));
    }
    let new_end = from_lsn + bytes.len() as u64;
    let max_range = file.layout.wal_size.saturating_sub(file.wal.reserve());
    if new_end.saturating_sub(head) > max_range {
        return Err(StorageError::InvalidFile(
            "the applied WAL range exceeds the ring's usable size; \
             incremental standby restore is not yet supported"
                .to_string(),
        ));
    }
    file.seed_ring(from_lsn, bytes)?;
    *tail = (*tail).max(new_end);
    Ok(())
}

fn apply_log_archive(
    file: &mut StorageFile,
    path: &Path,
    head: u64,
    tail: &mut u64,
    expected_epoch: u64,
) -> Result<(), StorageError> {
    use crate::backup::{BlockType, decode_log_chunk};
    let reader = std::io::BufReader::new(std::fs::File::open(path)?);
    let (mut r, header) = crate::backup::BackupReader::new(reader)?;
    if !header.flags.log_backup {
        return Err(StorageError::InvalidFile(format!(
            "{}: --log expects a BACKUP LOG archive, but this is a full backup",
            path.display()
        )));
    }
    // Timeline guard: after a failover, the old and new timelines' archives
    // share geometry AND continuous-looking LSNs — a mixed chain would restore
    // into a silent chimera. Only same-epoch archives may extend this chain.
    if header.epoch != expected_epoch {
        return Err(StorageError::InvalidFile(format!(
            "{}: log archive is from replication epoch {} but the restore chain is \
             epoch {expected_epoch} — a different timeline's archive cannot extend \
             this chain",
            path.display(),
            header.epoch
        )));
    }
    // Partial identity guard: a log archive whose page or ring geometry differs
    // was taken against a differently-configured database and cannot belong to
    // this chain. (A full database-identity match — a persisted DB uuid checked
    // against the full backup — is future work; today the LSN-continuity check
    // and geometry are the only cross-database guards.)
    if header.page_size as usize != PAGE_SIZE || header.wal_size != file.layout.wal_size {
        return Err(StorageError::InvalidFile(format!(
            "{}: log archive geometry does not match the restored database",
            path.display()
        )));
    }
    // Concatenate the archive's (contiguous) log chunks.
    let mut chunk: Option<(u64, Vec<u8>)> = None;
    while let Some((block_type, payload)) = r.next_block()? {
        if block_type == BlockType::LogChunk {
            let (start_lsn, bytes) = decode_log_chunk(&payload)?;
            match &mut chunk {
                Some((first, acc)) => {
                    if start_lsn != *first + acc.len() as u64 {
                        return Err(StorageError::InvalidFile(
                            "log archive chunks are not contiguous".to_string(),
                        ));
                    }
                    acc.extend_from_slice(bytes);
                }
                None => chunk = Some((start_lsn, bytes.to_vec())),
            }
        }
    }
    let Some((start_lsn, bytes)) = chunk else {
        return Ok(()); // an empty log backup contributes nothing
    };
    // Continuity: a start LATER than the current tail is a broken chain (4305).
    if start_lsn > *tail {
        return Err(StorageError::InvalidFile(format!(
            "log chain gap (4305): {} begins at LSN {start_lsn} but the restore has reached {tail}",
            path.display()
        )));
    }
    let new_end = start_lsn + bytes.len() as u64;
    // Leave the CLR reserve free: on open, ARIES undo appends compensation
    // records for the transactions in flight at the chain end (use_reserve), so
    // the recoverable range must fit `wal_size - reserve`, exactly as the full
    // backup caps its own shipped log. Otherwise a legitimate long chain fills
    // the ring and recovery's first undo append hits WalFull.
    let max_range = file.layout.wal_size.saturating_sub(file.wal.reserve());
    if new_end.saturating_sub(head) > max_range {
        return Err(StorageError::InvalidFile(
            "the full backup plus its log chain exceed the WAL ring's usable size; \
             incremental restore is not yet supported"
                .to_string(),
        ));
    }
    file.seed_ring(start_lsn, &bytes)?;
    *tail = (*tail).max(new_end);
    Ok(())
}

/// Rebuilds a [`StorageLayout`] from a backup header's region sizes. The
/// offsets follow the fixed region order (see [`compute_layout`]).
fn layout_from_backup_header(header: &crate::backup::BackupHeader) -> StorageLayout {
    let page = PAGE_SIZE as u64;
    let wal_offset = page * 3;
    let data_offset = wal_offset + header.wal_size;
    let metadata_offset = data_offset + header.data_size;
    let allocator_offset = metadata_offset + header.metadata_size;
    let snapshot_offset = allocator_offset + header.allocator_size;
    let reserved_offset = snapshot_offset + header.snapshot_size;
    StorageLayout {
        total_size: header.total_size,
        header_offset: 0,
        superblock_a_offset: page,
        superblock_b_offset: page * 2,
        wal_offset,
        wal_size: header.wal_size,
        data_offset,
        data_size: header.data_size,
        metadata_offset,
        metadata_size: header.metadata_size,
        allocator_offset,
        allocator_size: header.allocator_size,
        snapshot_offset,
        snapshot_size: header.snapshot_size,
        reserved_offset,
        reserved_size: header.reserved_size,
    }
}

/// Removes the page range `[start, start + len)` from a set of ascending,
/// disjoint allocated runs, splitting a run that straddles it.
fn subtract_run(runs: Vec<(u64, u64)>, start: u64, len: u64) -> Vec<(u64, u64)> {
    let cut_end = start + len;
    let mut out = Vec::with_capacity(runs.len());
    for (run_start, run_count) in runs {
        let run_end = run_start + run_count;
        if run_end <= start || run_start >= cut_end {
            out.push((run_start, run_count)); // disjoint
            continue;
        }
        if run_start < start {
            out.push((run_start, start - run_start)); // left remainder
        }
        if run_end > cut_end {
            out.push((cut_end, run_end - cut_end)); // right remainder
        }
    }
    out
}

use super::*;

impl Storage {
    /// The persisted log-backup floor (tests).
    #[cfg(test)]
    pub(crate) fn last_log_backup_lsn(&self) -> u64 {
        self.lock().last_log_backup_lsn
    }

    /// The active FULL-model log-backup truncation hold, if any (tests).
    #[cfg(test)]
    pub(crate) fn log_backup_hold(&self) -> Option<u64> {
        self.lock().truncation_gate.log_backup
    }

    /// Online full backup: writes a self-describing `TDBBAK1` file at `dst`
    /// capturing the database as of a consistent recovery point.
    ///
    /// The copy is fuzzy: pages are read in bounded chunks, each under the
    /// storage lock but releasing it between chunks so writers proceed. A
    /// truncation hold pins the WAL at `redo_start` for the duration, and the
    /// log `[redo_start, backup_end)` is shipped into the backup. `backup_end`
    /// is captured *after* the page copy, so it is at least the latest-change
    /// LSN of every page copied; ARIES redo therefore heals every page image —
    /// however stale, or with a future change already baked in — to the single
    /// `backup_end` point on restore, then undoes the transactions in flight
    /// there. `dst` is created; an existing file is truncated.
    pub fn backup_full(&self, dst: &Path) -> Result<crate::backup::BackupSummary, StorageError> {
        self.backup_full_with(dst, true, false)
    }

    /// Online full backup with explicit `WITH` options (`checksum` = verify
    /// every page as copied; `copy_only` = do not disturb the log-backup chain).
    pub fn backup_full_with(
        &self,
        dst: &Path,
        checksum: bool,
        copy_only: bool,
    ) -> Result<crate::backup::BackupSummary, StorageError> {
        let plan = self.lock().begin_backup(checksum, copy_only)?;
        // Release the hold on EVERY exit — normal return, an early `?` error out
        // of write_backup, or an unwind — via the guard's Drop. A leaked hold
        // permanently freezes WAL truncation and eventually wedges writes.
        let _hold = BackupHoldGuard { storage: self };
        self.write_backup(dst, &plan)
    }

    /// `BACKUP LOG`: ships the FULL-model log tail to a `TDBBAK1` log archive
    /// and advances the log-backup floor, releasing the ring it held. Requires
    /// the FULL recovery model.
    pub fn backup_log(
        &self,
        dst: &Path,
        checksum: bool,
        copy_only: bool,
    ) -> Result<crate::backup::BackupSummary, StorageError> {
        // Phase 1 (locked): capture the log range + header. The OLD marker still
        // pins `[start, ...)`, so no checkpoint can truncate the range we are
        // about to ship, even after we release the lock.
        let (header, start, end, log) = self.lock().begin_log_backup(checksum, copy_only)?;
        // Release the single-flight guard on EVERY exit — an early `?` out of
        // write_log_archive, a panic, or normal completion.
        let _guard = LogBackupGuard { storage: self };
        // Phase 2 (UNLOCKED): write and fsync the archive — including its parent
        // directory — so concurrent DML proceeds during the copy-out (BACKUP LOG
        // is online, like BACKUP DATABASE).
        write_log_archive(dst, &header, start, &log)?;
        // Phase 3 (locked): the archive is durable — durably advance the marker
        // and the hold (unless the FULL-model state changed meanwhile), releasing
        // `[start, end)` for reclamation. Copy-out strictly before truncate.
        self.lock().finish_log_backup(start, end)?;
        Ok(crate::backup::BackupSummary {
            redo_start_lsn: start,
            backup_end_lsn: end,
            pages_copied: 0,
            log_bytes: log.len() as u64,
            finished_at_millis: header.finished_at_millis,
        })
    }

    fn write_backup(
        &self,
        dst: &Path,
        plan: &BackupPlan,
    ) -> Result<crate::backup::BackupSummary, StorageError> {
        use crate::backup::{BlockType, encode_alloc_map, encode_log_chunk, encode_page_run};
        let file = std::fs::File::create(dst)?;
        let mut writer =
            crate::backup::BackupWriter::new(std::io::BufWriter::new(file), &plan.header())?;
        writer.write_block(BlockType::AllocMap, &encode_alloc_map(&plan.runs))?;

        let mut pages_copied = 0u64;
        let mut buf = vec![0u8; BACKUP_CHUNK_PAGES as usize * PAGE_SIZE];
        for &(run_start, run_count) in &plan.runs {
            let mut page = run_start;
            let end = run_start + run_count;
            while page < end {
                let chunk = (end - page).min(BACKUP_CHUNK_PAGES);
                let bytes = &mut buf[..chunk as usize * PAGE_SIZE];
                self.lock()
                    .read_pages_for_backup(page, chunk, bytes, plan.checksum)?;
                writer.write_block(BlockType::PageData, &encode_page_run(page, bytes))?;
                pages_copied += chunk;
                page += chunk;
            }
        }

        let (backup_end, log) = self.lock().ship_backup_log(plan.redo_start)?;
        writer.write_block(
            BlockType::LogChunk,
            &encode_log_chunk(plan.redo_start, &log),
        )?;
        writer.finish()?;
        Ok(crate::backup::BackupSummary {
            redo_start_lsn: plan.redo_start,
            backup_end_lsn: backup_end,
            pages_copied,
            log_bytes: log.len() as u64,
            finished_at_millis: plan.finished_at_millis,
        })
    }

    /// Offline restore: rebuilds a fresh database file at `dst_path` from the
    /// `TDBBAK1` backup at `bak_path`, then opens it once to run ARIES recovery,
    /// validating that the restored file is recoverable. `dst_path` must not
    /// already exist.
    /// Offline restore of a full backup with no log chain (recover to the full
    /// backup's own end).
    pub fn restore_full(dst_path: &Path, bak_path: &Path) -> Result<(), StorageError> {
        Self::restore_full_with_logs(dst_path, bak_path, &[], None)
    }

    /// Offline restore of a full backup followed by an ordered chain of
    /// `BACKUP LOG` archives. Recovers to the end of the last log, or — when
    /// `stop_at` is set — to that wall-clock point in time (transactions that
    /// committed past it are undone). Each archive must continue from where the
    /// previous coverage ended (no gap, error 4305); the whole recoverable range
    /// must fit in the WAL ring (a longer chain needs incremental restore, not
    /// yet supported).
    pub fn restore_full_with_logs(
        dst_path: &Path,
        bak_path: &Path,
        log_paths: &[std::path::PathBuf],
        stop_at: Option<u64>,
    ) -> Result<(), StorageError> {
        Self::restore_full_inner(dst_path, bak_path, log_paths, &[], stop_at, false)
    }

    /// Offline restore of a full backup (plus an optional `BACKUP LOG` chain)
    /// as a replication STANDBY seed: the file is stamped `is_standby` before
    /// its validating open, so recovery REPEATS history only (redo, no ARIES
    /// undo) and the file opens read-only. A plain restore's undo would roll
    /// back a transaction that was in flight at backup time with CLRs; if the
    /// primary later committed it, the CLRs' page LSNs would mask the shipped
    /// redo and the replica would silently diverge. Point-in-time restore is
    /// meaningless for a seed (the standby must match the primary, not a past
    /// point), so there is no `stop_at` here.
    pub fn restore_full_standby(
        dst_path: &Path,
        bak_path: &Path,
        log_paths: &[std::path::PathBuf],
    ) -> Result<(), StorageError> {
        Self::restore_full_inner(dst_path, bak_path, log_paths, &[], None, true)
    }

    /// Offline restore of a full backup followed by raw shipped WAL ring ranges —
    /// the physical-replication apply path (a standby seeded from a backup, fed
    /// the primary's `read_ring_range` bytes). Each range must continue from the
    /// current coverage (no gap, 4305); the whole recoverable range must fit the
    /// ring. Recovers to the end of the last range on open.
    pub fn restore_full_with_wal_ranges(
        dst_path: &Path,
        bak_path: &Path,
        wal_ranges: &[(u64, Vec<u8>)],
    ) -> Result<(), StorageError> {
        Self::restore_full_inner(dst_path, bak_path, &[], wal_ranges, None, false)
    }

    pub(super) fn restore_full_inner(
        dst_path: &Path,
        bak_path: &Path,
        log_paths: &[std::path::PathBuf],
        wal_ranges: &[(u64, Vec<u8>)],
        stop_at: Option<u64>,
        standby: bool,
    ) -> Result<(), StorageError> {
        assert_layout_invariants();
        let reader = std::io::BufReader::new(std::fs::File::open(bak_path)?);
        let (backup, header) = crate::backup::BackupReader::new(reader)?;
        if header.page_size as usize != PAGE_SIZE {
            return Err(StorageError::InvalidFile(
                "backup page size mismatch".to_string(),
            ));
        }
        let layout = layout_from_backup_header(&header);
        // The header is only integrity-checked (xxh64), not authenticated, so a
        // tampered-but-valid backup could carry inconsistent sizes or drive
        // writes outside the data region. Reject a header whose regions do not
        // tile the file exactly before creating anything (a bogus total_size
        // would otherwise `set_len` a huge sparse file).
        if layout.reserved_offset.checked_add(layout.reserved_size) != Some(layout.total_size)
            || layout.data_size == 0
        {
            return Err(StorageError::InvalidFile(
                "backup header region sizes are inconsistent".to_string(),
            ));
        }
        let file = StorageFile::create_from_layout(
            dst_path.to_path_buf(),
            layout,
            header.default_collation.clone(),
        )?;
        // The destination now exists and is ours: remove the partial file if any
        // later step fails, so a retry (which requires a fresh destination) can
        // proceed. Everything ABOVE this point errors without having created it.
        let outcome = Self::restore_body(
            file, backup, &header, log_paths, wal_ranges, dst_path, stop_at, standby,
        );
        if outcome.is_err() {
            let _ = std::fs::remove_file(dst_path);
        }
        outcome
    }

    /// The part of a restore that owns the (already-created) destination: lays
    /// down page images + the log, applies the log chain and any raw WAL ranges,
    /// writes the superblock, and validates by opening (running recovery). A
    /// failure here leaves a partial file the caller removes.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn restore_body(
        mut file: StorageFile,
        mut backup: crate::backup::BackupReader<std::io::BufReader<std::fs::File>>,
        header: &crate::backup::BackupHeader,
        log_paths: &[std::path::PathBuf],
        wal_ranges: &[(u64, Vec<u8>)],
        dst_path: &Path,
        stop_at: Option<u64>,
        standby: bool,
    ) -> Result<(), StorageError> {
        use crate::backup::{BlockType, decode_alloc_map, decode_log_chunk, decode_page_run};
        let data_pages = header.data_size / PAGE_SIZE as u64;
        let mut runs: Vec<(u64, u64)> = Vec::new();
        let mut log: Option<(u64, Vec<u8>)> = None;
        while let Some((block_type, payload)) = backup.next_block()? {
            match block_type {
                BlockType::AllocMap => {
                    runs = decode_alloc_map(&payload)?;
                    for &(start, count) in &runs {
                        if start.checked_add(count).is_none_or(|end| end > data_pages) {
                            return Err(StorageError::InvalidFile(
                                "backup allocation run is outside the data region".to_string(),
                            ));
                        }
                    }
                }
                BlockType::PageData => {
                    let (start, count, bytes) = decode_page_run(&payload)?;
                    if start.checked_add(count).is_none_or(|end| end > data_pages) {
                        return Err(StorageError::InvalidFile(
                            "backup page run is outside the data region".to_string(),
                        ));
                    }
                    file.restore_pages(start, count, bytes)?;
                }
                BlockType::LogChunk => {
                    let (start_lsn, bytes) = decode_log_chunk(&payload)?;
                    // One chunk today; a future split emitter must produce them
                    // contiguously in LSN order — enforce it so a gap or a
                    // reorder fails loudly rather than seeding a wrong range.
                    match &mut log {
                        Some((first_start, acc)) => {
                            if start_lsn != *first_start + acc.len() as u64 {
                                return Err(StorageError::InvalidFile(
                                    "backup log chunks are not contiguous".to_string(),
                                ));
                            }
                            acc.extend_from_slice(bytes);
                        }
                        None => log = Some((start_lsn, bytes.to_vec())),
                    }
                }
                BlockType::Header | BlockType::Trailer => {}
            }
        }

        file.restore_allocator_bitmap(&runs)?;
        let mut tail = match &log {
            Some((start_lsn, bytes)) => file.seed_ring(*start_lsn, bytes)?,
            None => header.redo_start_lsn,
        };
        // Apply the log chain in order, extending the seeded ring past the full
        // backup's end. Each archive continues from the current coverage (no
        // gap) and the whole range must fit the ring.
        for log_path in log_paths {
            apply_log_archive(
                &mut file,
                log_path,
                header.redo_start_lsn,
                &mut tail,
                header.epoch,
            )?;
        }
        // Apply raw shipped WAL ranges (physical replication) after the log
        // chain, on the same seeded ring, extending the tail further.
        for (from_lsn, bytes) in wal_ranges {
            apply_wal_range(
                &mut file,
                *from_lsn,
                bytes,
                header.redo_start_lsn,
                &mut tail,
            )?;
        }
        // The restored superblock brackets the ring at `[redo_start, tail)`; the
        // log-backup floor is the end of the applied chain (a fresh chain). A
        // standby seed is stamped BEFORE the validating open, so that open is
        // redo-only + read-only from the file's first moment.
        file.restore_superblock(header, tail, standby)?;
        file.sync_file()?;
        drop(file);

        // Validate + finalize: opening reruns allocator recovery + ARIES
        // relational recovery over the seeded ring. For a point-in-time restore,
        // recovery stops at `stop_at`, undoing later transactions with CLRs that
        // persist the point-in-time state across a normal reopen (their undo is
        // replayed and each undone txn is sealed with a TXN_END). Failing loudly
        // if the restored file is not recoverable.
        let storage = match stop_at {
            Some(ts) => Storage::open_with_stop_at(dst_path.to_path_buf(), ts)?,
            None => Storage::open(dst_path.to_path_buf())?,
        };
        drop(storage);
        Ok(())
    }
}

impl StorageFile {
    /// Registers an in-progress backup's `redo_start_lsn` as a truncation hold,
    /// so a concurrent checkpoint cannot reclaim log the backup still has to ship.
    /// Paired with [`Self::release_backup_hold`].
    pub(super) fn register_backup_hold(&mut self, redo_start_lsn: u64) {
        self.truncation_gate.backup = Some(redo_start_lsn);
    }

    /// Releases the backup truncation hold (the backup finished or failed).
    pub(super) fn release_backup_hold(&mut self) {
        self.truncation_gate.backup = None;
    }

    /// Pins the FULL-model log-backup floor at `last_log_backup_lsn` so a
    /// checkpoint cannot reclaim log a `BACKUP LOG` still has to ship. Set when
    /// FULL is enabled and re-set (advanced) after each `BACKUP LOG`.
    pub(super) fn register_log_backup_hold(&mut self, last_log_backup_lsn: u64) {
        self.truncation_gate.log_backup = Some(last_log_backup_lsn);
    }

    /// Drops the log-backup floor (recovery model set back to SIMPLE): log is
    /// reclaimable as soon as it is checkpointed.
    pub(super) fn release_log_backup_hold(&mut self) {
        self.truncation_gate.log_backup = None;
    }

    // --- Backup / restore (Stage 17) ---

    /// Captures a backup plan under the storage lock, rejecting a second
    /// concurrent backup, and registers the WAL truncation hold LAST (after all
    /// fallible work) so a failure here leaves no stale hold. On success the
    /// caller arms a [`BackupHoldGuard`] to release the hold on every exit.
    fn begin_backup(
        &mut self,
        checksum: bool,
        copy_only: bool,
    ) -> Result<BackupPlan, StorageError> {
        self.ensure_rel_usable()?;
        // Single-flight: the gate has one backup hold slot. A second concurrent
        // backup would overwrite the first's hold (and its release would clear
        // the survivor's), leaving a backup with a wrong or absent truncation
        // floor — a silently truncated restore. Reject it.
        if self.truncation_gate.backup.is_some() {
            return Err(StorageError::BackupInProgress);
        }
        let redo_start = self.wal.head();

        // The active superblock is the checkpoint whose head is redo_start, so
        // its roots are the checkpoint-consistent state recovery redoes onto.
        let (metadata_root, last_committed_seq, db_options) = {
            let active = match self.active_superblock {
                ActiveSuperblock::A => &self.superblock_a,
                ActiveSuperblock::B => &self.superblock_b,
            };
            debug_assert_eq!(
                redo_start, active.wal_head,
                "redo_start must equal the active checkpoint's wal_head"
            );
            (
                active.metadata_root,
                active.last_committed_seq,
                active.db_options(),
            )
        };

        let mut runs = self.allocator.allocated_runs();
        // The search-snapshot extent is a durable allocation (so allocated_runs
        // includes it) but holds raw, non-page-formatted bytes. This slice does
        // not preserve the search snapshot (Stage 19 retires it), so drop its
        // extent from the backup rather than copy pages that would fail checksum
        // verification and dangle unreferenced on restore.
        if let Some(desc) = self.load_active_snapshot_descriptor()? {
            let (start, pages) = self.descriptor_page_range(&desc)?;
            runs = subtract_run(runs, start, pages);
        }

        // Register the hold LAST, after every fallible step above: a failure
        // here must leave no stale hold behind (a leaked hold freezes WAL
        // truncation for the life of the process). We are still under the same
        // lock, so redo_start is unchanged.
        self.register_backup_hold(redo_start);
        Ok(BackupPlan {
            layout: self.layout,
            default_collation: self.default_collation.clone(),
            redo_start,
            metadata_root,
            last_committed_seq,
            db_options,
            epoch: self.active_sb().epoch(),
            runs,
            checksum,
            copy_only,
            finished_at_millis: now_millis(),
        })
    }

    /// Reads `count` data pages starting at `start_page` into `out`, verifying
    /// each page's checksum unless `checksum` is off (an all-zero page is an
    /// unwritten allocation and always passes).
    pub(super) fn read_pages_for_backup(
        &mut self,
        start_page: u64,
        count: u64,
        out: &mut [u8],
        checksum: bool,
    ) -> Result<(), StorageError> {
        debug_assert_eq!(out.len(), count as usize * PAGE_SIZE);
        for i in 0..count as usize {
            let page = start_page + i as u64;
            let slot = &mut out[i * PAGE_SIZE..(i + 1) * PAGE_SIZE];
            self.spill_read_page(page, slot)?;
            if checksum
                && !crate::relstore::page::is_zero_page(slot)
                && !crate::relstore::page::verify_checksum(slot)
                && self.page_is_live_regular(page)?
            {
                return Err(StorageError::InvalidFile(format!(
                    "backup aborted: data page {page} failed checksum (corrupt source page)"
                )));
            }
        }
        Ok(())
    }

    /// True iff `page` is still a live, page-formatted data page — so a checksum
    /// failure genuinely means source corruption. A page that has been freed
    /// since the backup began, or reused as the raw (non-page-formatted)
    /// search-snapshot extent by a between-chunk checkpoint, legitimately fails
    /// page-checksum verification and is NOT corrupt: on restore its stale
    /// image is irrelevant because redo frees or overwrites it.
    pub(super) fn page_is_live_regular(&mut self, page: u64) -> Result<bool, StorageError> {
        if !self.allocator.is_allocated(page) {
            return Ok(false); // freed since the backup began
        }
        if let Some(desc) = self.load_active_snapshot_descriptor()? {
            let (start, pages) = self.descriptor_page_range(&desc)?;
            if page >= start && page < start + pages {
                return Ok(false); // reused as the raw snapshot extent
            }
        }
        Ok(true)
    }

    /// Forces the log durable, captures `backup_end = tail`, and returns the raw
    /// ring bytes for `[redo_start, backup_end)`. The physical copy handles the
    /// ring wrap; the range never exceeds one ring lap (the truncation hold
    /// keeps `tail - head <= wal_size`).
    pub(super) fn ship_backup_log(
        &mut self,
        redo_start: u64,
    ) -> Result<(u64, Vec<u8>), StorageError> {
        self.wal.sync_all()?;
        let backup_end = self.wal.tail();
        debug_assert!(backup_end >= redo_start);
        let len = backup_end - redo_start;
        // Cap the shipped range so the restored ring leaves the reserve free:
        // restore's undo pass appends its own compensation records into that
        // reserve. Because head is pinned at redo_start for the whole backup,
        // a rollback storm can push the tail into the reserve (forward appends
        // stop short of it, but undo CLRs use it), which would otherwise yield
        // a backup that fails only at restore time. Fail cleanly here instead.
        let max_len = self.layout.wal_size.saturating_sub(self.wal.reserve());
        if len > max_len {
            return Err(StorageError::WalFull(
                "backup log range fills the WAL reserve (ring under pressure); \
                 checkpoint and retry the backup"
                    .to_string(),
            ));
        }
        let out = self.read_ring_range(redo_start, backup_end)?;
        Ok((backup_end, out))
    }

    /// Phase 1 of `BACKUP LOG`: under the storage lock, capture the log range
    /// `[last_log_backup_lsn, tail)` and its bytes plus the archive header. Does
    /// NOT advance the marker or hold, so the old marker keeps pinning the range
    /// while the caller writes the archive with the lock released. Returns
    /// `(header, start, end, log_bytes)`. Requires the FULL recovery model.
    pub(super) fn begin_log_backup(
        &mut self,
        checksum: bool,
        copy_only: bool,
    ) -> Result<(crate::backup::BackupHeader, u64, u64, Vec<u8>), StorageError> {
        self.ensure_rel_usable()?;
        if !self.version.recovery_full {
            return Err(StorageError::InvalidConfig(
                "BACKUP LOG requires the FULL recovery model".to_string(),
            ));
        }
        if self.active_sb().is_standby() {
            return Err(StorageError::InvalidConfig(
                "BACKUP LOG is not supported on a replication standby (its log chain \
                 belongs to the primary); run log backups there"
                    .to_string(),
            ));
        }
        if self.log_backup_in_progress {
            return Err(StorageError::BackupInProgress);
        }
        self.wal.sync_all()?;
        let start = self.last_log_backup_lsn;
        let end = self.wal.tail();
        debug_assert!(end >= start);
        let log = self.read_ring_range(start, end)?;
        // Reserve the single-flight slot only after the fallible reads above, so
        // a failure never strands the guard.
        self.log_backup_in_progress = true;
        // A log-only archive: no page/region data — the header carries the range
        // start in `redo_start_lsn` and the `log_backup` flag; the end is derived
        // from the LogChunk length on read.
        let header = crate::backup::BackupHeader {
            format_version: crate::backup::FORMAT_VERSION,
            page_size: PAGE_SIZE as u32,
            total_size: 0,
            wal_size: self.layout.wal_size,
            data_size: 0,
            metadata_size: 0,
            allocator_size: 0,
            snapshot_size: 0,
            reserved_size: 0,
            default_collation: None,
            redo_start_lsn: start,
            metadata_root: 0,
            last_committed_seq: 0,
            db_options: self.version.options_byte(),
            epoch: self.active_sb().epoch(),
            finished_at_millis: now_millis(),
            flags: crate::backup::BackupFlags {
                checksum,
                copy_only,
                log_backup: true,
            },
        };
        Ok((header, start, end, log))
    }

    /// Phase 3 of `BACKUP LOG`: the archive at `end` is durable, so durably
    /// advance the persisted marker then the in-memory marker and hold, letting
    /// the ring reclaim `[start, end)`.
    ///
    /// ORPHANS the backup (no marker/hold change) if the FULL-model state
    /// changed during the unlocked archive write — a concurrent `ALTER DATABASE
    /// SET RECOVERY SIMPLE` released the hold (and a checkpoint then advanced
    /// the head), or a re-enable moved the marker. Re-arming the hold at `end`
    /// in those cases could sit it below the advanced head, which `set_head`
    /// forbids. The single-flight guard is released by `LogBackupGuard` on every
    /// exit path, not here.
    pub(super) fn finish_log_backup(&mut self, start: u64, end: u64) -> Result<(), StorageError> {
        if !self.version.recovery_full || self.last_log_backup_lsn != start {
            return Ok(());
        }
        self.persist_last_log_backup_lsn(end)?;
        self.last_log_backup_lsn = end;
        self.register_log_backup_hold(end);
        Ok(())
    }

    /// Releases the `BACKUP LOG` single-flight guard (idempotent). Called by
    /// [`LogBackupGuard`] on every exit — error, panic, or success.
    pub(super) fn cancel_log_backup(&mut self) {
        self.log_backup_in_progress = false;
    }

    /// Lays a run of page images back at their page numbers (restore).
    pub(super) fn restore_pages(
        &mut self,
        start_page: u64,
        count: u64,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        debug_assert_eq!(bytes.len(), count as usize * PAGE_SIZE);
        for i in 0..count as usize {
            let page = start_page + i as u64;
            self.spill_write_page(page, &bytes[i * PAGE_SIZE..(i + 1) * PAGE_SIZE])?;
        }
        Ok(())
    }

    /// Rebuilds and persists the allocation bitmap from the shipped run list
    /// (restore).
    pub(super) fn restore_allocator_bitmap(
        &mut self,
        runs: &[(u64, u64)],
    ) -> Result<(), StorageError> {
        let mut allocator = PageAllocator::new(self.layout.data_size);
        for &(start, count) in runs {
            allocator.mark_used(start, count);
        }
        let bitmap = allocator.persistable_bitmap();
        if bitmap.len() as u64 > self.layout.allocator_size {
            return Err(StorageError::InvalidFile(
                "restored allocator bitmap exceeds allocator region".to_string(),
            ));
        }
        self.file
            .write_all_at(self.layout.allocator_offset, &bitmap)?;
        self.allocator = allocator;
        Ok(())
    }

    /// Writes the restored superblock: the source's catalog root and options as
    /// of `redo_start`, the ring bracketed at `[redo_start, backup_end)`. Slot A
    /// is active; slot B is a valid lower-generation mirror. The search-related
    /// roots are cleared — this slice does not restore the search snapshot.
    pub(super) fn restore_superblock(
        &mut self,
        header: &crate::backup::BackupHeader,
        backup_end: u64,
        standby: bool,
    ) -> Result<(), StorageError> {
        let mut base = Superblock {
            wal_head: header.redo_start_lsn,
            wal_tail: backup_end,
            last_committed_seq: header.last_committed_seq,
            metadata_root: header.metadata_root,
            ..Superblock::default()
        };
        base.set_db_options(header.db_options);
        // Seed the log-backup floor at the restore point (`backup_end`), not 0:
        // a restored FULL-model database starts a fresh log chain here. Leaving
        // it 0 would make the on-open log-backup hold sit BELOW wal_head, which
        // `set_head` forbids (the floor can only move forward).
        base.set_last_log_backup_lsn(backup_end);
        // The restartpoint = the end of everything laid down (full backup + any
        // applied log chain / shipped WAL ranges), which is the restored tail.
        base.set_applied_lsn(backup_end);
        base.set_standby(standby);
        // A STANDBY seed carries the source's timeline verbatim (the equal-epoch
        // fence relies on it: same epoch = a guaranteed log prefix). A WRITABLE
        // restore is a NEW timeline — its history is rewound to the restore
        // point (with PITR, deliberately) and its future writes diverge from
        // the original's, so it must NOT present the original's epoch to
        // standbys that followed the original.
        base.set_epoch(if standby {
            header.epoch
        } else {
            header.epoch.saturating_add(1)
        });

        let mut a = base;
        a.generation = 1;
        a.active = SUPERBLOCK_ACTIVE_A;
        a.checksum = a.compute_checksum();

        let mut b = base;
        b.generation = 0;
        b.active = SUPERBLOCK_ACTIVE_B;
        b.checksum = b.compute_checksum();

        self.file.write_all_at(
            self.layout.superblock_a_offset,
            &a.to_le_bytes_with_checksum(),
        )?;
        self.file.write_all_at(
            self.layout.superblock_b_offset,
            &b.to_le_bytes_with_checksum(),
        )?;
        Ok(())
    }

    /// Durably advances the persisted log-backup floor in both superblocks —
    /// the copy-out-before-truncate commit point for `BACKUP LOG`.
    pub(super) fn persist_last_log_backup_lsn(&mut self, lsn: u64) -> Result<(), StorageError> {
        self.commit_superblock(|sb| sb.set_last_log_backup_lsn(lsn))
    }
}
