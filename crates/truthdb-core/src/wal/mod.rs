//! WAL ring writer and recovery scan.
//!
//! v2 WAL semantics:
//! - Positions are unwrapped u64 byte offsets into the log stream (the LSN
//!   space); ring/file offsets are derived as `offset + pos % size`.
//! - Appends go through an in-memory tail-page image ([`LogBuffer`]); flushes
//!   write whole pages from memory and never read the tail back from disk.
//! - Every entry's `logical_ts` header field is stamped with the entry's own
//!   LSN. Recovery trusts the superblock's `wal_tail` only as a lower bound
//!   and scans forward past it, accepting entries while their CRCs verify and
//!   `logical_ts` equals the scan position (which also rejects stale
//!   entries from earlier ring laps).
//! - The superblock is written lazily: on checkpoint and roughly every
//!   [`SUPERBLOCK_REWRITE_INTERVAL`] bytes of appended log.

pub mod log_buffer;
pub mod records;

use crate::direct_io::{AlignedPageBuf, DirectFile};
use crate::storage::StorageError;
use crate::storage_layout::{
    MIB, PAGE_SIZE, WAL_ENTRY_FOOTER_SIZE, WAL_ENTRY_HEADER_SIZE, WalEntryFooter, WalEntryHeader,
    align_down, wal_entry_padded_len, wal_payload_crc,
};
use log_buffer::LogBuffer;

/// Rewrite the active superblock after this many appended bytes, so the
/// recovery forward scan stays short without paying a superblock write per
/// append.
pub(crate) const SUPERBLOCK_REWRITE_INTERVAL: u64 = 16 * MIB;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub entry_type: u16,
    pub entry_version: u16,
    pub seq_no: u64,
    pub logical_ts: u64,
    pub payload: Vec<u8>,
}

pub struct WalWriter {
    file: DirectFile,
    ring_offset: u64,
    ring_size: u64,
    head: u64,
    buffer: LogBuffer,
    /// Tail position up to which the log is known fsync-durable. Appends
    /// with `sync = false` advance the tail but not this watermark.
    flushed: u64,
    /// Ring bytes reserved for compensation records: forward relational
    /// appends stop at `ring_size - reserve` so rollback and recovery undo
    /// always have room to write their CLRs.
    reserve: u64,
    /// Set when a page write failed mid-append: the in-memory tail no longer
    /// matches the disk and no further appends can be trusted.
    poisoned: bool,
    /// A replication standby is read-only: it appends nothing to its own WAL
    /// (it only receives the primary's log via `apply_wal_stream`). A local
    /// write would diverge the replica and corrupt the next apply, so every
    /// append is rejected until promotion clears this.
    read_only: bool,
    bytes_since_superblock: u64,
    superblock_interval: u64,
    /// An encrypted database's: this writer's key, under which every entry it
    /// appends is sealed.
    sealer: Option<crate::encryption::WalSealer>,
}

impl WalWriter {
    /// Opens a writer positioned at a recovered `(head, tail)`. Seeds the
    /// in-memory tail page from disk (prefix only; the suffix stays zero so
    /// the next flush heals any torn bytes past the tail).
    /// With `keys` (an encrypted database), the writer draws its own sealing
    /// key.
    pub fn open(
        mut file: DirectFile,
        ring_offset: u64,
        ring_size: u64,
        head: u64,
        tail: u64,
        keys: Option<&crate::encryption::WalKeys>,
    ) -> Result<Self, StorageError> {
        let sealer = keys.map(|keys| keys.sealer()).transpose()?;
        let mut buffer = LogBuffer::new_at(tail);
        if !buffer.current_page_is_empty() {
            let (page_start, _) = buffer.current_page();
            let file_offset = ring_offset + page_start % ring_size;
            let mut disk_page = AlignedPageBuf::new();
            file.read_page_into(file_offset, &mut disk_page)?;
            buffer.seed_prefix(&disk_page);
        }
        Ok(WalWriter {
            file,
            ring_offset,
            ring_size,
            head,
            buffer,
            flushed: tail,
            reserve: ring_size / 4,
            poisoned: false,
            read_only: false,
            bytes_since_superblock: 0,
            superblock_interval: SUPERBLOCK_REWRITE_INTERVAL,
            sealer,
        })
    }

    /// Re-seats the writer at `new_tail` after a standby applied shipped log
    /// into the ring behind the writer's back (a standby never appends, so its
    /// in-memory tail otherwise lags the seeded ring — breaking anything that
    /// reads `tail()`/`flushed_lsn()`, e.g. a backup or checkpoint). The ring
    /// bytes up to `new_tail` are already durable, so this mirrors `open`: seat a
    /// fresh buffer at `new_tail` and seed its current page from disk. `new_tail`
    /// must not move the tail backward.
    pub(crate) fn resync_tail(&mut self, new_tail: u64) -> Result<(), StorageError> {
        debug_assert!(new_tail >= self.tail());
        let mut buffer = LogBuffer::new_at(new_tail);
        if !buffer.current_page_is_empty() {
            let (page_start, _) = buffer.current_page();
            let file_offset = self.ring_offset + page_start % self.ring_size;
            let mut disk_page = AlignedPageBuf::new();
            self.file.read_page_into(file_offset, &mut disk_page)?;
            buffer.seed_prefix(&disk_page);
        }
        self.buffer = buffer;
        self.flushed = new_tail;
        Ok(())
    }

    /// Test hook: shrink the lazy-superblock cadence so it can be exercised
    /// without appending tens of MiB.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_superblock_interval(&mut self, bytes: u64) {
        self.superblock_interval = bytes;
    }

    /// Marks the writer read-only (a replication standby) or writable again (at
    /// promotion). While read-only every append is rejected.
    pub(crate) fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn head(&self) -> u64 {
        self.head
    }

    pub fn tail(&self) -> u64 {
        self.buffer.tail()
    }

    /// Advances the head (checkpoint reclamation).
    pub fn set_head(&mut self, head: u64) {
        debug_assert!(head >= self.head && head <= self.tail());
        self.head = head;
    }

    pub fn usage_ratio(&self) -> f64 {
        if self.ring_size == 0 {
            return 1.0;
        }
        let used = self.tail().saturating_sub(self.head);
        used as f64 / self.ring_size as f64
    }

    /// The log's dedicated file handle (also used for lazy superblock writes
    /// so they ride the log path rather than serializing behind data flushes).
    pub fn file_mut(&mut self) -> &mut DirectFile {
        &mut self.file
    }

    /// Duplicates the log file descriptor as a plain [`std::fs::File`], for the
    /// group-commit log-writer to fsync off the storage lock.
    pub fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
        self.file.try_clone_std()
    }

    /// True once enough bytes have been appended since the last superblock
    /// write; calling this resets the counter.
    pub fn take_superblock_due(&mut self) -> bool {
        if self.bytes_since_superblock >= self.superblock_interval {
            self.bytes_since_superblock = 0;
            true
        } else {
            false
        }
    }

    /// The tail position up to which the log is fsync-durable.
    pub fn flushed_lsn(&self) -> u64 {
        self.flushed
    }

    /// Ring bytes reserved for compensation records (rollback and recovery
    /// undo). Forward appends stop `reserve` bytes short of a full ring so undo
    /// always has room; a backup uses this to keep the shipped log range small
    /// enough that a restore's undo pass still fits.
    pub fn reserve(&self) -> u64 {
        self.reserve
    }

    /// Makes the log durable at least up to and including the record that
    /// STARTS at `lsn` (page writes always happen at append time; this only
    /// issues the missing fsync). `flushed` counts covered bytes, so a
    /// record starting exactly at the watermark is not yet durable.
    pub fn sync_to(&mut self, lsn: u64) -> Result<(), StorageError> {
        if lsn >= self.flushed {
            self.sync_all()?;
        }
        Ok(())
    }

    /// Fsyncs everything appended so far.
    pub fn sync_all(&mut self) -> Result<(), StorageError> {
        if self.flushed < self.tail() {
            self.file.sync_data()?;
            self.flushed = self.tail();
        }
        Ok(())
    }

    /// Appends one entry, makes it durable (whole-page flush + fsync) and
    /// returns its LSN.
    pub fn append_entry(
        &mut self,
        entry_type: u16,
        entry_version: u16,
        seq_no: u64,
        payload: &[u8],
    ) -> Result<u64, StorageError> {
        self.append_entry_opts(entry_type, entry_version, seq_no, payload, true)
    }

    /// Appends one entry; page images are written immediately, but the fsync
    /// is skipped when `sync` is false (force-at-commit: the caller fsyncs
    /// via [`WalWriter::sync_all`] before acknowledging anything).
    pub fn append_entry_opts(
        &mut self,
        entry_type: u16,
        entry_version: u16,
        seq_no: u64,
        payload: &[u8],
        sync: bool,
    ) -> Result<u64, StorageError> {
        self.append_entry_reserve(entry_type, entry_version, seq_no, payload, sync, true)
    }

    /// Like [`WalWriter::append_entry_opts`], but with explicit access to the
    /// compensation reserve: forward relational appends pass
    /// `allow_reserve = false` so rollback/recovery CLRs always have room.
    pub fn append_entry_reserve(
        &mut self,
        entry_type: u16,
        entry_version: u16,
        seq_no: u64,
        payload: &[u8],
        sync: bool,
        allow_reserve: bool,
    ) -> Result<u64, StorageError> {
        if self.read_only {
            return Err(StorageError::InvalidConfig(
                "write rejected: this database is a replication standby (read-only until promotion)"
                    .to_string(),
            ));
        }
        if self.poisoned {
            return Err(StorageError::InvalidFile(
                "wal writer disabled after a failed page write; restart to recover".to_string(),
            ));
        }
        if self.ring_size == 0 {
            return Err(StorageError::InvalidFile(
                "wal region size is zero".to_string(),
            ));
        }
        let stored_len = payload.len()
            + if self.sealer.is_some() {
                crate::encryption::WAL_SEAL_OVERHEAD
            } else {
                0
            };
        let entry_len = wal_entry_padded_len(stored_len);
        if entry_len as u64 > self.ring_size {
            return Err(StorageError::WalFull(
                "entry larger than wal ring".to_string(),
            ));
        }

        let tail = self.tail();
        let bytes_to_lap_end = self.ring_size - (tail % self.ring_size);
        let gap = if (entry_len as u64) > bytes_to_lap_end {
            bytes_to_lap_end
        } else {
            0
        };
        // Free-space guard over the *flushed* range, not just the entry: the
        // tail page is always written as a whole-page image, so its zero
        // suffix beyond the new tail must also fit inside the ring window
        // [head, head + ring_size) — otherwise the suffix would alias onto
        // (and zero out) the oldest live entries at the head.
        let new_tail = tail + gap + entry_len as u64;
        let page = PAGE_SIZE as u64;
        let flush_end = if new_tail.is_multiple_of(page) {
            new_tail
        } else {
            align_down(new_tail, page) + page
        };
        let capacity = if allow_reserve {
            self.ring_size
        } else {
            self.ring_size - self.reserve
        };
        if flush_end.saturating_sub(self.head) > capacity {
            return Err(StorageError::WalFull("wal ring full".to_string()));
        }

        let mut completed = Vec::new();
        if gap > 0 {
            completed.extend(self.buffer.skip_zero_to(tail + gap));
        }
        let lsn = self.buffer.tail();

        // A sealed entry authenticates its header's leading fields.
        let sealed;
        let (entry_version, stored) = match &self.sealer {
            Some(sealer) => {
                let version = entry_version | crate::storage_layout::WAL_ENTRY_SEALED;
                let aad = WalEntryHeader::new(entry_type, version, stored_len as u32, seq_no, lsn, 0)
                    .to_le_bytes();
                sealed = sealer.seal_entry(
                    lsn,
                    &aad[..crate::storage_layout::WAL_ENTRY_AAD_LEN],
                    payload,
                );
                (version, sealed.as_slice())
            }
            None => (entry_version, payload),
        };
        let payload_crc = wal_payload_crc(stored);
        let header = WalEntryHeader::new(
            entry_type,
            entry_version,
            stored.len() as u32,
            seq_no,
            lsn,
            payload_crc,
        );
        let footer = WalEntryFooter {
            payload_len: stored.len() as u32,
            payload_crc,
        };
        let mut entry_bytes = Vec::with_capacity(entry_len);
        entry_bytes.extend_from_slice(&header.to_le_bytes());
        entry_bytes.extend_from_slice(stored);
        entry_bytes.extend_from_slice(&footer.to_le_bytes());
        entry_bytes.resize(entry_len, 0);

        completed.extend(self.buffer.append(&entry_bytes));
        // A failed page write leaves the in-memory tail ahead of the disk:
        // nothing appended afterwards could be trusted, so poison the writer
        // (restart recovers to the last durable prefix).
        if let Err(err) = self.write_pages(&completed) {
            self.poisoned = true;
            return Err(err);
        }
        if !self.buffer.current_page_is_empty() {
            let (page_start, page) = self.buffer.current_page();
            let file_offset = self.ring_offset + page_start % self.ring_size;
            if let Err(err) = self.file.write_page_from(file_offset, page) {
                self.poisoned = true;
                return Err(err.into());
            }
        }
        if sync {
            self.file.sync_data()?;
            self.flushed = self.tail();
        }

        self.bytes_since_superblock += gap + entry_len as u64;
        Ok(lsn)
    }

    /// Writes completed page images, batching runs that are contiguous in
    /// file space (runs break at ring-lap boundaries).
    fn write_pages(&mut self, pages: &[(u64, AlignedPageBuf)]) -> Result<(), StorageError> {
        let mut index = 0;
        while index < pages.len() {
            let run_start = index;
            // Pages are consecutive in unwrapped space; a run breaks where the
            // next page wraps to the start of the ring.
            while index + 1 < pages.len() && !pages[index + 1].0.is_multiple_of(self.ring_size) {
                index += 1;
            }
            let run = &pages[run_start..=index];
            let frames: Vec<&AlignedPageBuf> = run.iter().map(|(_, page)| page).collect();
            let file_offset = self.ring_offset + run[0].0 % self.ring_size;
            self.file.write_pages_from(file_offset, &frames)?;
            index += 1;
        }
        Ok(())
    }
}

pub(crate) struct ScanResult {
    pub records: Vec<WalRecord>,
    /// Discovered end of the valid log (unwrapped).
    pub tail: u64,
}

/// Zeroes the ring bytes `[from, to)` (logical LSNs, lap-aware), so a
/// forward scan stops at `from`: an all-zero header reads as a wrap gap, and
/// the bytes after it are zero too (on every lap the range touches), so the
/// scan ends at `from`. Point-in-time restore cuts the log this way.
pub(crate) fn zero_ring_range(
    file: &mut DirectFile,
    ring_offset: u64,
    ring_size: u64,
    from: u64,
    to: u64,
) -> Result<(), StorageError> {
    debug_assert!(ring_size.is_multiple_of(PAGE_SIZE as u64));
    let mut page_start = from - from % PAGE_SIZE as u64;
    while page_start < to {
        let file_offset = ring_offset + page_start % ring_size;
        let mut page = AlignedPageBuf::new();
        file.read_page_into(file_offset, &mut page)?;
        let zero_from = from.saturating_sub(page_start) as usize;
        let zero_to = (to - page_start).min(PAGE_SIZE as u64) as usize;
        page.as_mut_slice()[zero_from..zero_to].fill(0);
        file.write_page_from(file_offset, &page)?;
        page_start += PAGE_SIZE as u64;
    }
    Ok(())
}

/// Scans the ring from `head`, collecting valid entries.
///
/// Entries starting before `trusted_tail` (the superblock's recorded tail)
/// are validated by CRC alone. Past it, the scan continues while entries
/// verify **and** carry `logical_ts == position`, which both detects torn
/// tails and rejects stale entries from earlier ring laps. A run of zero
/// bytes is interpreted as a ring-wrap gap: the scan skips to the next lap
/// and continues if a valid entry sits there, otherwise the log ends before
/// the gap.
///
/// Residual hazard (accepted): if media corruption destroys an entry inside
/// the trusted region, the log is truncated there and new entries overwrite
/// the old suffix. An old-history entry that sits at exactly the position
/// the new history reaches passes the LSN self-identity check and would be
/// resurrected — distinguishing histories at the same position would need
/// timeline identifiers (out of scope for Stage 1; this only follows an
/// already-violated durability assumption).
///
/// With `keys` (an encrypted database) every entry must be sealed and
/// open under them, and its payload comes back opened (the version without
/// the sealed bit); without, a sealed entry is not accepted. An entry that
/// fails either way ends the log, like one that fails its checksum.
pub(crate) fn scan_ring(
    file: &mut DirectFile,
    ring_offset: u64,
    ring_size: u64,
    head: u64,
    trusted_tail: u64,
    keys: Option<&crate::encryption::WalKeys>,
) -> Result<ScanResult, StorageError> {
    let mut records = Vec::new();
    let mut cursor = head;
    let hard_end = head.saturating_add(ring_size);
    // When the scan skips a wrap gap beyond the trusted region, the gap only
    // counts if a valid entry follows; otherwise the log ends where the gap
    // began.
    let mut pending_gap_start: Option<u64> = None;

    if ring_size == 0 {
        return Ok(ScanResult {
            records,
            tail: head,
        });
    }

    loop {
        if cursor >= hard_end {
            break;
        }
        let ring_pos = cursor % ring_size;
        let bytes_to_lap_end = ring_size - ring_pos;
        let trusted = cursor < trusted_tail;

        if bytes_to_lap_end < WAL_ENTRY_HEADER_SIZE as u64 {
            // Too small for a header: implicit wrap gap.
            if !trusted {
                pending_gap_start.get_or_insert(cursor);
            }
            cursor += bytes_to_lap_end;
            continue;
        }

        let file_pos = ring_offset + ring_pos;
        let mut header_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
        file.read_exact_at(file_pos, &mut header_bytes)?;
        if header_bytes.iter().all(|b| *b == 0) {
            // Wrap gap: writer zero-fills to the lap end before wrapping.
            if !trusted {
                pending_gap_start.get_or_insert(cursor);
            }
            cursor += bytes_to_lap_end;
            continue;
        }

        let header = WalEntryHeader::from_le_bytes(&header_bytes);
        if !header.verify_header_crc() {
            break;
        }
        let payload_len = header.payload_len as usize;
        let entry_len = wal_entry_padded_len(payload_len) as u64;
        let total_len = (WAL_ENTRY_HEADER_SIZE + payload_len + WAL_ENTRY_FOOTER_SIZE) as u64;
        if total_len > bytes_to_lap_end {
            // Entries never straddle a lap boundary.
            break;
        }
        if !trusted && header.logical_ts != cursor {
            break;
        }

        let mut payload = vec![0u8; payload_len];
        file.read_exact_at(file_pos + WAL_ENTRY_HEADER_SIZE as u64, &mut payload)?;
        if wal_payload_crc(&payload) != header.payload_crc {
            break;
        }
        let mut footer_bytes = [0u8; WAL_ENTRY_FOOTER_SIZE];
        file.read_exact_at(
            file_pos + (WAL_ENTRY_HEADER_SIZE + payload_len) as u64,
            &mut footer_bytes,
        )?;
        let footer = WalEntryFooter::from_le_bytes(&footer_bytes);
        if footer.payload_len != header.payload_len || footer.payload_crc != header.payload_crc {
            break;
        }
        let sealed = header.entry_version & crate::storage_layout::WAL_ENTRY_SEALED != 0;
        let payload = match keys {
            Some(keys) if sealed => match keys.open_entry(
                header.logical_ts,
                &header_bytes[..crate::storage_layout::WAL_ENTRY_AAD_LEN],
                &payload,
            ) {
                Some(plain) => plain,
                None => break,
            },
            None if !sealed => payload,
            _ => break,
        };

        records.push(WalRecord {
            entry_type: header.entry_type,
            entry_version: header.entry_version & !crate::storage_layout::WAL_ENTRY_SEALED,
            seq_no: header.seq_no,
            logical_ts: header.logical_ts,
            payload,
        });
        pending_gap_start = None;
        cursor += entry_len;
    }

    let tail = pending_gap_start.unwrap_or(cursor);
    Ok(ScanResult { records, tail })
}
