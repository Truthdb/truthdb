//! Buffer pool: fixed set of page frames with clock eviction.
//!
//! Steal is allowed — a dirty page may be evicted before its transaction
//! commits — with WAL-before-data as the only flush constraint: before a
//! dirty page goes to disk, the WAL must be flushed up to that page's
//! `page_lsn`. The pool enforces this through the [`PoolBackend`] it is
//! given.

use std::collections::HashMap;

use crate::direct_io::AlignedPageBuf;
use crate::relstore::page;
use crate::storage::StorageError;
use crate::storage_layout::PAGE_SIZE;

pub const DEFAULT_CAPACITY_BYTES: u64 = 64 * crate::storage_layout::MIB;

/// The backing services a buffer pool needs: page I/O over the data region
/// and the WAL flush watermark for the WAL-before-data rule.
pub trait PoolBackend {
    fn read_page(&mut self, page_no: u64, frame: &mut AlignedPageBuf) -> Result<(), StorageError>;
    fn write_page(&mut self, page_no: u64, frame: &AlignedPageBuf) -> Result<(), StorageError>;
    /// Highest LSN durably flushed to the WAL.
    fn flushed_lsn(&self) -> u64;
    /// Makes the WAL durable at least up to `lsn`.
    fn flush_wal_to(&mut self, lsn: u64) -> Result<(), StorageError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameId(usize);

struct Frame {
    buf: AlignedPageBuf,
    page_no: u64,
    pin_count: u32,
    dirty: bool,
    referenced: bool,
    valid: bool,
}

pub struct BufferPool {
    frames: Vec<Frame>,
    table: HashMap<u64, usize>,
    clock_hand: usize,
    /// Indices of frames not holding a page (never used, or vacated by a
    /// failed fetch), so misses don't scan the whole frame array.
    free: Vec<usize>,
}

impl BufferPool {
    /// Creates a pool with `capacity_bytes` of frames (default 64 MiB; at
    /// least one frame).
    pub fn new(capacity_bytes: u64) -> Self {
        let frame_count = (capacity_bytes / PAGE_SIZE as u64).max(1) as usize;
        let frames = (0..frame_count)
            .map(|_| Frame {
                buf: AlignedPageBuf::new(),
                page_no: 0,
                pin_count: 0,
                dirty: false,
                referenced: false,
                valid: false,
            })
            .collect();
        BufferPool {
            free: (0..frame_count).rev().collect(),
            frames,
            table: HashMap::new(),
            clock_hand: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.frames.len()
    }

    pub fn cached_pages(&self) -> usize {
        self.table.len()
    }

    /// Fetches and pins a page, reading it from the backend on a miss.
    /// Verifies the page checksum on read (all-zero pages — fresh
    /// allocations — are accepted).
    pub fn fetch(
        &mut self,
        page_no: u64,
        backend: &mut impl PoolBackend,
    ) -> Result<FrameId, StorageError> {
        if let Some(&index) = self.table.get(&page_no) {
            let frame = &mut self.frames[index];
            frame.pin_count += 1;
            frame.referenced = true;
            return Ok(FrameId(index));
        }

        let index = self.claim_victim(backend)?;
        if let Err(err) = self.read_and_verify(index, page_no, backend) {
            // Return the frame to the free list so a later fetch reuses it.
            self.free.push(index);
            return Err(err);
        }
        self.install(index, page_no);
        Ok(FrameId(index))
    }

    /// Reads a page into a frame and validates it: checksum plus the page
    /// header's self-identity (`page_no`), which catches misdirected
    /// reads/writes. All-zero pages (fresh allocations) are accepted.
    fn read_and_verify(
        &mut self,
        index: usize,
        page_no: u64,
        backend: &mut impl PoolBackend,
    ) -> Result<(), StorageError> {
        backend.read_page(page_no, &mut self.frames[index].buf)?;
        let content = self.frames[index].buf.as_slice();
        if page::is_zero_page(content) {
            return Ok(());
        }
        if !page::verify_checksum(content) {
            return Err(StorageError::InvalidFile(format!(
                "page {page_no} checksum mismatch"
            )));
        }
        let header = page::read_header(content);
        if header.page_no != page_no {
            return Err(StorageError::InvalidFile(format!(
                "page {page_no} identity mismatch: header claims page {}",
                header.page_no
            )));
        }
        Ok(())
    }

    /// Pins a frame for a brand-new page without reading from disk: the
    /// frame starts zeroed and dirty.
    pub fn fetch_zeroed(
        &mut self,
        page_no: u64,
        backend: &mut impl PoolBackend,
    ) -> Result<FrameId, StorageError> {
        if let Some(&index) = self.table.get(&page_no) {
            let frame = &mut self.frames[index];
            frame.pin_count += 1;
            frame.referenced = true;
            frame.dirty = true;
            frame.buf.zero();
            return Ok(FrameId(index));
        }
        let index = self.claim_victim(backend)?;
        self.frames[index].buf.zero();
        self.install(index, page_no);
        self.frames[index].dirty = true;
        Ok(FrameId(index))
    }

    pub fn page(&self, id: FrameId) -> &[u8] {
        debug_assert!(self.frames[id.0].valid && self.frames[id.0].pin_count > 0);
        self.frames[id.0].buf.as_slice()
    }

    /// Mutable access marks the frame dirty. The caller maintains
    /// `page_lsn` in the page header; the pool reads it back at flush time
    /// to enforce WAL-before-data.
    pub fn page_mut(&mut self, id: FrameId) -> &mut [u8] {
        let frame = &mut self.frames[id.0];
        debug_assert!(frame.valid && frame.pin_count > 0);
        frame.dirty = true;
        frame.buf.as_mut_slice()
    }

    pub fn unpin(&mut self, id: FrameId) {
        let frame = &mut self.frames[id.0];
        debug_assert!(frame.pin_count > 0, "unpin of unpinned frame");
        frame.pin_count = frame.pin_count.saturating_sub(1);
    }

    /// Writes every dirty page back (checkpoint support). Pinned pages are
    /// flushed too — they stay cached and pinned, just clean.
    pub fn flush_all(&mut self, backend: &mut impl PoolBackend) -> Result<(), StorageError> {
        for index in 0..self.frames.len() {
            if self.frames[index].valid && self.frames[index].dirty {
                self.write_back(index, backend)?;
            }
        }
        Ok(())
    }

    /// Whether a page is currently cached (test hook).
    #[cfg(test)]
    pub fn contains(&self, page_no: u64) -> bool {
        self.table.contains_key(&page_no)
    }

    fn install(&mut self, index: usize, page_no: u64) {
        let frame = &mut self.frames[index];
        frame.page_no = page_no;
        frame.pin_count = 1;
        frame.dirty = false;
        frame.referenced = true;
        frame.valid = true;
        self.table.insert(page_no, index);
    }

    /// Finds a frame to (re)use: a free frame if any, otherwise a clock
    /// sweep over unpinned frames, evicting the victim (flushing WAL first
    /// for dirty pages — steal). Errors when every frame is pinned.
    fn claim_victim(&mut self, backend: &mut impl PoolBackend) -> Result<usize, StorageError> {
        if let Some(index) = self.free.pop() {
            return Ok(index);
        }
        // Clock: each frame gets its reference bit cleared once before it can
        // be chosen, so two full sweeps guarantee a victim if one exists.
        let mut remaining = self.frames.len() * 2;
        while remaining > 0 {
            let index = self.clock_hand;
            self.clock_hand = (self.clock_hand + 1) % self.frames.len();
            remaining -= 1;
            let frame = &mut self.frames[index];
            if frame.pin_count > 0 {
                continue;
            }
            if frame.referenced {
                frame.referenced = false;
                continue;
            }
            if frame.dirty {
                self.write_back(index, backend)?;
            }
            self.table.remove(&self.frames[index].page_no);
            self.frames[index].valid = false;
            return Ok(index);
        }
        Err(StorageError::InvalidConfig(
            "buffer pool exhausted: all frames pinned".to_string(),
        ))
    }

    /// WAL-before-data write-back: flush the WAL to the page's LSN, stamp
    /// the page checksum, write the page. The flushed watermark counts
    /// covered bytes, so a record STARTING at the watermark is not durable
    /// yet — hence `>=`.
    fn write_back(
        &mut self,
        index: usize,
        backend: &mut impl PoolBackend,
    ) -> Result<(), StorageError> {
        let lsn = page::page_lsn(self.frames[index].buf.as_slice());
        if lsn >= backend.flushed_lsn() && lsn > 0 {
            backend.flush_wal_to(lsn)?;
        }
        page::stamp_checksum(self.frames[index].buf.as_mut_slice());
        backend.write_page(self.frames[index].page_no, &self.frames[index].buf)?;
        self.frames[index].dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relstore::page::{PAGE_HEADER_SIZE, PageHeader};

    /// In-memory backend that panics if the pool violates WAL-before-data.
    struct MockBackend {
        pages: HashMap<u64, Vec<u8>>,
        flushed_lsn: u64,
        wal_flush_calls: usize,
        reads: usize,
        writes: usize,
    }

    impl MockBackend {
        fn new() -> Self {
            MockBackend {
                pages: HashMap::new(),
                flushed_lsn: 0,
                wal_flush_calls: 0,
                reads: 0,
                writes: 0,
            }
        }
    }

    impl PoolBackend for MockBackend {
        fn read_page(
            &mut self,
            page_no: u64,
            frame: &mut AlignedPageBuf,
        ) -> Result<(), StorageError> {
            self.reads += 1;
            match self.pages.get(&page_no) {
                Some(content) => frame.as_mut_slice().copy_from_slice(content),
                None => frame.zero(),
            }
            Ok(())
        }

        fn write_page(&mut self, page_no: u64, frame: &AlignedPageBuf) -> Result<(), StorageError> {
            self.writes += 1;
            let lsn = page::page_lsn(frame.as_slice());
            assert!(
                lsn <= self.flushed_lsn,
                "WAL-before-data violated: page {page_no} with lsn {lsn} written while flushed_lsn is {}",
                self.flushed_lsn
            );
            assert!(
                page::verify_checksum(frame.as_slice()),
                "page {page_no} written without a valid checksum"
            );
            self.pages.insert(page_no, frame.as_slice().to_vec());
            Ok(())
        }

        fn flushed_lsn(&self) -> u64 {
            self.flushed_lsn
        }

        fn flush_wal_to(&mut self, lsn: u64) -> Result<(), StorageError> {
            self.wal_flush_calls += 1;
            self.flushed_lsn = self.flushed_lsn.max(lsn);
            Ok(())
        }
    }

    fn write_page_payload(pool: &mut BufferPool, id: FrameId, page_no: u64, lsn: u64, marker: u8) {
        let content = pool.page_mut(id);
        page::write_header(
            content,
            &PageHeader {
                page_lsn: lsn,
                page_no,
                ..PageHeader::default()
            },
        );
        content[PAGE_HEADER_SIZE] = marker;
    }

    /// Deterministic xorshift PRNG so the property test needs no new deps.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }

    #[test]
    fn fetch_caches_and_pins() {
        let mut backend = MockBackend::new();
        let mut pool = BufferPool::new(4 * PAGE_SIZE as u64);
        let a = pool.fetch(1, &mut backend).expect("fetch");
        assert_eq!(backend.reads, 1);
        pool.unpin(a);
        let b = pool.fetch(1, &mut backend).expect("refetch");
        assert_eq!(backend.reads, 1, "second fetch must hit the cache");
        pool.unpin(b);
    }

    #[test]
    fn pinned_pages_are_never_evicted() {
        let mut backend = MockBackend::new();
        let mut pool = BufferPool::new(2 * PAGE_SIZE as u64);
        let pinned = pool.fetch(1, &mut backend).expect("pin page 1");
        for page_no in 2..10 {
            let id = pool.fetch(page_no, &mut backend).expect("fetch");
            pool.unpin(id);
        }
        assert!(pool.contains(1), "pinned page must stay cached");
        assert_eq!(&pool.page(pinned)[..8], &[0u8; 8]);
        pool.unpin(pinned);
    }

    #[test]
    fn all_frames_pinned_errors() {
        let mut backend = MockBackend::new();
        let mut pool = BufferPool::new(2 * PAGE_SIZE as u64);
        let _a = pool.fetch(1, &mut backend).expect("a");
        let _b = pool.fetch(2, &mut backend).expect("b");
        assert!(pool.fetch(3, &mut backend).is_err());
    }

    #[test]
    fn dirty_eviction_flushes_wal_first_steal() {
        let mut backend = MockBackend::new();
        let mut pool = BufferPool::new(PAGE_SIZE as u64); // single frame
        let id = pool.fetch_zeroed(1, &mut backend).expect("new page");
        write_page_payload(&mut pool, id, 1, 500, 0xAB);
        pool.unpin(id);

        // Fetching another page forces eviction of the dirty page: the mock
        // asserts the WAL was flushed to lsn 500 before the write.
        let other = pool.fetch(2, &mut backend).expect("force eviction");
        pool.unpin(other);
        assert_eq!(backend.wal_flush_calls, 1);
        assert!(backend.flushed_lsn >= 500);
        assert_eq!(backend.pages.get(&1).unwrap()[PAGE_HEADER_SIZE], 0xAB);
    }

    #[test]
    fn checksum_mismatch_is_detected_on_fetch() {
        let mut backend = MockBackend::new();
        let mut corrupt = vec![0u8; PAGE_SIZE];
        corrupt[100] = 0xFF; // non-zero page without a valid checksum
        backend.pages.insert(7, corrupt);
        let mut pool = BufferPool::new(4 * PAGE_SIZE as u64);
        assert!(pool.fetch(7, &mut backend).is_err());
        // The failed fetch must not leak its frame: the pool still has full
        // capacity for other pages.
        for page_no in 0..4u64 {
            let id = pool.fetch(100 + page_no, &mut backend).expect("fetch");
            pool.unpin(id);
        }
    }

    #[test]
    fn page_identity_mismatch_is_detected_on_fetch() {
        let mut backend = MockBackend::new();
        // A checksummed page whose header claims a different page number
        // (misdirected write).
        let mut misdirected = vec![0u8; PAGE_SIZE];
        page::write_header(
            &mut misdirected,
            &PageHeader {
                page_no: 9,
                ..PageHeader::default()
            },
        );
        page::stamp_checksum(&mut misdirected);
        backend.pages.insert(3, misdirected);
        let mut pool = BufferPool::new(4 * PAGE_SIZE as u64);
        assert!(pool.fetch(3, &mut backend).is_err());
    }

    #[test]
    fn flush_all_persists_dirty_pages() {
        let mut backend = MockBackend::new();
        let mut pool = BufferPool::new(8 * PAGE_SIZE as u64);
        for page_no in 0..4u64 {
            let id = pool.fetch_zeroed(page_no, &mut backend).expect("new");
            write_page_payload(&mut pool, id, page_no, 100 + page_no, page_no as u8);
            pool.unpin(id);
        }
        pool.flush_all(&mut backend).expect("flush_all");
        assert_eq!(backend.writes, 4);
        for page_no in 0..4u64 {
            assert_eq!(
                backend.pages.get(&page_no).unwrap()[PAGE_HEADER_SIZE],
                page_no as u8
            );
        }
    }

    /// Property test: a random workload of fetches, writes, unpins and
    /// flushes against an oracle of expected page contents. Invariants:
    /// - a fetched page always shows the last content written to it;
    /// - WAL-before-data holds on every backend write (mock asserts);
    /// - the pool never caches more pages than its capacity.
    #[test]
    fn random_workload_matches_oracle() {
        let mut rng = Rng(0x9E3779B97F4A7C15);
        for round in 0..8 {
            let capacity_frames = 2 + (rng.next() % 6) as usize;
            let mut backend = MockBackend::new();
            let mut pool = BufferPool::new((capacity_frames * PAGE_SIZE) as u64);
            let mut oracle: HashMap<u64, u8> = HashMap::new();
            let mut next_lsn = 1u64;
            let page_universe = 1 + (rng.next() % 16);

            for _step in 0..2000 {
                let page_no = rng.next() % page_universe;
                let id = match pool.fetch(page_no, &mut backend) {
                    Ok(id) => id,
                    Err(err) => panic!("round {round}: fetch failed: {err}"),
                };
                let expected = oracle.get(&page_no).copied().unwrap_or(0);
                assert_eq!(
                    pool.page(id)[PAGE_HEADER_SIZE],
                    expected,
                    "round {round}: page {page_no} content diverged from oracle"
                );

                if rng.next() % 2 == 0 {
                    let marker = (rng.next() % 255) as u8 + 1;
                    write_page_payload(&mut pool, id, page_no, next_lsn, marker);
                    oracle.insert(page_no, marker);
                    next_lsn += 1;
                }
                pool.unpin(id);

                if rng.next() % 64 == 0 {
                    pool.flush_all(&mut backend).expect("flush_all");
                }
                assert!(pool.cached_pages() <= pool.capacity());
            }

            // Final flush: backend must now hold exactly the oracle state.
            pool.flush_all(&mut backend).expect("final flush");
            for (page_no, marker) in &oracle {
                assert_eq!(
                    backend.pages.get(page_no).unwrap()[PAGE_HEADER_SIZE],
                    *marker,
                    "round {round}: backend diverged for page {page_no}"
                );
            }
        }
    }
}
