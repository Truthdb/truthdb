//! In-memory tail-page log buffer.
//!
//! The WAL tail page is kept as a whole-page image in memory. Appends copy
//! into the image; flushing always writes whole pages from memory and never
//! reads the tail page back from disk. This fixes the torn-tail
//! read-modify-write hazard of the v1 WAL: bytes that were already fsynced on
//! the shared tail page are always rewritten from the same in-memory image,
//! so (under sector-atomic writes) a torn page rewrite cannot destroy them,
//! and latent on-disk corruption beyond the tail is healed rather than
//! immortalized.
//!
//! Positions here are *unwrapped* log positions (monotonic u64 byte offsets,
//! the LSN space). Mapping to ring/file offsets is the caller's job. The ring
//! size is a page multiple, so a page never straddles a ring wrap.

use crate::direct_io::AlignedPageBuf;
use crate::storage_layout::PAGE_SIZE;

pub(crate) struct LogBuffer {
    /// Unwrapped position of the first byte of `tail_page`.
    page_start: u64,
    /// Unwrapped position of the next byte to append (the log tail).
    tail: u64,
    /// Image of the page containing `tail`; zeroed beyond `tail`.
    tail_page: AlignedPageBuf,
}

impl LogBuffer {
    /// Creates a buffer positioned at `tail`. The image starts zeroed; if
    /// `tail` is mid-page the caller must seed the on-disk prefix with
    /// [`LogBuffer::seed_prefix`].
    pub fn new_at(tail: u64) -> Self {
        let page_size = PAGE_SIZE as u64;
        LogBuffer {
            page_start: tail - (tail % page_size),
            tail,
            tail_page: AlignedPageBuf::new(),
        }
    }

    /// Seeds the image bytes in `[page_start, tail)` from an on-disk copy of
    /// the tail page. Bytes at and beyond `tail` remain zero regardless of
    /// what the disk holds, so the next flush heals any torn suffix.
    pub fn seed_prefix(&mut self, disk_page: &AlignedPageBuf) {
        let prefix_len = (self.tail - self.page_start) as usize;
        self.tail_page.as_mut_slice()[..prefix_len]
            .copy_from_slice(&disk_page.as_slice()[..prefix_len]);
    }

    pub fn tail(&self) -> u64 {
        self.tail
    }

    /// Appends `bytes` at the tail. Returns completed whole-page images
    /// (unwrapped page position, image); the still-partial tail page is kept
    /// internal and available via [`LogBuffer::current_page`].
    pub fn append(&mut self, bytes: &[u8]) -> Vec<(u64, AlignedPageBuf)> {
        let mut completed = Vec::new();
        let mut written = 0usize;
        while written < bytes.len() {
            let offset_in_page = (self.tail - self.page_start) as usize;
            let space = PAGE_SIZE - offset_in_page;
            let copy_len = space.min(bytes.len() - written);
            self.tail_page.as_mut_slice()[offset_in_page..offset_in_page + copy_len]
                .copy_from_slice(&bytes[written..written + copy_len]);
            self.tail += copy_len as u64;
            written += copy_len;
            if self.tail - self.page_start == PAGE_SIZE as u64 {
                self.complete_page(&mut completed);
            }
        }
        completed
    }

    /// Advances the tail to `new_tail` with zero bytes (ring-wrap gap fill).
    /// Returns completed whole-page images exactly like [`LogBuffer::append`];
    /// the skipped range is all zeros in the images.
    pub fn skip_zero_to(&mut self, new_tail: u64) -> Vec<(u64, AlignedPageBuf)> {
        debug_assert!(new_tail >= self.tail);
        let mut completed = Vec::new();
        while self.tail < new_tail {
            let offset_in_page = self.tail - self.page_start;
            let space = PAGE_SIZE as u64 - offset_in_page;
            let advance = space.min(new_tail - self.tail);
            // Image bytes beyond the previous tail are already zero.
            self.tail += advance;
            if self.tail - self.page_start == PAGE_SIZE as u64 {
                self.complete_page(&mut completed);
            }
        }
        completed
    }

    /// The current (possibly partial) tail page: unwrapped page position and
    /// whole-page image, zero-padded beyond the tail.
    pub fn current_page(&self) -> (u64, &AlignedPageBuf) {
        (self.page_start, &self.tail_page)
    }

    /// True when the tail sits exactly on a page boundary, i.e. the current
    /// page image holds no bytes and does not need to be flushed.
    pub fn current_page_is_empty(&self) -> bool {
        self.tail == self.page_start
    }

    fn complete_page(&mut self, completed: &mut Vec<(u64, AlignedPageBuf)>) {
        let full = std::mem::take(&mut self.tail_page);
        completed.push((self.page_start, full));
        self.page_start += PAGE_SIZE as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn concat(buffer: &mut LogBuffer, pages: &mut Vec<(u64, AlignedPageBuf)>, bytes: &[u8]) {
        pages.extend(buffer.append(bytes));
    }

    fn reconstruct(pages: &[(u64, AlignedPageBuf)], buffer: &LogBuffer, up_to: u64) -> Vec<u8> {
        let mut out = vec![0u8; up_to as usize];
        for (pos, page) in pages {
            let start = *pos as usize;
            let end = (start + PAGE_SIZE).min(out.len());
            if start < out.len() {
                out[start..end].copy_from_slice(&page.as_slice()[..end - start]);
            }
        }
        let (pos, page) = buffer.current_page();
        let start = pos as usize;
        if start < out.len() {
            let end = (start + PAGE_SIZE).min(out.len());
            out[start..end].copy_from_slice(&page.as_slice()[..end - start]);
        }
        out
    }

    #[test]
    fn append_within_one_page_completes_nothing() {
        let mut buffer = LogBuffer::new_at(0);
        let pages = buffer.append(&[7u8; 100]);
        assert!(pages.is_empty());
        assert_eq!(buffer.tail(), 100);
        assert_eq!(&buffer.current_page().1.as_slice()[..100], &[7u8; 100]);
        assert!(
            buffer.current_page().1.as_slice()[100..]
                .iter()
                .all(|b| *b == 0)
        );
    }

    #[test]
    fn append_across_pages_emits_full_page_images() {
        let mut buffer = LogBuffer::new_at(0);
        let mut pages = Vec::new();
        let data: Vec<u8> = (0..PAGE_SIZE + 100).map(|i| (i % 251) as u8).collect();
        concat(&mut buffer, &mut pages, &data);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].0, 0);
        assert_eq!(buffer.tail(), (PAGE_SIZE + 100) as u64);
        let reconstructed = reconstruct(&pages, &buffer, buffer.tail());
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn exact_page_fill_leaves_empty_current_page() {
        let mut buffer = LogBuffer::new_at(0);
        let pages = buffer.append(&vec![1u8; PAGE_SIZE]);
        assert_eq!(pages.len(), 1);
        assert!(buffer.current_page_is_empty());
        assert_eq!(buffer.current_page().0, PAGE_SIZE as u64);
    }

    #[test]
    fn skip_zero_advances_and_zero_fills() {
        let mut buffer = LogBuffer::new_at(0);
        let mut pages = buffer.append(&[9u8; 10]);
        pages.extend(buffer.skip_zero_to(2 * PAGE_SIZE as u64));
        assert_eq!(pages.len(), 2);
        assert!(buffer.current_page_is_empty());
        let reconstructed = reconstruct(&pages, &buffer, 2 * PAGE_SIZE as u64);
        assert_eq!(&reconstructed[..10], &[9u8; 10]);
        assert!(reconstructed[10..].iter().all(|b| *b == 0));
    }

    #[test]
    fn seed_prefix_keeps_suffix_zero() {
        let mut disk = AlignedPageBuf::new();
        disk.as_mut_slice().fill(0xAB);
        let mut buffer = LogBuffer::new_at(100);
        buffer.seed_prefix(&disk);
        let (pos, page) = buffer.current_page();
        assert_eq!(pos, 0);
        assert!(page.as_slice()[..100].iter().all(|b| *b == 0xAB));
        assert!(page.as_slice()[100..].iter().all(|b| *b == 0));
    }

    #[test]
    fn mid_page_start_positions_correctly() {
        let start = PAGE_SIZE as u64 * 3 + 500;
        let mut buffer = LogBuffer::new_at(start);
        assert_eq!(buffer.current_page().0, PAGE_SIZE as u64 * 3);
        let pages = buffer.append(&[5u8; 4000]);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].0, PAGE_SIZE as u64 * 3);
        assert_eq!(buffer.tail(), start + 4000);
    }
}
