use crate::storage_layout::PAGE_SIZE;

/// Pages per allocation extent.
pub const EXTENT_PAGES: u64 = 64;

/// Bitmap page allocator over the data region.
///
/// v2 semantics: a single live instance is kept in memory while the engine
/// runs. The bitmap is persisted only at checkpoint; between checkpoints,
/// persistent alloc/free operations are WAL-logged by the storage layer and
/// replayed on recovery. Temporary extents (spills, version store) are
/// tracked separately, excluded from the persisted bitmap, and vanish on
/// restart.
pub struct PageAllocator {
    bitmap: Vec<u8>,
    total_pages: u64,
    /// Next-fit cursor: searches resume where the last allocation ended.
    cursor: u64,
    /// Live temporary extents as (start_page, num_pages).
    temp_extents: Vec<(u64, u64)>,
}

impl PageAllocator {
    pub fn new(data_region_size: u64) -> Self {
        let total_pages = data_region_size / PAGE_SIZE as u64;
        let bitmap_bytes = total_pages.div_ceil(8) as usize;
        PageAllocator {
            bitmap: vec![0u8; bitmap_bytes],
            total_pages,
            cursor: 0,
            temp_extents: Vec::new(),
        }
    }

    pub fn from_bitmap(bitmap: Vec<u8>, data_region_size: u64) -> Self {
        let total_pages = data_region_size / PAGE_SIZE as u64;
        PageAllocator {
            bitmap,
            total_pages,
            cursor: 0,
            temp_extents: Vec::new(),
        }
    }

    /// Allocates a contiguous run of `num_pages`, next-fit from the cursor.
    pub fn allocate(&mut self, num_pages: u64) -> Option<u64> {
        if num_pages == 0 || num_pages > self.total_pages {
            return None;
        }
        // Two passes: cursor -> end, then start -> cursor (runs never span
        // the wrap because page numbers restart the run at 0).
        if let Some(start) = self.scan_range(self.cursor, self.total_pages, num_pages) {
            self.take_run(start, num_pages);
            return Some(start);
        }
        if let Some(start) = self.scan_range(0, self.cursor, num_pages) {
            self.take_run(start, num_pages);
            return Some(start);
        }
        None
    }

    /// Allocates one standard extent ([`EXTENT_PAGES`] pages).
    pub fn allocate_extent(&mut self) -> Option<u64> {
        self.allocate(EXTENT_PAGES)
    }

    /// Allocates one standard extent flagged temporary: excluded from the
    /// persisted bitmap and reclaimed wholesale on restart.
    pub fn allocate_temp_extent(&mut self) -> Option<u64> {
        let start = self.allocate(EXTENT_PAGES)?;
        self.temp_extents.push((start, EXTENT_PAGES));
        Some(start)
    }

    pub fn free(&mut self, start_page: u64, num_pages: u64) {
        for p in start_page..start_page + num_pages {
            self.set_free(p);
        }
        self.temp_extents
            .retain(|(start, len)| !(*start == start_page && *len == num_pages));
    }

    /// Marks a run as allocated regardless of current state (recovery
    /// reconciliation and WAL redo).
    pub fn mark_used(&mut self, start_page: u64, num_pages: u64) {
        for p in start_page..start_page + num_pages {
            self.set_used(p);
        }
    }

    /// The bitmap to persist at checkpoint: temporary extents cleared, since
    /// they must not survive a restart.
    pub fn persistable_bitmap(&self) -> Vec<u8> {
        let mut bitmap = self.bitmap.clone();
        for (start, len) in &self.temp_extents {
            for p in *start..*start + *len {
                let byte_idx = (p / 8) as usize;
                if byte_idx < bitmap.len() {
                    bitmap[byte_idx] &= !(1 << (p % 8) as u8);
                }
            }
        }
        bitmap
    }

    pub fn is_allocated(&self, page: u64) -> bool {
        !self.is_free(page)
    }

    fn scan_range(&self, from: u64, to: u64, num_pages: u64) -> Option<u64> {
        let mut run_start = from;
        let mut run_len = 0u64;
        for page in from..to {
            if self.is_free(page) {
                if run_len == 0 {
                    run_start = page;
                }
                run_len += 1;
                if run_len == num_pages {
                    return Some(run_start);
                }
            } else {
                run_len = 0;
            }
        }
        None
    }

    fn take_run(&mut self, start: u64, num_pages: u64) {
        for p in start..start + num_pages {
            self.set_used(p);
        }
        self.cursor = start + num_pages;
        if self.cursor >= self.total_pages {
            self.cursor = 0;
        }
    }

    fn is_free(&self, page: u64) -> bool {
        let byte_idx = (page / 8) as usize;
        let bit_idx = (page % 8) as u8;
        if byte_idx >= self.bitmap.len() || page >= self.total_pages {
            return false;
        }
        (self.bitmap[byte_idx] & (1 << bit_idx)) == 0
    }

    fn set_used(&mut self, page: u64) {
        let byte_idx = (page / 8) as usize;
        let bit_idx = (page % 8) as u8;
        if byte_idx < self.bitmap.len() {
            self.bitmap[byte_idx] |= 1 << bit_idx;
        }
    }

    fn set_free(&mut self, page: u64) {
        let byte_idx = (page / 8) as usize;
        let bit_idx = (page % 8) as u8;
        if byte_idx < self.bitmap.len() {
            self.bitmap[byte_idx] &= !(1 << bit_idx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocator_with_pages(pages: u64) -> PageAllocator {
        PageAllocator::new(pages * PAGE_SIZE as u64)
    }

    #[test]
    fn next_fit_resumes_after_last_allocation() {
        let mut alloc = allocator_with_pages(1000);
        assert_eq!(alloc.allocate(10), Some(0));
        assert_eq!(alloc.allocate(10), Some(10));
        alloc.free(0, 10);
        // Next-fit keeps moving forward instead of reusing the freed hole.
        assert_eq!(alloc.allocate(10), Some(20));
        // But wraps around to find the hole once the tail is exhausted.
        assert_eq!(alloc.allocate(970), Some(30));
        assert_eq!(alloc.allocate(10), Some(0));
        assert_eq!(alloc.allocate(1), None);
    }

    #[test]
    fn extent_allocation_is_64_pages() {
        let mut alloc = allocator_with_pages(1000);
        let start = alloc.allocate_extent().expect("extent");
        assert_eq!(start, 0);
        for p in 0..EXTENT_PAGES {
            assert!(alloc.is_allocated(p));
        }
        assert!(!alloc.is_allocated(EXTENT_PAGES));
    }

    #[test]
    fn temp_extents_excluded_from_persistable_bitmap() {
        let mut alloc = allocator_with_pages(1000);
        let durable = alloc.allocate_extent().expect("durable");
        let temp = alloc.allocate_temp_extent().expect("temp");
        assert!(alloc.is_allocated(temp));

        // Restart = reload from the persisted bitmap: temp extents vanish.
        let persisted = alloc.persistable_bitmap();
        let reloaded = PageAllocator::from_bitmap(persisted, 1000 * PAGE_SIZE as u64);
        assert!(reloaded.is_allocated(durable));
        assert!(!reloaded.is_allocated(temp));
    }

    #[test]
    fn freeing_a_temp_extent_directly_drops_the_temp_tracking() {
        let mut alloc = allocator_with_pages(1000);
        let temp = alloc.allocate_temp_extent().expect("temp");
        alloc.free(temp, EXTENT_PAGES);
        assert!(!alloc.is_allocated(temp));
        // Exhaust the tail so the next allocation wraps into the freed
        // range; once reallocated durably it must no longer be masked out of
        // the persisted bitmap.
        alloc.allocate(1000 - EXTENT_PAGES).expect("fill tail");
        let durable = alloc.allocate(EXTENT_PAGES).expect("reuse");
        assert_eq!(durable, temp);
        let reloaded =
            PageAllocator::from_bitmap(alloc.persistable_bitmap(), 1000 * PAGE_SIZE as u64);
        assert!(reloaded.is_allocated(durable));
    }

    #[test]
    fn out_of_range_pages_are_never_free() {
        let alloc = allocator_with_pages(10);
        assert!(alloc.is_allocated(10));
        assert!(alloc.is_allocated(u64::MAX));
    }

    #[test]
    fn allocate_zero_or_oversized_fails() {
        let mut alloc = allocator_with_pages(10);
        assert_eq!(alloc.allocate(0), None);
        assert_eq!(alloc.allocate(11), None);
        assert_eq!(alloc.allocate(10), Some(0));
    }
}
