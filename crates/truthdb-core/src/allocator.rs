use crate::storage_layout::PAGE_SIZE;

pub struct PageAllocator {
    bitmap: Vec<u8>,
    total_pages: u64,
}

impl PageAllocator {
    pub fn new(data_region_size: u64) -> Self {
        let total_pages = data_region_size / PAGE_SIZE as u64;
        let bitmap_bytes = ((total_pages + 7) / 8) as usize;
        PageAllocator {
            bitmap: vec![0u8; bitmap_bytes],
            total_pages,
        }
    }

    pub fn from_bitmap(bitmap: Vec<u8>, data_region_size: u64) -> Self {
        let total_pages = data_region_size / PAGE_SIZE as u64;
        PageAllocator {
            bitmap,
            total_pages,
        }
    }

    pub fn allocate(&mut self, num_pages: u64) -> Option<u64> {
        if num_pages == 0 || num_pages > self.total_pages {
            return None;
        }
        let mut run_start = 0u64;
        let mut run_len = 0u64;
        for page in 0..self.total_pages {
            if self.is_free(page) {
                if run_len == 0 {
                    run_start = page;
                }
                run_len += 1;
                if run_len == num_pages {
                    for p in run_start..run_start + num_pages {
                        self.set_used(p);
                    }
                    return Some(run_start);
                }
            } else {
                run_len = 0;
            }
        }
        None
    }

    pub fn free(&mut self, start_page: u64, num_pages: u64) {
        for p in start_page..start_page + num_pages {
            self.set_free(p);
        }
    }

    pub fn bitmap(&self) -> &[u8] {
        &self.bitmap
    }

    fn is_free(&self, page: u64) -> bool {
        let byte_idx = (page / 8) as usize;
        let bit_idx = (page % 8) as u8;
        if byte_idx >= self.bitmap.len() {
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
