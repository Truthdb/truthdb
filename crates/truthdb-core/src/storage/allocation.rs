use super::*;

impl Storage {
    pub fn allocate_extent(&self, temp: bool) -> Result<u64, StorageError> {
        self.lock().allocate_extent(temp)
    }

    pub fn free_extent(&self, start_page: u64) -> Result<(), StorageError> {
        self.lock().free_extent(start_page)
    }

    /// Writes one raw page (`PAGE_SIZE` bytes) to a data-region page — used by
    /// the spill spool over temp extents. Bypasses the buffer pool and the WAL
    /// (spill pages are query-scratch, never recovered).
    pub(crate) fn spill_write_page(&self, page: u64, data: &[u8]) -> Result<(), StorageError> {
        self.lock().spill_write_page(page, data)
    }

    /// Reads one raw data-region page (`PAGE_SIZE` bytes) into `out`.
    pub(crate) fn spill_read_page(&self, page: u64, out: &mut [u8]) -> Result<(), StorageError> {
        self.lock().spill_read_page(page, out)
    }

    pub fn is_page_allocated(&self, page: u64) -> bool {
        self.lock().is_page_allocated(page)
    }

    #[cfg(test)]
    pub(crate) fn data_page_offset(&self, page: u64) -> u64 {
        self.lock().data_page_offset(page)
    }
}

impl StorageFile {
    /// Whether a data-region page is currently allocated (test/diagnostic
    /// hook).
    pub fn is_page_allocated(&self, page: u64) -> bool {
        self.allocator.is_allocated(page)
    }

    /// Test hook: the absolute file offset of a data-region page.
    #[cfg(test)]
    pub(crate) fn data_page_offset(&self, page: u64) -> u64 {
        self.layout.data_offset + page * PAGE_SIZE as u64
    }

    /// Rebuilds the live allocator: persisted bitmap, then reconciliation
    /// with the snapshot descriptors and the WAL.
    ///
    /// Order matters:
    /// 1. free the stale snapshot descriptor's extent — logically this free
    ///    belongs to the checkpoint that superseded it, which precedes every
    ///    replayed WAL record;
    /// 2. replay logged alloc/free extents (all idempotent bit operations);
    /// 3. mark the live snapshot's extent allocated last, healing the crash
    ///    window where the descriptor was written but the bitmap was not.
    pub(super) fn recover_allocator(&mut self) -> Result<(), StorageError> {
        let bitmap_len = (self.layout.data_size / PAGE_SIZE as u64).div_ceil(8) as usize;
        let mut bitmap = vec![0u8; bitmap_len];
        self.file
            .read_exact_at(self.layout.allocator_offset, &mut bitmap)?;
        self.allocator = PageAllocator::from_bitmap(bitmap, self.layout.data_size);

        let descriptors = self.read_snapshot_descriptors()?;
        let live_slot = live_descriptor_slot(&descriptors);
        for (slot, desc) in descriptors.iter().enumerate() {
            let Some(desc) = desc else { continue };
            if Some(slot) != live_slot {
                let (start, pages) = self.descriptor_page_range(desc)?;
                self.allocator.free(start, pages);
            }
        }

        let rel_records: Vec<RelRecord> = self
            .replay_cache
            .iter()
            .filter(|record| record.entry_type == WAL_ENTRY_TYPE_REL)
            .map(|record| RelRecord::decode(&record.payload))
            .collect::<Result<_, _>>()?;
        for record in rel_records {
            match record.kind {
                REL_KIND_ALLOC_EXTENT => {
                    let (start, pages) = record.decode_extent_redo()?;
                    self.allocator.mark_used(start, pages);
                }
                REL_KIND_FREE_EXTENT => {
                    let (start, pages) = record.decode_extent_redo()?;
                    self.allocator.free(start, pages);
                }
                // Transaction/page records are ARIES recovery's business
                // (recover_rel); the allocator only replays extent state.
                _ => {}
            }
        }

        if let Some(live) = live_slot.and_then(|slot| descriptors[slot]) {
            let (start, pages) = self.descriptor_page_range(&live)?;
            self.allocator.mark_used(start, pages);
        }
        Ok(())
    }

    /// Converts a snapshot descriptor's byte extent into data-region pages.
    pub(super) fn descriptor_page_range(
        &self,
        desc: &SnapshotDescriptor,
    ) -> Result<(u64, u64), StorageError> {
        let page = PAGE_SIZE as u64;
        if desc.data_offset < self.layout.data_offset
            || !desc.data_offset.is_multiple_of(page)
            || desc.data_offset + desc.data_len > self.layout.data_offset + self.layout.data_size
        {
            return Err(StorageError::InvalidFile(
                "snapshot descriptor extent outside data region".to_string(),
            ));
        }
        let start = (desc.data_offset - self.layout.data_offset) / page;
        let pages = desc.data_len.div_ceil(page);
        Ok((start, pages))
    }

    pub(super) fn allocate_extent(&mut self, temp: bool) -> Result<u64, StorageError> {
        if temp {
            // Spill scratch shares the data region and the allocator bitmap
            // with REPLICATED extents. On a standby, an extent that is free at
            // the applied LSN may already be allocated on the primary (its
            // ALLOC record still in flight) — a spool there would be clobbered
            // by the arriving redo, and the spool's raw writes would clobber
            // replicated pages. Refuse until the readable-standby slice gives
            // scratch its own storage.
            if self.active_sb().is_standby() {
                return Err(StorageError::InvalidConfig(
                    "a query on this replication standby needed spill scratch, which shares \
                     the data region the primary's stream writes into; run it on the primary \
                     or raise the memory budget (dedicated standby scratch storage is \
                     planned)"
                        .to_string(),
                ));
            }
            return self.allocator.allocate_temp_extent().ok_or_else(|| {
                StorageError::InvalidConfig("data region full: cannot allocate extent".to_string())
            });
        }
        let start = self.allocator.allocate_extent().ok_or_else(|| {
            StorageError::InvalidConfig("data region full: cannot allocate extent".to_string())
        })?;
        let record = RelRecord::alloc_extent(start, EXTENT_PAGES);
        if let Err(err) = self.append_wal_entry(
            WAL_ENTRY_TYPE_REL,
            REL_WAL_ENTRY_VERSION,
            0,
            &record.encode(),
        ) {
            self.allocator.free(start, EXTENT_PAGES);
            return Err(err);
        }
        Ok(start)
    }

    pub(super) fn free_extent(&mut self, start_page: u64) -> Result<(), StorageError> {
        // Log first, then mutate: a free whose record never became durable
        // must not leave the pages reusable in memory.
        let record = RelRecord::free_extent(start_page, EXTENT_PAGES);
        self.append_wal_entry(
            WAL_ENTRY_TYPE_REL,
            REL_WAL_ENTRY_VERSION,
            0,
            &record.encode(),
        )?;
        self.allocator.free(start_page, EXTENT_PAGES);
        Ok(())
    }

    pub(super) fn spill_write_page(&mut self, page: u64, data: &[u8]) -> Result<(), StorageError> {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        let mut frame = crate::direct_io::AlignedPageBuf::new();
        frame.as_mut_slice().copy_from_slice(data);
        let offset = self.layout.data_offset + page * PAGE_SIZE as u64;
        self.file.write_page_from(offset, &frame)?;
        Ok(())
    }

    pub(super) fn spill_read_page(
        &mut self,
        page: u64,
        out: &mut [u8],
    ) -> Result<(), StorageError> {
        debug_assert_eq!(out.len(), PAGE_SIZE);
        let mut frame = crate::direct_io::AlignedPageBuf::new();
        let offset = self.layout.data_offset + page * PAGE_SIZE as u64;
        self.file.read_page_into(offset, &mut frame)?;
        out.copy_from_slice(frame.as_slice());
        Ok(())
    }

    /// Writes `data` (zero-padded to whole pages) at a page-aligned offset
    /// using batched page writes.
    pub(super) fn write_data_pages(
        &mut self,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StorageError> {
        const BATCH_FRAMES: usize = 64;
        let mut frames: Vec<AlignedPageBuf> = Vec::with_capacity(BATCH_FRAMES);
        let mut batch_start = offset;
        let mut cursor = 0usize;
        let total_pages = data.len().div_ceil(PAGE_SIZE);
        for _ in 0..total_pages {
            let mut frame = AlignedPageBuf::new();
            let len = (data.len() - cursor).min(PAGE_SIZE);
            frame.as_mut_slice()[..len].copy_from_slice(&data[cursor..cursor + len]);
            cursor += len;
            frames.push(frame);
            if frames.len() == BATCH_FRAMES {
                let refs: Vec<&AlignedPageBuf> = frames.iter().collect();
                self.file.write_pages_from(batch_start, &refs)?;
                batch_start += (BATCH_FRAMES * PAGE_SIZE) as u64;
                frames.clear();
            }
        }
        if !frames.is_empty() {
            let refs: Vec<&AlignedPageBuf> = frames.iter().collect();
            self.file.write_pages_from(batch_start, &refs)?;
        }
        Ok(())
    }
}
