//! `encryption enable`: turning encryption on for a database that holds
//! data, while it runs.
//!
//! The switch writes a key block in state Converting (the header copy
//! first), then seals every page and log entry written from then on, while
//! plain ones are still read. A pass in bounded steps then rewrites every
//! page still plain on disk: through the pool (a logged image, sealed when
//! flushed) or, for a blob page, through a BLOB_REWRITE record. When a whole
//! sweep finds nothing plain left, the conversion finishes: the snapshot is
//! sealed (a checkpoint after the switch rewrites it), the log is sealed from
//! its head on, free pages and the ring outside the live log are zeroed (old
//! plaintext), and the key block's state becomes Encrypted, after which plain
//! pages and entries are refused.
//!
//! A restart resumes it (the key block says Converting). Standbys are not
//! converted: the key fingerprint changes, so they are refused and reseeded;
//! a log chain does not cross the switch.

use super::*;

/// The state of a conversion under way.
#[derive(Debug, Clone)]
pub(crate) struct Conversion {
    /// The next data page the sweep looks at.
    cursor: u64,
    /// The sweep rewrote a page since it last started.
    rewrote: bool,
    /// Every log entry from this LSN on is sealed.
    sealed_from: u64,
    /// The completion's free-page zeroing has reached this page (`None`: the
    /// sweep is still running).
    zeroing: Option<u64>,
}

impl Conversion {
    pub(crate) fn new(sealed_from: u64) -> Self {
        Conversion {
            cursor: 0,
            rewrote: false,
            sealed_from,
            zeroing: None,
        }
    }
}

/// What one bounded step of the conversion reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionStep {
    /// No conversion is under way.
    Idle,
    /// More to do.
    Working,
    /// Pages were rewritten through the pool (or the snapshot, or the log's
    /// head, is not sealed yet): a checkpoint must run before it can finish.
    NeedsCheckpoint,
    /// The conversion finished: the database is encrypted.
    Done,
}

impl Storage {
    /// `encryption enable`: turns encryption on for this (plain) database,
    /// under the configured master key. Every page and log entry written from
    /// now on is sealed; [`Self::conversion_step`] rewrites the rest. The new
    /// key is made (and wrapped, which may run a key command) without the
    /// storage lock.
    pub fn enable_encryption(&self) -> Result<(), StorageError> {
        let (header, source) = self.lock().header_to_enable()?;
        let file_uuid = crate::encryption::random_uuid()?;
        let (block, dek) = crate::encryption::new_converting_key_block(&source, &file_uuid)?;
        self.lock()
            .switch_to_encrypted(&header, file_uuid, &block, &dek)
    }

    /// One bounded step (at most `budget` pages read) of the conversion under
    /// way, under the storage lock.
    pub fn conversion_step(&self, budget: usize) -> Result<ConversionStep, StorageError> {
        self.lock().conversion_step(budget)
    }

    /// How far the conversion under way is, in percent (`None`: none).
    pub fn conversion_percent(&self) -> Option<u32> {
        let locked = self.lock();
        let conversion = locked.conversion.as_ref()?;
        let pages = (locked.layout.data_size / PAGE_SIZE as u64).max(1);
        let done = match conversion.zeroing {
            Some(_) => pages,
            None => conversion.cursor,
        };
        Some((done * 100 / pages).min(99) as u32)
    }
}

impl StorageFile {
    /// The header of this plain database and the master key source, for
    /// `encryption enable`.
    fn header_to_enable(
        &mut self,
    ) -> Result<(FileHeader, crate::encryption::KeySource), StorageError> {
        if self.active_sb().is_standby() {
            return Err(StorageError::InvalidConfig(
                "a standby is not converted: enable encryption on the primary, then reseed the \
                 standby from a backup taken after it"
                    .to_string(),
            ));
        }
        let header = self.header_on_disk()?;
        if header.is_encrypted() {
            return Err(StorageError::InvalidConfig(
                "this database is already encrypted".to_string(),
            ));
        }
        if header.reserved_size < PAGE_SIZE as u64 {
            return Err(StorageError::InvalidConfig(
                "an encrypted database needs a reserved region of at least one page for the copy \
                 of its header; this file has none"
                    .to_string(),
            ));
        }
        let source = self.key_source.clone().ok_or_else(|| {
            StorageError::InvalidConfig(
                "no master key is configured: set [encryption] and restart first".to_string(),
            )
        })?;
        Ok((header, source))
    }

    /// Writes the key block (Converting) and switches to sealing: the header
    /// copy first, then the header (a crash before the header leaves the
    /// plain database as it was), then pages and log entries written from
    /// now on are sealed.
    fn switch_to_encrypted(
        &mut self,
        before: &FileHeader,
        file_uuid: [u8; 16],
        block: &[u8],
        dek: &[u8; 32],
    ) -> Result<(), StorageError> {
        let mut header = self.header_on_disk()?;
        if header.to_le_bytes_with_checksum() != before.to_le_bytes_with_checksum() {
            return Err(StorageError::InvalidConfig(
                "the file header changed while the key was made (another `encryption enable` \
                 ran): retry"
                    .to_string(),
            ));
        }
        header.file_uuid = file_uuid;
        header.set_key_block(block);
        header.header_checksum = header.compute_checksum();
        let out = header.to_le_bytes_with_checksum();
        let end = header.reserved_offset + header.reserved_size;
        self.file.write_all_at(end - PAGE_SIZE as u64, &out)?;
        self.file.sync_data()?;
        self.file.write_all_at(0, &out)?;
        self.file.sync_data()?;

        let keys = crate::encryption::PageKeys::new(dek, &file_uuid, true)?;
        self.codec = crate::encryption::PageCodec::Encrypted(Box::new(keys));
        let wal_keys = self.codec.wal_keys().expect("an encrypted codec");
        self.wal.start_sealing(&wal_keys)?;
        self.key_record = Some(crate::backup::KeyRecord {
            file_uuid,
            key_block: block.to_vec(),
            fingerprint: wal_keys.fingerprint(),
        });
        self.conversion = Some(Conversion::new(self.wal.tail()));
        Ok(())
    }

    fn conversion_step(&mut self, budget: usize) -> Result<ConversionStep, StorageError> {
        // A standby writes nothing (it was refused the switch; one restored
        // from a backup taken mid-conversion follows its primary instead).
        let Some(mut conversion) = self.conversion.clone() else {
            return Ok(ConversionStep::Idle);
        };
        if self.active_sb().is_standby() {
            return Ok(ConversionStep::Idle);
        }
        let step = match conversion.zeroing {
            None => self.sweep(&mut conversion, budget)?,
            Some(_) => self.zero_free_pages(&mut conversion, budget)?,
        };
        self.conversion = match step {
            ConversionStep::Done => None,
            _ => Some(conversion),
        };
        Ok(step)
    }

    /// Rewrites up to `budget` pages still plain on disk.
    fn sweep(
        &mut self,
        conversion: &mut Conversion,
        budget: usize,
    ) -> Result<ConversionStep, StorageError> {
        let data_pages = self.layout.data_size / PAGE_SIZE as u64;
        // The live search snapshot is not page-formatted (a checkpoint
        // rewrites it sealed).
        let snapshot = match self.load_active_snapshot_descriptor()? {
            Some(desc) => Some(self.descriptor_page_range(&desc)?),
            None => None,
        };
        let mut page = vec![0u8; PAGE_SIZE];
        // The budget bounds the pages read under one lock hold.
        let mut read = 0;
        while conversion.cursor < data_pages && read < budget {
            let page_no = conversion.cursor;
            conversion.cursor += 1;
            if !self.allocator.is_allocated(page_no)
                || self.allocator.is_temp(page_no)
                || snapshot.is_some_and(|(start, pages)| (start..start + pages).contains(&page_no))
            {
                continue;
            }
            self.spill_read_page(page_no, &mut page)?;
            read += 1;
            if crate::relstore::page::is_zero_page(&page) {
                continue;
            }
            let flags = u16::from_le_bytes([page[18], page[19]]);
            if flags & crate::encryption::PAGE_FLAG_SEALED != 0 {
                continue;
            }
            let header = crate::relstore::page::read_header(&page);
            let mut ctx = self.page_ctx(0);
            if header.page_type == crate::relstore::page::PAGE_TYPE_BLOB
                && crate::relstore::page::verify_checksum(&page)
            {
                ctx.rewrite_blob_page(page_no, &page)?;
            } else {
                // Through the pool: a logged image of the page as it is, which
                // the next flush writes sealed.
                ctx.log_system_image(page_no)?;
            }
            conversion.rewrote = true;
        }
        if conversion.cursor < data_pages {
            return Ok(ConversionStep::Working);
        }
        // A whole sweep done. Pages rewritten through the pool are sealed on
        // disk only once flushed: sweep again after a checkpoint.
        conversion.cursor = 0;
        if std::mem::take(&mut conversion.rewrote) {
            return Ok(ConversionStep::NeedsCheckpoint);
        }
        // Nothing plain left among the pages. The snapshot and the log from
        // its head on must be sealed too.
        let snapshot_sealed = match self.load_active_snapshot_descriptor()? {
            Some(desc) => desc.sealed(),
            None => true,
        };
        if !snapshot_sealed || self.wal.head() < conversion.sealed_from {
            return Ok(ConversionStep::NeedsCheckpoint);
        }
        conversion.zeroing = Some(0);
        Ok(ConversionStep::Working)
    }

    /// Zeroes up to `budget` free data pages that hold something (old
    /// plaintext, maybe), then finishes.
    fn zero_free_pages(
        &mut self,
        conversion: &mut Conversion,
        budget: usize,
    ) -> Result<ConversionStep, StorageError> {
        // A page is zeroed only once its free is durable: a free not yet
        // committed durably could be undone by a crash.
        self.wal.sync_all()?;
        let data_pages = self.layout.data_size / PAGE_SIZE as u64;
        let zero = vec![0u8; PAGE_SIZE];
        let mut page = vec![0u8; PAGE_SIZE];
        let mut at = conversion.zeroing.unwrap_or(0);
        let mut read = 0;
        while at < data_pages && read < budget {
            // Only a page that holds something: writing zeros over a sparse
            // hole would allocate it on disk.
            if !self.allocator.is_allocated(at) {
                self.spill_read_page(at, &mut page)?;
                read += 1;
                if !crate::relstore::page::is_zero_page(&page) {
                    self.spill_write_page(at, &zero)?;
                }
            }
            at += 1;
        }
        conversion.zeroing = Some(at);
        if at < data_pages {
            return Ok(ConversionStep::Working);
        }
        self.file.sync_data()?;
        self.finish_conversion()?;
        Ok(ConversionStep::Done)
    }

    /// The last step: the ring outside the live log is zeroed (old plaintext
    /// entries), then the key block's state becomes Encrypted and plain pages
    /// and entries are refused from then on.
    fn finish_conversion(&mut self) -> Result<(), StorageError> {
        let (head, tail) = (self.wal.head(), self.wal.tail());
        let ring = self.layout.wal_size;
        let page = PAGE_SIZE as u64;
        // The tail's own page is the writer's (its bytes past the tail are
        // zero), and the head's holds live entries: zero the whole pages in
        // between, a lap round.
        let from = tail.div_ceil(page) * page;
        let to = head - head % page + ring;
        if from < to {
            crate::wal::zero_ring_range(&mut self.file, self.layout.wal_offset, ring, from, to)?;
            self.file.sync_data()?;
        }
        let wal_keys = self.codec.wal_keys().expect("an encrypted codec");
        let header = self.header_on_disk()?;
        let block =
            wal_keys.restate_key_block(header.key_block(), crate::encryption::KEY_STATE_ENCRYPTED);
        self.write_key_block(header.key_block(), &block)?;
        wal_keys.finish_conversion();
        Ok(())
    }
}
