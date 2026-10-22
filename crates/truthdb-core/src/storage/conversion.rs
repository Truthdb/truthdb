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
//!
//! `encryption rotate database key` replaces the database key the same way:
//! the key block (state Rekeying) holds the new key and the old one, every
//! page and entry written from then on is sealed under the new key while the
//! old key still opens what it sealed, and the same pass rewrites every page
//! the old key sealed. At the end the old key leaves the key block.

use super::*;

/// The state of a conversion under way.
#[derive(Debug, Clone)]
pub(crate) struct Conversion {
    /// A rekeying (old key to new), not a conversion from plain.
    rekey: bool,
    /// A rekeying's snapshot when first found otherwise done: a newer one
    /// (written by a checkpoint since, under the new key) is needed.
    snapshot_generation: Option<u64>,
    /// The next data page the sweep looks at.
    cursor: u64,
    /// The sweep rewrote a page since it last started.
    rewrote: bool,
    /// Every log entry from this LSN on is sealed.
    sealed_from: u64,
    /// A whole sweep found nothing plain: what remains is the snapshot and
    /// the log's head (no page written since can be plain).
    clean: bool,
    /// The log's head cannot move past `sealed_from` yet (a log backup, a
    /// replication slot or a long transaction holds it).
    waiting_for_log: bool,
    /// The completion's free-page zeroing has reached this page (`None`: the
    /// sweep is still running).
    zeroing: Option<u64>,
    /// Spill extents that existed at the switch: their pages the spool never
    /// wrote may hold old plaintext, so they are zeroed once freed (extents
    /// allocated since are zeroed when allocated).
    spill_extents: Vec<(u64, u64)>,
}

impl Conversion {
    /// A rekeying (not a conversion from plain).
    pub(crate) fn is_rekey(&self) -> bool {
        self.rekey
    }

    /// A spill extent of the switch, handed out again (and zeroed): no
    /// longer one to wait for.
    pub(crate) fn forget_spill_extent(&mut self, start: u64) {
        self.spill_extents.retain(|&(extent, _)| extent != start);
    }

    pub(crate) fn new(sealed_from: u64, rekey: bool) -> Self {
        Conversion {
            rekey,
            snapshot_generation: None,
            cursor: 0,
            rewrote: false,
            sealed_from,
            clean: false,
            waiting_for_log: false,
            zeroing: None,
            spill_extents: Vec::new(),
        }
    }

    /// The pass a database opened (or restored) with `codec` resumes, if its
    /// key block says one is under way. Entries from `sealed_from` on are
    /// sealed under the current key.
    pub(crate) fn resumed(codec: &crate::encryption::PageCodec, sealed_from: u64) -> Option<Self> {
        let keys = codec.wal_keys()?;
        (keys.converting() || keys.rekeying())
            .then(|| Conversion::new(sealed_from, keys.rekeying()))
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
    /// Only the log's head is left, and it cannot move past the switch yet:
    /// a log backup, a replication slot or a long transaction holds it.
    WaitingForLog,
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
        // A connected standby would be streamed sealed entries it cannot
        // open: it is reseeded after the switch anyway. The registry stays
        // locked across the switch, so none joins in between (a join checks
        // the key fingerprint under the same lock).
        let connected = self
            .repl_connected
            .lock()
            .expect("repl-connected set poisoned");
        if !connected.is_empty() {
            return Err(StorageError::InvalidConfig(
                "a standby is connected: stop the standbys first (reseed them from a backup \
                 taken after `encryption enable`)"
                    .to_string(),
            ));
        }
        self.lock()
            .switch_to_encrypted(&header, file_uuid, &block, &dek)?;
        self.encrypted
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// `encryption rotate database key`: replaces this encrypted database's
    /// key with a new one. Every page and log entry written from now on is
    /// sealed under the new key; [`Self::conversion_step`] rewrites what the
    /// old one sealed. The old key is unwrapped and the new one made (which
    /// may run a key command) without the storage lock.
    pub fn rotate_database_key(&self) -> Result<(), StorageError> {
        let (header, source) = self.lock().header_to_rekey()?;
        let old =
            crate::encryption::unlock_key_block(header.key_block(), &source, &header.file_uuid)?;
        let (block, dek) =
            crate::encryption::new_rekeying_key_block(&source, &header.file_uuid, &old)?;
        // As for `encryption enable`: a standby cannot open what the new key
        // seals, and none joins across the switch.
        let connected = self
            .repl_connected
            .lock()
            .expect("repl-connected set poisoned");
        if !connected.is_empty() {
            return Err(StorageError::InvalidConfig(
                "a standby is connected: stop the standbys first (reseed them from a backup \
                 taken after the rotation)"
                    .to_string(),
            ));
        }
        self.lock().switch_to_rekeyed(&header, &block, &dek, &old)
    }

    /// One bounded step (at most `budget` pages read) of the conversion under
    /// way, under the storage lock.
    pub fn conversion_step(&self, budget: usize) -> Result<ConversionStep, StorageError> {
        self.lock().conversion_step(budget)
    }

    /// Whether the conversion under way waits for the log to be truncated
    /// past the switch (a log backup, a replication slot or a long
    /// transaction holds it).
    pub fn conversion_waits_for_log(&self) -> bool {
        self.lock()
            .conversion
            .as_ref()
            .is_some_and(|conversion| conversion.waiting_for_log)
    }
}

impl StorageFile {
    /// How far the conversion or rekeying under way is, in percent (`None`:
    /// none).
    pub(super) fn conversion_percent(&self) -> Option<u32> {
        let conversion = self.conversion.as_ref()?;
        let pages = (self.layout.data_size / PAGE_SIZE as u64).max(1);
        let done = match conversion.zeroing {
            Some(_) => pages,
            None => conversion.cursor,
        };
        Some((done * 100 / pages).min(99) as u32)
    }

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

    /// The header of this encrypted database and the master key source, for
    /// `encryption rotate database key`.
    fn header_to_rekey(
        &mut self,
    ) -> Result<(FileHeader, crate::encryption::KeySource), StorageError> {
        if self.active_sb().is_standby() {
            return Err(StorageError::InvalidConfig(
                "a standby's key is not rotated: rotate it on the primary, then reseed the \
                 standby from a backup taken after it"
                    .to_string(),
            ));
        }
        let header = self.header_on_disk()?;
        if !header.is_encrypted() {
            return Err(StorageError::InvalidConfig(
                "this database is not encrypted: `encryption enable` first".to_string(),
            ));
        }
        // The key block on disk decides too: a second rotation over one not
        // settled would drop the old key that pages still need.
        if self.conversion.is_some()
            || crate::encryption::key_block_state(header.key_block())
                != crate::encryption::KEY_STATE_ENCRYPTED
        {
            return Err(StorageError::InvalidConfig(
                "a conversion or key rotation is under way: rotate after it finishes".to_string(),
            ));
        }
        let source = self.key_source.clone().ok_or_else(|| {
            StorageError::InvalidConfig(
                "no master key is configured: set [encryption] and restart first".to_string(),
            )
        })?;
        Ok((header, source))
    }

    /// Writes the key block (Rekeying) and switches to sealing under the new
    /// key: the header copy first, then the header (a crash before the
    /// header leaves the old key in place), then pages and log entries
    /// written from now on are sealed under the new key.
    fn switch_to_rekeyed(
        &mut self,
        before: &FileHeader,
        block: &[u8],
        dek: &[u8; 32],
        old: &[u8; 32],
    ) -> Result<(), StorageError> {
        // A backup in progress recorded the old key block: pages and log
        // copied after the switch would be sealed under a key it lacks.
        if self.truncation_gate.backup.is_some() || self.log_backup_in_progress {
            return Err(StorageError::InvalidConfig(
                "a backup is in progress: rotate the database key after it finishes".to_string(),
            ));
        }
        let mut header = self.header_on_disk()?;
        if header.to_le_bytes_with_checksum() != before.to_le_bytes_with_checksum() {
            return Err(StorageError::InvalidConfig(
                "the file header changed while the key was made (another rotation ran): retry"
                    .to_string(),
            ));
        }
        header.set_key_block(block);
        header.header_checksum = header.compute_checksum();
        let out = header.to_le_bytes_with_checksum();
        let end = header.reserved_offset + header.reserved_size;
        self.file.write_all_at(end - PAGE_SIZE as u64, &out)?;
        self.file.sync_data()?;
        self.file.write_all_at(0, &out)?;
        self.file.sync_data()?;

        let keys = crate::encryption::PageKeys::rekeying(dek, old, &header.file_uuid);
        self.codec = crate::encryption::PageCodec::Encrypted(Box::new(keys));
        let wal_keys = self.codec.wal_keys().expect("an encrypted codec");
        self.wal.start_sealing(&wal_keys)?;
        self.key_record = Some(crate::backup::KeyRecord {
            file_uuid: header.file_uuid,
            key_block: block.to_vec(),
            fingerprint: wal_keys.fingerprint(),
        });
        let mut conversion = Conversion::new(self.wal.tail(), true);
        conversion.spill_extents = self.allocator.temp_extents();
        self.conversion = Some(conversion);
        Ok(())
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
        // A backup in progress recorded the database as plain: its pages and
        // log copied after the switch would be sealed.
        if self.truncation_gate.backup.is_some() || self.log_backup_in_progress {
            return Err(StorageError::InvalidConfig(
                "a backup is in progress: enable encryption after it finishes".to_string(),
            ));
        }
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
        let mut conversion = Conversion::new(self.wal.tail(), false);
        conversion.spill_extents = self.allocator.temp_extents();
        self.conversion = Some(conversion);
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
            Some(_) => self.zero_free_pages(&mut conversion, budget)?,
            None if conversion.clean => self.ready_to_finish(&mut conversion)?,
            None => self.sweep(&mut conversion, budget)?,
        };
        self.conversion = match step {
            ConversionStep::Done => None,
            _ => Some(conversion),
        };
        Ok(step)
    }

    /// Rewrites up to `budget` pages still plain (or, rekeying, sealed under
    /// the old key) on disk.
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
            // Sealed under the current key: done (garbage can carry the
            // sealed bit, so it must open).
            if self.codec.open_current(page_no, &mut page.clone()) {
                continue;
            }
            // The page's plaintext, when it is a readable page still to do.
            let plain = if conversion.rekey {
                let mut opened = page.clone();
                self.codec
                    .open_previous(page_no, &mut opened)
                    .then_some(opened)
            } else {
                let flags = u16::from_le_bytes([page[18], page[19]]);
                (flags & crate::encryption::PAGE_FLAG_SEALED == 0
                    && crate::relstore::page::verify_checksum(&page))
                .then(|| page.clone())
            };
            let blob = plain.as_ref().is_some_and(|plain| {
                crate::relstore::page::read_header(plain).page_type
                    == crate::relstore::page::PAGE_TYPE_BLOB
            });
            if let (Some(plain), true) = (&plain, blob) {
                self.page_ctx(0).rewrite_blob_page(page_no, plain)?;
            } else if plain.is_some() || self.pages.pool.contains(page_no) {
                // Through the pool: a logged image of the page as it is (a
                // cached frame may hold its newest version), which the next
                // flush writes sealed.
                self.page_ctx(0).log_system_image(page_no)?;
            } else {
                // Not a readable page at all, and not cached (bytes left in a
                // leaked extent): it was unreadable and stays so, without its
                // plaintext. Written straight to disk, so nothing for a
                // checkpoint to flush; a later sweep finds the noise again.
                self.overwrite_unreadable(page_no)?;
                continue;
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
        // Nothing plain left among the pages, and every page written from now
        // on is sealed: no sweep again.
        conversion.clean = true;
        self.ready_to_finish(conversion)
    }

    /// With the pages done: the snapshot and the log from its head on must be
    /// sealed too. A checkpoint seals the snapshot and moves the head, unless
    /// something holds the log where it is: then the conversion waits.
    fn ready_to_finish(
        &mut self,
        conversion: &mut Conversion,
    ) -> Result<ConversionStep, StorageError> {
        let snapshot_sealed = match self.load_active_snapshot_descriptor()? {
            // The snapshot found when the pages were done may still be under
            // the old key: one written after it is not.
            Some(desc) if conversion.rekey => {
                *conversion
                    .snapshot_generation
                    .get_or_insert(desc.generation)
                    < desc.generation
            }
            Some(desc) => desc.sealed(),
            None => true,
        };
        if !snapshot_sealed {
            return Ok(ConversionStep::NeedsCheckpoint);
        }
        if self.wal.head() < conversion.sealed_from {
            conversion.waiting_for_log = self.checkpoint_wal_head() < conversion.sealed_from;
            return Ok(if conversion.waiting_for_log {
                ConversionStep::WaitingForLog
            } else {
                ConversionStep::NeedsCheckpoint
            });
        }
        conversion.waiting_for_log = false;
        conversion.zeroing = Some(0);
        Ok(ConversionStep::Working)
    }

    /// Overwrites an allocated page that is not a readable page (not plain,
    /// not sealed) with noise, marked sealed: it stays unreadable, and its
    /// bytes (old plaintext, maybe) are gone.
    fn overwrite_unreadable(&mut self, page_no: u64) -> Result<(), StorageError> {
        use ring::rand::SecureRandom;
        let mut noise = vec![0u8; PAGE_SIZE];
        ring::rand::SystemRandom::new()
            .fill(&mut noise)
            .map_err(|_| StorageError::InvalidConfig("no randomness".to_string()))?;
        let flags =
            u16::from_le_bytes([noise[18], noise[19]]) | crate::encryption::PAGE_FLAG_SEALED;
        noise[18..20].copy_from_slice(&flags.to_le_bytes());
        self.spill_write_page(page_no, &noise)
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
        // The spill extents of the switch, once their queries freed them.
        let freed: Vec<(u64, u64)> = conversion
            .spill_extents
            .iter()
            .copied()
            .filter(|&(start, _)| !self.allocator.is_temp(start))
            .collect();
        for (start, pages) in freed {
            for at in start..start + pages {
                if !self.allocator.is_allocated(at) {
                    self.spill_read_page(at, &mut page)?;
                    if !crate::relstore::page::is_zero_page(&page) {
                        self.spill_write_page(at, &zero)?;
                    }
                }
            }
            conversion
                .spill_extents
                .retain(|&extent| extent != (start, pages));
        }
        if !conversion.spill_extents.is_empty() {
            return Ok(ConversionStep::Working);
        }
        self.file.sync_data()?;
        self.finish_conversion()?;
        Ok(ConversionStep::Done)
    }

    /// The last step: the ring outside the live log is zeroed (old plaintext
    /// entries, or entries under the old key), then the key block's state
    /// becomes Encrypted (the old key leaves it) and plain pages and entries,
    /// and those under the old key, are refused from then on.
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
        }
        // The head's page before the head: dead log, maybe plain. When it is
        // also the tail's page, the writer holds its prefix in memory, so it
        // is seated again from the zeroed disk.
        if head % page != 0 {
            let offset = self.layout.wal_offset + (head - head % page) % ring;
            let mut bytes = vec![0u8; PAGE_SIZE];
            self.file.read_exact_at(offset, &mut bytes)?;
            bytes[..(head % page) as usize].fill(0);
            self.file.write_all_at(offset, &bytes)?;
        }
        self.file.sync_data()?;
        if head % page != 0 && head - head % page == tail - tail % page {
            self.wal.resync_tail(tail)?;
        }
        let wal_keys = self.codec.wal_keys().expect("an encrypted codec");
        let header = self.header_on_disk()?;
        let block = wal_keys.settle_key_block(header.key_block());
        self.write_key_block(header.key_block(), &block)?;
        wal_keys.finish_conversion();
        self.codec.finish_rekeying();
        Ok(())
    }
}
