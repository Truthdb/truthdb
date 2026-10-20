//! The full-text indexer (S3 plan item 15): keeps the object engine's
//! full-text index in step with the change journal, backfills newly enabled
//! buckets and purges disabled ones. It runs on its own thread, a bounded
//! pass at a time; a body is read and tokenized outside the storage lock
//! under a lease, and each transaction is bounded by its log: a large
//! document is written, and removed, over several (see
//! `ObjTxn::fulltext_build_step`).

use truthdb_fulltext::DocTerms;
use truthdb_fulltext::Progress;
use truthdb_object::{FtBucket, FtState, FtStatus, ListAfter, VersionSel};

use super::{Engine, EngineError};

/// Journal rows one pass applies at most.
const JOURNAL_BATCH: usize = 64;
/// Keys one pass backfills per bucket at most.
const BACKFILL_BATCH: usize = 64;
/// Documents (or key rows) one purge transaction removes at most (and
/// fewer when they fill its log budget).
const PURGE_BATCH: usize = 256;
/// Bytes of a body read per storage-lock hold.
const READ_CHUNK: usize = 1 << 20;

/// What syncing a key needs done, decided under one lock hold.
enum Plan {
    /// The index holds what it should.
    Nothing,
    /// Hold `version` as skipped for `status` (or nothing, with `None`).
    Record(Option<u64>, FtStatus),
    /// Index `version`, whose body (leased) is `size` bytes.
    Read { version: u64, body: u64, size: u64 },
}

impl Engine {
    /// One pass of the full-text indexer: purges, journal rows, backfill.
    /// Returns whether work remains; `stop` is asked between keys, so a
    /// shutdown does not wait out a whole pass. A failure in one part leaves
    /// the others to run and is returned at the end. A standby never indexes
    /// (its log is the primary's).
    pub(crate) fn fulltext_pass(&self, stop: &dyn Fn() -> bool) -> Result<bool, EngineError> {
        if self.storage.is_standby() || !self.runs_objects() {
            return Ok(false);
        }
        let buckets = self.object(|file| file.fulltext_buckets())?;
        if buckets.is_empty() {
            return Ok(false);
        }
        let mut more = false;
        let mut failure = None;
        let mut note = |result: Result<bool, EngineError>| match result {
            Ok(left) => more |= left,
            Err(err) => {
                failure.get_or_insert(err);
            }
        };
        for bucket in &buckets {
            if bucket.state == FtState::Purging {
                note(
                    self.object(|file| {
                        file.transaction(|tx| tx.purge_fulltext(&bucket.name, PURGE_BATCH))
                    })
                    .map(|done| !done),
                );
            }
        }
        note(self.fulltext_journal(&buckets, stop));
        for bucket in &buckets {
            if let FtState::Backfilling(after) = &bucket.state {
                note(self.fulltext_backfill(bucket, after.as_deref(), stop));
            }
        }
        match failure {
            Some(err) => Err(err),
            None => Ok(more),
        }
    }

    /// Applies the journal rows after the cursor (a batch) to the indexed
    /// buckets, and moves the cursor past the ones applied. Returns whether
    /// rows remain.
    fn fulltext_journal(
        &self,
        buckets: &[FtBucket],
        stop: &dyn Fn() -> bool,
    ) -> Result<bool, EngineError> {
        let cursor = self.object(|file| file.fulltext_cursor())?;
        let records = self.object(|file| file.journal(cursor, JOURNAL_BATCH))?;
        let mut applied = None;
        let mut outcome = Ok(records.len() == JOURNAL_BATCH);
        for record in &records {
            if stop() {
                outcome = Ok(true);
                break;
            }
            if let Some(bucket) = buckets
                .iter()
                .find(|bucket| bucket.name == record.bucket && bucket.state != FtState::Purging)
                && let Err(err) = self.fulltext_sync(bucket, &record.key)
            {
                outcome = Err(err);
                break;
            }
            applied = Some(record.sequence);
        }
        if let Some(sequence) = applied {
            self.object(|file| file.transaction(|tx| tx.set_fulltext_cursor(sequence)))?;
        }
        outcome
    }

    /// Backfills a batch of `bucket`'s keys after `after`, and records how
    /// far it got. Returns whether keys remain.
    fn fulltext_backfill(
        &self,
        bucket: &FtBucket,
        after: Option<&str>,
        stop: &dyn Fn() -> bool,
    ) -> Result<bool, EngineError> {
        let listing = self.object(|file| {
            file.list_objects(
                &bucket.name,
                "",
                None,
                after.map(ListAfter::Key),
                BACKFILL_BATCH,
            )
        })?;
        // A bucket deleted since is purging now.
        let Ok(listing) = listing else {
            return Ok(false);
        };
        // The last key synced, and whether the whole page was.
        let mut done = None;
        let mut complete = true;
        let mut outcome = Ok(());
        for object in &listing.objects {
            if stop() {
                complete = false;
                break;
            }
            if let Err(err) = self.fulltext_sync(bucket, &object.key) {
                outcome = Err(err);
                complete = false;
                break;
            }
            done = Some(object.key.clone());
        }
        let next = if complete {
            // A truncated page may end past its last listed key (rows read
            // but not listed): resume where the listing says.
            listing
                .truncated
                .then(|| listing.last.map(|(key, _)| key).or(done))
                .flatten()
        } else {
            match done {
                Some(key) => Some(key),
                None => return outcome.map(|()| true),
            }
        };
        self.object(|file| {
            file.transaction(|tx| {
                tx.set_fulltext_backfill(&bucket.name, bucket.generation, next.as_deref())
            })
        })?;
        outcome.map(|()| next.is_some())
    }

    /// Makes the index hold what it should for `key`: its latest live
    /// version, indexed or skipped, or nothing.
    pub(in crate::engine) fn fulltext_sync(
        &self,
        bucket: &FtBucket,
        key: &str,
    ) -> Result<(), EngineError> {
        let plan = self.object(|file| {
            let held = file.fulltext_key(&bucket.name, key)?;
            let latest = file
                .head_object(&bucket.name, key, VersionSel::Latest)?
                .ok();
            let Some(info) = latest else {
                return Ok(match held {
                    Some(_) => Plan::Record(None, FtStatus::Indexed),
                    None => Plan::Nothing,
                });
            };
            let wanted = truthdb_object::fulltext_status(&info, bucket.max_bytes)
                .unwrap_or(FtStatus::Indexed);
            // Held as wanted, or failed already: a version whose body could
            // not be read is retried only by a later write.
            if held.is_some_and(|held| {
                held.version == info.version
                    && (held.status == wanted || held.status == FtStatus::Failed)
            }) {
                return Ok(Plan::Nothing);
            }
            if wanted != FtStatus::Indexed {
                return Ok(Plan::Record(Some(info.version), wanted));
            }
            file.take_lease(info.body)?;
            Ok(Plan::Read {
                version: info.version,
                body: info.body,
                size: info.size,
            })
        })?;
        let (version, terms) = match plan {
            Plan::Nothing => return Ok(()),
            Plan::Record(version, status) => (version, Err(status)),
            Plan::Read {
                version,
                body,
                size,
            } => {
                let text = self.fulltext_read(body, size);
                self.object(|file| {
                    file.release_lease(body);
                    Ok(())
                })?;
                (
                    Some(version),
                    // A body that cannot be read is held as failed rather
                    // than retried on every pass.
                    text.map(|text| DocTerms::from_text(&text))
                        .map_err(|_| FtStatus::Failed),
                )
            }
        };
        // Each step is a transaction within the log budget; a failure (the
        // log full for the moment, say) is returned and the key synced again
        // from the start. First whatever the index holds for the key goes.
        let (name, generation) = (bucket.name.as_str(), bucket.generation);
        loop {
            match self.object(|file| {
                file.transaction(|tx| tx.fulltext_remove_step(name, key, generation))
            })? {
                None => return Ok(()),
                Some(true) => break,
                Some(false) => {}
            }
        }
        let Some(version) = version else {
            return Ok(());
        };
        let terms = match terms {
            Ok(terms) => terms,
            Err(status) => {
                self.object(|file| {
                    file.transaction(|tx| {
                        tx.fulltext_record(name, key, generation, version, status)
                    })
                })?;
                return Ok(());
            }
        };
        let mut progress = Progress::default();
        loop {
            match self.object(|file| {
                file.transaction(|tx| {
                    tx.fulltext_build_step(name, key, generation, version, &terms, &mut progress)
                })
            })? {
                // Not the latest any more, or re-enabled: the part written
                // is held as such, for the key's next sync to remove.
                None => return Ok(()),
                Some(true) => break,
                Some(false) => {}
            }
        }
        let chunks = progress.next_chunk;
        self.object(|file| {
            file.transaction(|tx| {
                tx.fulltext_finish(name, key, generation, version, terms.len, chunks)
            })
        })?;
        Ok(())
    }

    /// A leased body's text (invalid UTF-8 replaced), read a chunk per
    /// storage-lock hold.
    fn fulltext_read(&self, body: u64, size: u64) -> Result<String, EngineError> {
        let mut bytes = vec![0u8; size as usize];
        for (index, chunk) in bytes.chunks_mut(READ_CHUNK).enumerate() {
            let offset = (index * READ_CHUNK) as u64;
            self.object(|file| file.read_body(body, offset, chunk))?;
        }
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}
