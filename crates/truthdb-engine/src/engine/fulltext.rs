//! The full-text indexer (S3 plan item 15): keeps the object engine's
//! full-text index in step with the change journal, backfills newly enabled
//! buckets and purges disabled ones. It runs on its own thread, a bounded
//! pass at a time; every storage-lock hold in a pass is short, and a body is
//! read and tokenized outside the lock under a lease.

use truthdb_fulltext::DocTerms;
use truthdb_object::{FtBucket, FtDoc, FtState, FtStatus, ListAfter, VersionSel};

use super::{Engine, EngineError};

/// Journal rows one pass applies at most.
const JOURNAL_BATCH: usize = 64;
/// Keys one pass backfills per bucket at most.
const BACKFILL_BATCH: usize = 64;
/// Documents (or key rows) one purge transaction removes at most.
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
    /// Returns whether work remains. A standby never indexes (its log is the
    /// primary's).
    pub(crate) fn fulltext_pass(&self) -> Result<bool, EngineError> {
        if self.storage.is_standby() || !self.runs_objects() {
            return Ok(false);
        }
        let buckets = self.object(|file| file.fulltext_buckets())?;
        if buckets.is_empty() {
            return Ok(false);
        }
        let mut more = false;
        for bucket in &buckets {
            if bucket.state == FtState::Purging {
                let done = self.object(|file| {
                    file.transaction(|tx| tx.purge_fulltext(&bucket.name, PURGE_BATCH))
                })?;
                more |= !done;
            }
        }
        let cursor = self.object(|file| file.fulltext_cursor())?;
        let records = self.object(|file| file.journal(cursor, JOURNAL_BATCH))?;
        for record in &records {
            if let Some(bucket) = buckets
                .iter()
                .find(|bucket| bucket.name == record.bucket && bucket.state != FtState::Purging)
            {
                self.fulltext_sync(bucket, &record.key)?;
            }
        }
        if let Some(last) = records.last() {
            self.object(|file| file.transaction(|tx| tx.set_fulltext_cursor(last.sequence)))?;
            more |= records.len() == JOURNAL_BATCH;
        }
        for bucket in &buckets {
            let FtState::Backfilling(after) = &bucket.state else {
                continue;
            };
            let listing = self.object(|file| {
                file.list_objects(
                    &bucket.name,
                    "",
                    None,
                    after.as_deref().map(ListAfter::Key),
                    BACKFILL_BATCH,
                )
            })?;
            // A bucket deleted since is purging now.
            let Ok(listing) = listing else {
                continue;
            };
            for object in &listing.objects {
                self.fulltext_sync(bucket, &object.key)?;
            }
            let next = listing
                .truncated
                .then(|| listing.last.map(|(key, _)| key))
                .flatten();
            more |= next.is_some();
            self.object(|file| {
                file.transaction(|tx| tx.set_fulltext_backfill(&bucket.name, next.as_deref()))
            })?;
        }
        Ok(more)
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
            // Held as wanted, or failed already: a failure is retried only by
            // a later write (or enabling the bucket again).
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
        let apply = |doc: FtDoc<'_>| {
            self.object(|file| {
                file.transaction(|tx| tx.apply_fulltext(&bucket.name, key, version, doc))
            })
        };
        match &terms {
            Ok(terms) => {
                if apply(FtDoc::Indexed(terms)).is_err() {
                    // Rolled back (its terms did not fit the log, say): hold the
                    // version as failed so the journal moves on.
                    apply(FtDoc::Skipped(FtStatus::Failed))?;
                }
            }
            Err(status) => {
                apply(FtDoc::Skipped(*status))?;
            }
        }
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
