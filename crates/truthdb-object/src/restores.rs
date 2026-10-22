//! Restores of archived objects (S3 RestoreObject). A version in an archive
//! class (GLACIER, DEEP_ARCHIVE) cannot be read until it is restored. Its
//! body is not copied: a restore is a window of time, from when it is ready
//! until it expires, during which reads are allowed. The window lives in the
//! restores tree under the version's number, and the restore timers tree
//! orders the windows by expiry, so expired ones are removed in the
//! background. A window past its expiry no longer counts, removed or not.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::{self, timed_key};
use crate::objects::{ObjectError, StorageClass, VersionSel, validate_key};
use crate::{ObjFile, ObjTxn, RESTORE_TIMERS_TREE, RESTORES_TREE};

const DAY_MS: u64 = 86_400_000;

/// A version's restore window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Restore {
    /// When the restore completes (reads are allowed from then on).
    pub ready_ms: u64,
    /// When the restored copy expires.
    pub expiry_ms: u64,
}

/// Where a restore stands at a given time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreState {
    Ongoing,
    Restored { expiry_ms: u64 },
}

impl Restore {
    /// Where the restore stands at `now_ms`; `None` once it has expired.
    pub fn state(&self, now_ms: u64) -> Option<RestoreState> {
        if now_ms >= self.expiry_ms {
            None
        } else if now_ms < self.ready_ms {
            Some(RestoreState::Ongoing)
        } else {
            Some(RestoreState::Restored {
                expiry_ms: self.expiry_ms,
            })
        }
    }

    /// `[ready ms u64][expiry ms u64]`.
    fn encode(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.ready_ms.to_le_bytes());
        out[8..].copy_from_slice(&self.expiry_ms.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != 16 {
            return Err(StorageError::InvalidFile("corrupt restore".to_string()));
        }
        Ok(Restore {
            ready_ms: codec::u64_le(&bytes[..8])?,
            expiry_ms: codec::u64_le(&bytes[8..])?,
        })
    }
}

/// What a RestoreObject asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoreRequest {
    /// How long the restored copy is kept.
    pub days: u32,
    /// How long the restore takes.
    pub delay_ms: u64,
    /// The Expedited retrieval tier, which DEEP_ARCHIVE does not offer.
    pub expedited: bool,
}

/// What a RestoreObject did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restored {
    /// A new restore began (S3 answers 202).
    Started,
    /// The version was already restored; its expiry moved (S3 answers 200).
    Extended,
}

/// When a restore kept `days` from `from_ms` expires: `days` after it,
/// rounded up to the next midnight UTC, as S3 computes it (from when a new
/// restore completes, or when an extension is asked for).
pub fn restore_expiry(from_ms: u64, days: u32) -> u64 {
    let end = from_ms.saturating_add(u64::from(days).saturating_mul(DAY_MS));
    (end / DAY_MS).saturating_add(1).saturating_mul(DAY_MS)
}

impl StorageClass {
    /// Whether a version in this class must be restored before it is read.
    pub fn is_archived(self) -> bool {
        matches!(self, StorageClass::Glacier | StorageClass::DeepArchive)
    }
}

/// Whether a version of `class` whose restore stands at `restore` may not
/// be read: it is archived and not restored.
pub fn unreadable(class: StorageClass, restore: Option<RestoreState>) -> bool {
    class.is_archived() && !matches!(restore, Some(RestoreState::Restored { .. }))
}

impl ObjTxn<'_, '_> {
    /// Restores the version `which` picks (S3 RestoreObject): it may be
    /// read from `delay_ms` after now until its days from then are up (see
    /// [`restore_expiry`]). On a version already restored, only the expiry
    /// moves, counted from now; one still being restored is refused.
    /// Returns what was done, the version's number and whether it is the
    /// null version.
    pub fn restore_object(
        &mut self,
        bucket: &str,
        key: &str,
        which: VersionSel,
        request: RestoreRequest,
        now_ms: u64,
    ) -> Result<Result<(Restored, u64, bool), ObjectError>, StorageError> {
        if let Err(err) = validate_key(key) {
            return Ok(Err(err));
        }
        if self.bucket_row(bucket)?.is_none() {
            return Ok(Err(ObjectError::NoSuchBucket));
        }
        let (version, null_version, class) = match self.find_live(bucket, key, which)? {
            Ok(found) => found,
            Err(err) => return Ok(Err(err)),
        };
        if !class.is_archived() {
            return Ok(Err(ObjectError::NotArchived));
        }
        let current = self.restore_of(version)?;
        let (done, restore) = match current.and_then(|restore| restore.state(now_ms)) {
            Some(RestoreState::Ongoing) => return Ok(Err(ObjectError::RestoreInProgress)),
            Some(RestoreState::Restored { .. }) => (
                Restored::Extended,
                Restore {
                    expiry_ms: restore_expiry(now_ms, request.days),
                    ..current.unwrap()
                },
            ),
            // Only a new restore retrieves anything, so only it has a tier.
            None if request.expedited && class == StorageClass::DeepArchive => {
                return Ok(Err(ObjectError::TierUnavailable));
            }
            None => {
                let ready_ms = now_ms.saturating_add(request.delay_ms);
                (
                    Restored::Started,
                    Restore {
                        ready_ms,
                        expiry_ms: restore_expiry(ready_ms, request.days),
                    },
                )
            }
        };
        self.forget_restore(version)?;
        let mode = &mut OpMode::Txn(self.txn);
        self.trees.restores().insert_unique(
            self.ctx,
            mode,
            &version.to_be_bytes(),
            &restore.encode(),
        )?;
        self.trees.restore_timers().insert_unique(
            self.ctx,
            mode,
            &timed_key(restore.expiry_ms, version),
            &[],
        )?;
        Ok(Ok((done, version, null_version)))
    }

    fn restore_of(&mut self, version: u64) -> Result<Option<Restore>, StorageError> {
        self.trees
            .restores()
            .get(self.ctx, &version.to_be_bytes())?
            .map(|row| Restore::decode(&row))
            .transpose()
    }

    /// Removes a version's restore and its timer, if it has one.
    pub(crate) fn forget_restore(&mut self, version: u64) -> Result<(), StorageError> {
        let Some(restore) = self.restore_of(version)? else {
            return Ok(());
        };
        let mode = &mut OpMode::Txn(self.txn);
        self.trees
            .restores()
            .delete(self.ctx, mode, &version.to_be_bytes())?;
        self.trees.restore_timers().delete(
            self.ctx,
            mode,
            &timed_key(restore.expiry_ms, version),
        )?;
        Ok(())
    }

    /// Removes up to `limit` restores expired by `now_ms`. Returns how many.
    pub fn expire_restores(&mut self, now_ms: u64, limit: usize) -> Result<usize, StorageError> {
        let due = self.trees.restore_timers().scan_range_limit(
            self.ctx,
            None,
            Some(&timed_key(now_ms, u64::MAX)),
            limit,
        )?;
        for (key, _) in &due {
            let (expiry_ms, version) = codec::decode_timed_key(key)?;
            self.trees
                .restore_timers()
                .delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
            if self
                .restore_of(version)?
                .is_some_and(|restore| restore.expiry_ms == expiry_ms)
            {
                self.trees.restores().delete(
                    self.ctx,
                    &mut OpMode::Txn(self.txn),
                    &version.to_be_bytes(),
                )?;
            }
        }
        Ok(due.len())
    }
}

impl ObjFile<'_> {
    /// A version's restore window, if it has one (expired or not).
    pub fn restore(&mut self, version: u64) -> Result<Option<Restore>, StorageError> {
        let Some(restores) = self.existing_tree(RESTORES_TREE)? else {
            return Ok(None);
        };
        restores
            .get(&mut self.core.page_ctx(0), &version.to_be_bytes())?
            .map(|row| Restore::decode(&row))
            .transpose()
    }

    /// Whether a version of `class` may not be read at `now_ms`: it is
    /// archived and not restored.
    pub fn archived_unreadable(
        &mut self,
        version: u64,
        class: StorageClass,
        now_ms: u64,
    ) -> Result<bool, StorageError> {
        if !class.is_archived() {
            return Ok(false);
        }
        let restore = self.restore(version)?;
        Ok(unreadable(
            class,
            restore.and_then(|restore| restore.state(now_ms)),
        ))
    }

    /// Whether any restore has expired by `now_ms` (read first, so an idle
    /// sweep writes nothing).
    pub fn restores_due(&mut self, now_ms: u64) -> Result<bool, StorageError> {
        let Some(timers) = self.existing_tree(RESTORE_TIMERS_TREE)? else {
            return Ok(false);
        };
        Ok(!timers
            .scan_range_limit(
                &mut self.core.page_ctx(0),
                None,
                Some(&timed_key(now_ms, u64::MAX)),
                1,
            )?
            .is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_expiry_is_rounded_up_to_the_next_midnight_utc() {
        // 2012-10-15 10:30 UTC plus 3 days expires 2012-10-19 00:00 UTC (the
        // example S3 documents).
        let request = 1_350_297_000_000;
        assert_eq!(restore_expiry(request, 3), 1_350_604_800_000);
        // Exactly midnight still moves to the next one.
        assert_eq!(restore_expiry(DAY_MS * 10, 1), DAY_MS * 12);
        assert_eq!(restore_expiry(u64::MAX - 5, u32::MAX), u64::MAX);
    }

    #[test]
    fn a_restore_is_ongoing_then_readable_then_gone() {
        let restore = Restore {
            ready_ms: 100,
            expiry_ms: 200,
        };
        assert_eq!(Restore::decode(&restore.encode()).unwrap(), restore);
        assert!(Restore::decode(&[0; 15]).is_err());
        assert_eq!(restore.state(99), Some(RestoreState::Ongoing));
        assert_eq!(
            restore.state(100),
            Some(RestoreState::Restored { expiry_ms: 200 })
        );
        assert_eq!(restore.state(200), None);
        assert!(unreadable(StorageClass::Glacier, None));
        assert!(unreadable(
            StorageClass::DeepArchive,
            Some(RestoreState::Ongoing)
        ));
        assert!(!unreadable(
            StorageClass::Glacier,
            Some(RestoreState::Restored { expiry_ms: 1 })
        ));
        assert!(!unreadable(StorageClass::GlacierIr, None));
        assert!(!unreadable(StorageClass::Standard, None));
    }
}
