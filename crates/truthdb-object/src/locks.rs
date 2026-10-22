//! Object Lock (S3's WORM protection): a version's retention (a mode and a
//! retain-until date) and legal hold, in the locks tree under the version's
//! number. While a version is protected the engine refuses to remove it, in
//! every path that removes versions, so whatever deletes through the engine
//! obeys the same rules. A multipart upload's requested lock waits under
//! `(1, upload)` and passes to the object on completion.
//!
//! A retention in COMPLIANCE mode can only be extended. One in GOVERNANCE
//! mode can be shortened, removed or ignored by a request that bypasses it
//! (the caller decides who may). A legal hold protects until it is released,
//! whatever the retention.
//!
//! A retention may be variable: while its event hold is on, the version is
//! kept until its duration after now (and at least until its date), so the
//! date moves forward; releasing the hold fixes it there.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::objects::{ObjectError, VersionSel, validate_key};
use crate::{LOCKS_TREE, ObjFile, ObjTxn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionMode {
    Governance,
    Compliance,
}

const DAY_MS: u64 = 86_400_000;

/// A variable retention's event hold and its duration (in days, or in years
/// of 365 days).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventHold {
    pub on: bool,
    pub years: bool,
    pub count: u32,
}

impl EventHold {
    pub fn duration_ms(&self) -> u64 {
        u64::from(self.count)
            .saturating_mul(if self.years { 365 } else { 1 })
            .saturating_mul(DAY_MS)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub mode: RetentionMode,
    /// The retain-until date; with an event hold, the earliest it may be
    /// (0: none).
    pub until_ms: u64,
    /// `None` for a fixed retention.
    pub event: Option<EventHold>,
}

impl Retention {
    /// The retain-until date at `now_ms`: while an event hold is on, its
    /// duration from now, or the date when that is later.
    pub fn effective_until(&self, now_ms: u64) -> u64 {
        match self.event {
            Some(event) if event.on => self
                .until_ms
                .max(now_ms.saturating_add(event.duration_ms())),
            _ => self.until_ms,
        }
    }
}

/// A PutObjectRetention change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionChange {
    Set(Retention),
    /// Turns the event hold off, fixing the date at its duration from now,
    /// or at `until_ms` when that is later. On a version not held, a date
    /// makes a fixed retention (0: none, and the release is refused).
    Release {
        mode: RetentionMode,
        until_ms: u64,
    },
    Remove,
}

/// A version's Object Lock settings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectLock {
    pub retention: Option<Retention>,
    /// `None` when no legal hold was ever set (S3 then shows none).
    pub legal_hold: Option<bool>,
}

impl ObjectLock {
    fn is_empty(&self) -> bool {
        *self == ObjectLock::default()
    }

    /// Whether the version may not be removed at `now_ms`; `bypass` lifts a
    /// GOVERNANCE retention (never a legal hold).
    pub fn blocks_delete(&self, now_ms: u64, bypass: bool) -> bool {
        self.legal_hold == Some(true)
            || self.retention.is_some_and(|retention| {
                retention.effective_until(now_ms) > now_ms
                    && !(retention.mode == RetentionMode::Governance && bypass)
            })
    }

    /// `[mode u8: 0 none, 1 GOVERNANCE, 2 COMPLIANCE][retain-until ms u64]
    /// [legal hold u8: 0 never set, 1 off, 2 on]`, then for a variable
    /// retention `[event hold u8: 1 on, 2 off][unit u8: 1 days, 2 years]
    /// [count u32]`.
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        match self.retention {
            None => out.extend_from_slice(&[0; 9]),
            Some(retention) => {
                out.push(match retention.mode {
                    RetentionMode::Governance => 1,
                    RetentionMode::Compliance => 2,
                });
                out.extend_from_slice(&retention.until_ms.to_le_bytes());
            }
        }
        out.push(match self.legal_hold {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        });
        if let Some(event) = self.retention.and_then(|retention| retention.event) {
            out.push(if event.on { 1 } else { 2 });
            out.push(if event.years { 2 } else { 1 });
            out.extend_from_slice(&event.count.to_le_bytes());
        }
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let corrupt = || StorageError::InvalidFile("corrupt object lock".to_string());
        if bytes.len() != 10 && bytes.len() != 16 {
            return Err(corrupt());
        }
        let until_ms = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let event = match bytes.get(10..16) {
            None => None,
            Some(&[on, unit, ref count @ ..]) => Some(EventHold {
                on: match on {
                    1 => true,
                    2 => false,
                    _ => return Err(corrupt()),
                },
                years: match unit {
                    1 => false,
                    2 => true,
                    _ => return Err(corrupt()),
                },
                count: u32::from_le_bytes(count.try_into().unwrap()),
            }),
            Some(_) => return Err(corrupt()),
        };
        let retention = match bytes[0] {
            0 if event.is_none() => None,
            1 => Some(RetentionMode::Governance),
            2 => Some(RetentionMode::Compliance),
            _ => return Err(corrupt()),
        }
        .map(|mode| Retention {
            mode,
            until_ms,
            event,
        });
        let legal_hold = match bytes[9] {
            0 => None,
            1 => Some(false),
            2 => Some(true),
            _ => return Err(corrupt()),
        };
        Ok(ObjectLock {
            retention,
            legal_hold,
        })
    }
}

fn upload_key(upload: u64) -> [u8; 9] {
    let mut key = [1u8; 9];
    key[1..].copy_from_slice(&upload.to_be_bytes());
    key
}

impl ObjTxn<'_, '_> {
    /// The lock of a version (none when it has none).
    pub fn lock_of(&mut self, version: u64) -> Result<ObjectLock, StorageError> {
        self.trees
            .locks()
            .get(self.ctx, &version.to_be_bytes())?
            .map_or(Ok(ObjectLock::default()), |row| ObjectLock::decode(&row))
    }

    /// Sets the lock of a version (none removes it). The caller checks that
    /// the bucket has Object Lock and the change is allowed.
    pub fn set_lock(&mut self, version: u64, lock: &ObjectLock) -> Result<(), StorageError> {
        self.write_lock(&version.to_be_bytes(), lock)
    }

    /// Sets the lock the object of `upload` will have when it completes.
    pub fn set_upload_lock(&mut self, upload: u64, lock: &ObjectLock) -> Result<(), StorageError> {
        self.write_lock(&upload_key(upload), lock)
    }

    /// Removes the lock `upload` asked for, returning it.
    pub(crate) fn take_upload_lock(&mut self, upload: u64) -> Result<ObjectLock, StorageError> {
        let locks = self.trees.locks();
        match locks.delete(self.ctx, &mut OpMode::Txn(self.txn), &upload_key(upload))? {
            Some(row) => ObjectLock::decode(&row),
            None => Ok(ObjectLock::default()),
        }
    }

    fn write_lock(&mut self, key: &[u8], lock: &ObjectLock) -> Result<(), StorageError> {
        let locks = self.trees.locks();
        locks.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
        if !lock.is_empty() {
            locks.insert_unique(self.ctx, &mut OpMode::Txn(self.txn), key, &lock.encode())?;
        }
        Ok(())
    }

    /// The version `which` picks, for an Object Lock change: its number and
    /// whether it is the null version, with its lock. The bucket must have
    /// Object Lock, and the version must not be a delete marker.
    fn lockable(
        &mut self,
        bucket: &str,
        key: &str,
        which: VersionSel,
    ) -> Result<Result<(u64, bool, ObjectLock), ObjectError>, StorageError> {
        if let Err(err) = validate_key(key) {
            return Ok(Err(err));
        }
        let Some(bucket_row) = self.bucket_row(bucket)? else {
            return Ok(Err(ObjectError::NoSuchBucket));
        };
        if !bucket_row.object_lock {
            return Ok(Err(ObjectError::ObjectLockNotEnabled));
        }
        let (version, null_version, _) = match self.find_live(bucket, key, which)? {
            Ok(found) => found,
            Err(err) => return Ok(Err(err)),
        };
        Ok(Ok((version, null_version, self.lock_of(version)?)))
    }

    /// Changes the retention of the version `which` picks (S3
    /// PutObjectRetention). An active COMPLIANCE retention may only be kept
    /// or extended in the same mode; an active GOVERNANCE retention likewise,
    /// unless `bypass`. Kept means its date never moves earlier: a variable
    /// retention's new date is at least the current one, and a fixed one
    /// earlier than that is refused. Returns the version's number and
    /// whether it is the null version.
    pub fn put_retention(
        &mut self,
        bucket: &str,
        key: &str,
        which: VersionSel,
        change: RetentionChange,
        bypass: bool,
        now_ms: u64,
    ) -> Result<Result<(u64, bool), ObjectError>, StorageError> {
        let (version, null_version, mut lock) = match self.lockable(bucket, key, which)? {
            Ok(found) => found,
            Err(err) => return Ok(Err(err)),
        };
        let mut retention = match change {
            RetentionChange::Set(retention) => Some(retention),
            RetentionChange::Remove => None,
            RetentionChange::Release { mode, until_ms } => {
                let held = lock.retention.and_then(|current| {
                    current
                        .event
                        .filter(|event| event.on)
                        .map(|event| (current, event))
                });
                match held {
                    Some((current, event)) => Some(Retention {
                        mode,
                        until_ms: until_ms.max(current.effective_until(now_ms)),
                        event: Some(EventHold { on: false, ..event }),
                    }),
                    None if until_ms > 0 => Some(Retention {
                        mode,
                        until_ms,
                        event: None,
                    }),
                    None => {
                        return Ok(Err(ObjectError::InvalidRetention(
                            "the object version has no event hold to release",
                        )));
                    }
                }
            }
        };
        if let Some(current) = lock.retention {
            let floor = current.effective_until(now_ms);
            let bypassed = bypass && current.mode == RetentionMode::Governance;
            if floor > now_ms && !bypassed {
                match &mut retention {
                    Some(new) if new.mode == current.mode => match new.event {
                        Some(event) if event.on => new.until_ms = new.until_ms.max(floor),
                        _ if new.until_ms < floor => return Ok(Err(ObjectError::Locked)),
                        _ => {}
                    },
                    _ => return Ok(Err(ObjectError::Locked)),
                }
            }
        }
        lock.retention = retention;
        self.set_lock(version, &lock)?;
        Ok(Ok((version, null_version)))
    }

    /// Places or releases a legal hold on the version `which` picks (S3
    /// PutObjectLegalHold). Returns the version's number and whether it is
    /// the null version.
    pub fn put_legal_hold(
        &mut self,
        bucket: &str,
        key: &str,
        which: VersionSel,
        on: bool,
    ) -> Result<Result<(u64, bool), ObjectError>, StorageError> {
        let (version, null_version, mut lock) = match self.lockable(bucket, key, which)? {
            Ok(found) => found,
            Err(err) => return Ok(Err(err)),
        };
        lock.legal_hold = Some(on);
        self.set_lock(version, &lock)?;
        Ok(Ok((version, null_version)))
    }
}

impl ObjFile<'_> {
    /// The Object Lock settings of a version (none when it has none).
    pub fn object_lock(&mut self, version: u64) -> Result<ObjectLock, StorageError> {
        let Some(locks) = self.existing_tree(LOCKS_TREE)? else {
            return Ok(ObjectLock::default());
        };
        locks
            .get(&mut self.core.page_ctx(0), &version.to_be_bytes())?
            .map_or(Ok(ObjectLock::default()), |row| ObjectLock::decode(&row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locks_round_trip_and_block_deletes_as_their_mode_says() {
        let governance = ObjectLock {
            retention: Some(Retention {
                mode: RetentionMode::Governance,
                until_ms: 1_000,
                event: None,
            }),
            legal_hold: None,
        };
        for lock in [
            governance,
            ObjectLock {
                retention: Some(Retention {
                    mode: RetentionMode::Compliance,
                    until_ms: u64::MAX,
                    event: None,
                }),
                legal_hold: Some(false),
            },
            ObjectLock {
                retention: None,
                legal_hold: Some(true),
            },
        ] {
            assert_eq!(ObjectLock::decode(&lock.encode()).unwrap(), lock);
        }
        let variable = ObjectLock {
            retention: Some(Retention {
                mode: RetentionMode::Compliance,
                until_ms: 0,
                event: Some(EventHold {
                    on: true,
                    years: true,
                    count: 7,
                }),
            }),
            legal_hold: None,
        };
        assert_eq!(variable.encode().len(), 16);
        assert_eq!(ObjectLock::decode(&variable.encode()).unwrap(), variable);
        let mut bad = variable.encode();
        bad[10] = 3;
        assert!(ObjectLock::decode(&bad).is_err());
        assert!(ObjectLock::decode(&variable.encode()[..12]).is_err());
        assert!(ObjectLock::decode(&[3; 10]).is_err());
        assert!(ObjectLock::decode(&[0; 9]).is_err());
        assert!(governance.blocks_delete(999, false));
        assert!(!governance.blocks_delete(999, true));
        assert!(!governance.blocks_delete(1_000, false), "expired");
        let compliance = ObjectLock {
            retention: Some(Retention {
                mode: RetentionMode::Compliance,
                until_ms: 1_000,
                event: None,
            }),
            legal_hold: None,
        };
        assert!(compliance.blocks_delete(999, true));
        let held = ObjectLock {
            legal_hold: Some(true),
            ..governance
        };
        assert!(
            held.blocks_delete(5_000, true),
            "a legal hold is never bypassed"
        );
        let released = ObjectLock {
            legal_hold: Some(false),
            ..ObjectLock::default()
        };
        assert!(!released.blocks_delete(0, false));
    }
}
