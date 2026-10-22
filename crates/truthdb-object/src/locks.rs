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

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::objects::{ObjectError, VersionSel, validate_key};
use crate::{LOCKS_TREE, ObjFile, ObjTxn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionMode {
    Governance,
    Compliance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    pub mode: RetentionMode,
    pub until_ms: u64,
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
                retention.until_ms > now_ms
                    && !(retention.mode == RetentionMode::Governance && bypass)
            })
    }

    /// `[mode u8: 0 none, 1 GOVERNANCE, 2 COMPLIANCE][retain-until ms u64]
    /// [legal hold u8: 0 never set, 1 off, 2 on]`.
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(10);
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
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let corrupt = || StorageError::InvalidFile("corrupt object lock".to_string());
        if bytes.len() != 10 {
            return Err(corrupt());
        }
        let until_ms = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let retention = match bytes[0] {
            0 => None,
            1 => Some(RetentionMode::Governance),
            2 => Some(RetentionMode::Compliance),
            _ => return Err(corrupt()),
        }
        .map(|mode| Retention { mode, until_ms });
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
        let (version, null_version) = match self.find_live(bucket, key, which)? {
            Ok(found) => found,
            Err(err) => return Ok(Err(err)),
        };
        Ok(Ok((version, null_version, self.lock_of(version)?)))
    }

    /// Sets the retention of the version `which` picks (S3
    /// PutObjectRetention; `None` removes it). An active COMPLIANCE retention
    /// may only be extended in the same mode; an active GOVERNANCE retention
    /// likewise, unless `bypass`. Returns the version's number and whether
    /// it is the null version.
    pub fn put_retention(
        &mut self,
        bucket: &str,
        key: &str,
        which: VersionSel,
        retention: Option<Retention>,
        bypass: bool,
        now_ms: u64,
    ) -> Result<Result<(u64, bool), ObjectError>, StorageError> {
        let (version, null_version, mut lock) = match self.lockable(bucket, key, which)? {
            Ok(found) => found,
            Err(err) => return Ok(Err(err)),
        };
        if let Some(current) = lock.retention
            && current.until_ms > now_ms
        {
            let extends = retention
                .is_some_and(|new| new.mode == current.mode && new.until_ms >= current.until_ms);
            let bypassed = bypass && current.mode == RetentionMode::Governance;
            if !extends && !bypassed {
                return Ok(Err(ObjectError::Locked));
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
            }),
            legal_hold: None,
        };
        for lock in [
            governance,
            ObjectLock {
                retention: Some(Retention {
                    mode: RetentionMode::Compliance,
                    until_ms: u64::MAX,
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
        assert!(ObjectLock::decode(&[3; 10]).is_err());
        assert!(ObjectLock::decode(&[0; 9]).is_err());
        assert!(governance.blocks_delete(999, false));
        assert!(!governance.blocks_delete(999, true));
        assert!(!governance.blocks_delete(1_000, false), "expired");
        let compliance = ObjectLock {
            retention: Some(Retention {
                mode: RetentionMode::Compliance,
                until_ms: 1_000,
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
