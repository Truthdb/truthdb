//! Buckets: one flat namespace of names (AWS general-purpose naming rules),
//! each with its owning account, creation time, versioning state and
//! whether Object Lock is enabled. A bucket is deleted only when it holds no
//! object key and no multipart upload.

use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::accounts::DEFAULT_ACCOUNT;
use crate::{BUCKETS_TREE, ObjFile, ObjTxn};

/// A bucket's versioning state (S3 `PutBucketVersioning`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Versioning {
    Unversioned,
    Enabled,
    Suspended,
}

/// A bucket as listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketInfo {
    pub name: String,
    /// The account that owns the bucket.
    pub owner: String,
    pub created_ms: u64,
    pub versioning: Versioning,
    /// The change journal's last sequence number when the bucket was
    /// created: its rows numbered after this are this bucket's (a bucket
    /// deleted and created again does not inherit the old one's history).
    pub journal_from: u64,
    /// Whether Object Lock is enabled (see `locks`): versioning then stays
    /// enabled.
    pub object_lock: bool,
}

/// Why a bucket operation was refused (the storage itself worked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketError {
    NoSuchBucket,
    AlreadyOwnedByYou,
    /// Another account owns a bucket of that name.
    AlreadyExists,
    NotEmpty,
    InvalidName(&'static str),
    /// The bucket's state does not allow the change (Object Lock and
    /// versioning), and why.
    InvalidBucketState(&'static str),
}

/// A bucket's stored row: creation time, versioning state, where its journal
/// starts, its owning account, and whether Object Lock is enabled.
pub(crate) struct BucketRow {
    pub created_ms: u64,
    pub versioning: Versioning,
    pub journal_from: u64,
    pub owner: String,
    pub object_lock: bool,
}

impl BucketRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.created_ms.to_le_bytes().to_vec();
        out.push(match self.versioning {
            Versioning::Unversioned => 0,
            Versioning::Enabled => 1,
            Versioning::Suspended => 2,
        });
        out.extend_from_slice(&self.journal_from.to_le_bytes());
        out.extend_from_slice(self.owner.as_bytes());
        // Only a bucket with Object Lock has the byte: the others stay
        // readable by a release from before it.
        if self.object_lock {
            out.push(1);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let corrupt = || StorageError::InvalidFile("corrupt bucket row".to_string());
        // A row written before the journal has no journal start: all of the
        // journal is the bucket's. A row written before accounts has no owner:
        // the default account owns it. Only a row with Object Lock enabled
        // has its flag byte.
        if ![9, 17, 29, 30].contains(&bytes.len()) {
            return Err(corrupt());
        }
        Ok(BucketRow {
            created_ms: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            versioning: match bytes[8] {
                0 => Versioning::Unversioned,
                1 => Versioning::Enabled,
                2 => Versioning::Suspended,
                _ => return Err(corrupt()),
            },
            journal_from: match bytes.get(9..17) {
                Some(from) => u64::from_le_bytes(from.try_into().unwrap()),
                None => 0,
            },
            owner: match bytes.get(17..29) {
                Some(owner) => String::from_utf8(owner.to_vec()).map_err(|_| corrupt())?,
                None => DEFAULT_ACCOUNT.to_string(),
            },
            object_lock: match bytes.get(29) {
                None => false,
                Some(1) => true,
                Some(_) => return Err(corrupt()),
            },
        })
    }
}

/// AWS's general-purpose bucket naming rules.
pub fn validate_bucket_name(name: &str) -> Result<(), BucketError> {
    let bytes = name.as_bytes();
    if !(3..=63).contains(&bytes.len()) {
        return Err(BucketError::InvalidName("must be 3 to 63 characters long"));
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'.' || *b == b'-')
    {
        return Err(BucketError::InvalidName(
            "may contain only lowercase letters, digits, dots and hyphens",
        ));
    }
    let edge = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    if !edge(bytes[0]) || !edge(bytes[bytes.len() - 1]) {
        return Err(BucketError::InvalidName(
            "must begin and end with a letter or digit",
        ));
    }
    if name.contains("..") {
        return Err(BucketError::InvalidName(
            "must not contain two adjacent dots",
        ));
    }
    if name.parse::<std::net::Ipv4Addr>().is_ok() {
        return Err(BucketError::InvalidName(
            "must not be formatted as an IP address",
        ));
    }
    for prefix in ["xn--", "sthree-", "amzn-s3-demo-"] {
        if name.starts_with(prefix) {
            return Err(BucketError::InvalidName("uses a reserved prefix"));
        }
    }
    for suffix in ["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"] {
        if name.ends_with(suffix) {
            return Err(BucketError::InvalidName("uses a reserved suffix"));
        }
    }
    Ok(())
}

/// The first key of `bucket`'s objects and the first key past them: object
/// keys start with the bucket name and a 0 byte (bucket names have none).
pub(crate) fn bucket_object_range(bucket: &str) -> (Vec<u8>, Vec<u8>) {
    let mut lower = bucket.as_bytes().to_vec();
    lower.push(0);
    let mut upper = bucket.as_bytes().to_vec();
    upper.push(1);
    (lower, upper)
}

impl ObjTxn<'_, '_> {
    /// Creates bucket `name`, owned by account `owner`.
    pub fn create_bucket(
        &mut self,
        name: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<Result<(), BucketError>, StorageError> {
        if let Err(err) = validate_bucket_name(name) {
            return Ok(Err(err));
        }
        let row = BucketRow {
            created_ms: now_ms,
            versioning: Versioning::Unversioned,
            journal_from: self.last_change()?,
            owner: owner.to_string(),
            object_lock: false,
        };
        let buckets = self.trees.buckets();
        let inserted = buckets.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            name.as_bytes(),
            &row.encode(),
        )?;
        Ok(match inserted {
            TreeInsert::Inserted => Ok(()),
            TreeInsert::DuplicateKey => match self.bucket_row(name)? {
                Some(existing) if existing.owner != owner => Err(BucketError::AlreadyExists),
                _ => Err(BucketError::AlreadyOwnedByYou),
            },
        })
    }

    /// Sets the bucket's versioning (S3 PutBucketVersioning) to `Enabled` or
    /// `Suspended`: a bucket that had versioning never returns to
    /// unversioned, and one with Object Lock keeps it enabled.
    pub fn set_versioning(
        &mut self,
        name: &str,
        versioning: Versioning,
    ) -> Result<Result<(), BucketError>, StorageError> {
        debug_assert_ne!(versioning, Versioning::Unversioned);
        let Some(mut row) = self.bucket_row(name)? else {
            return Ok(Err(BucketError::NoSuchBucket));
        };
        if row.object_lock && versioning != Versioning::Enabled {
            return Ok(Err(BucketError::InvalidBucketState(
                "Object Lock is enabled on the bucket, so its versioning cannot be suspended",
            )));
        }
        row.versioning = versioning;
        self.trees.buckets().update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            name.as_bytes(),
            &row.encode(),
        )?;
        Ok(Ok(()))
    }

    /// Enables Object Lock on the bucket, which must have versioning
    /// enabled; it cannot be disabled again.
    pub fn enable_object_lock(
        &mut self,
        name: &str,
    ) -> Result<Result<(), BucketError>, StorageError> {
        let Some(mut row) = self.bucket_row(name)? else {
            return Ok(Err(BucketError::NoSuchBucket));
        };
        if row.versioning != Versioning::Enabled {
            return Ok(Err(BucketError::InvalidBucketState(
                "Versioning must be enabled on the bucket to enable Object Lock",
            )));
        }
        if !row.object_lock {
            row.object_lock = true;
            self.trees.buckets().update(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                name.as_bytes(),
                &row.encode(),
            )?;
        }
        Ok(Ok(()))
    }

    pub fn delete_bucket(&mut self, name: &str) -> Result<Result<(), BucketError>, StorageError> {
        let buckets = self.trees.buckets();
        if buckets.get(self.ctx, name.as_bytes())?.is_none() {
            return Ok(Err(BucketError::NoSuchBucket));
        }
        // Object versions and multipart uploads both key on the bucket name
        // first: either one keeps the bucket.
        let (lower, upper) = bucket_object_range(name);
        for tree in [self.trees.objects(), self.trees.upload_index()] {
            if !tree
                .scan_range_limit(self.ctx, Some(&lower), Some(&upper), 1)?
                .is_empty()
            {
                return Ok(Err(BucketError::NotEmpty));
            }
        }
        buckets.delete(self.ctx, &mut OpMode::Txn(self.txn), name.as_bytes())?;
        self.delete_bucket_configs(name)?;
        self.drop_fulltext(name)?;
        Ok(Ok(()))
    }
}

impl ObjFile<'_> {
    /// The bucket, if it exists.
    pub fn bucket(&mut self, name: &str) -> Result<Option<BucketInfo>, StorageError> {
        let Some(buckets) = self.existing_tree(BUCKETS_TREE)? else {
            return Ok(None);
        };
        let mut ctx = self.core.page_ctx(0);
        buckets
            .get(&mut ctx, name.as_bytes())?
            .map(|row| {
                let row = BucketRow::decode(&row)?;
                Ok(BucketInfo {
                    name: name.to_string(),
                    owner: row.owner,
                    created_ms: row.created_ms,
                    versioning: row.versioning,
                    journal_from: row.journal_from,
                    object_lock: row.object_lock,
                })
            })
            .transpose()
    }

    /// Every bucket, by name.
    pub fn list_buckets(&mut self) -> Result<Vec<BucketInfo>, StorageError> {
        let Some(buckets) = self.existing_tree(BUCKETS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut ctx = self.core.page_ctx(0);
        buckets
            .scan(&mut ctx)?
            .into_iter()
            .map(|(name, row)| {
                let row = BucketRow::decode(&row)?;
                Ok(BucketInfo {
                    name: String::from_utf8(name).map_err(|_| {
                        StorageError::InvalidFile("corrupt bucket name".to_string())
                    })?,
                    owner: row.owner,
                    created_ms: row.created_ms,
                    versioning: row.versioning,
                    journal_from: row.journal_from,
                    object_lock: row.object_lock,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_names_follow_the_aws_rules() {
        for good in ["abc", "my-bucket.2024", "a".repeat(63).as_str()] {
            assert_eq!(validate_bucket_name(good), Ok(()), "{good}");
        }
        for bad in [
            "ab",
            &"a".repeat(64),
            "My-Bucket",
            "my_bucket",
            "-bucket",
            "bucket.",
            "my..bucket",
            "192.168.5.4",
            "xn--bucket",
            "bucket-s3alias",
        ] {
            assert!(validate_bucket_name(bad).is_err(), "{bad}");
        }
    }
}
