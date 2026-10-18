//! Buckets: one flat namespace of names (AWS general-purpose naming rules),
//! each with its creation time and versioning state. A bucket is deleted only
//! when it holds no object key.

use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

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
    pub created_ms: u64,
    pub versioning: Versioning,
}

/// Why a bucket operation was refused (the storage itself worked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BucketError {
    NoSuchBucket,
    AlreadyOwnedByYou,
    NotEmpty,
    InvalidName(&'static str),
}

/// A bucket's stored row: creation time and versioning state.
pub(crate) struct BucketRow {
    pub created_ms: u64,
    pub versioning: Versioning,
}

impl BucketRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.created_ms.to_le_bytes().to_vec();
        out.push(match self.versioning {
            Versioning::Unversioned => 0,
            Versioning::Enabled => 1,
            Versioning::Suspended => 2,
        });
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let corrupt = || StorageError::InvalidFile("corrupt bucket row".to_string());
        if bytes.len() != 9 {
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
    pub fn create_bucket(
        &mut self,
        name: &str,
        now_ms: u64,
    ) -> Result<Result<(), BucketError>, StorageError> {
        if let Err(err) = validate_bucket_name(name) {
            return Ok(Err(err));
        }
        let row = BucketRow {
            created_ms: now_ms,
            versioning: Versioning::Unversioned,
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
            TreeInsert::DuplicateKey => Err(BucketError::AlreadyOwnedByYou),
        })
    }

    /// Sets the bucket's versioning (S3 PutBucketVersioning) to `Enabled` or
    /// `Suspended`: a bucket that had versioning never returns to
    /// unversioned.
    pub fn set_versioning(
        &mut self,
        name: &str,
        versioning: Versioning,
    ) -> Result<Result<(), BucketError>, StorageError> {
        debug_assert_ne!(versioning, Versioning::Unversioned);
        let Some(mut row) = self.bucket_row(name)? else {
            return Ok(Err(BucketError::NoSuchBucket));
        };
        row.versioning = versioning;
        self.trees.buckets().update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            name.as_bytes(),
            &row.encode(),
        )?;
        Ok(Ok(()))
    }

    pub fn delete_bucket(&mut self, name: &str) -> Result<Result<(), BucketError>, StorageError> {
        let buckets = self.trees.buckets();
        if buckets.get(self.ctx, name.as_bytes())?.is_none() {
            return Ok(Err(BucketError::NoSuchBucket));
        }
        let (lower, upper) = bucket_object_range(name);
        let objects = self.trees.objects();
        if !objects
            .scan_range_limit(self.ctx, Some(&lower), Some(&upper), 1)?
            .is_empty()
        {
            return Ok(Err(BucketError::NotEmpty));
        }
        buckets.delete(self.ctx, &mut OpMode::Txn(self.txn), name.as_bytes())?;
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
                    created_ms: row.created_ms,
                    versioning: row.versioning,
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
                    created_ms: row.created_ms,
                    versioning: row.versioning,
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
