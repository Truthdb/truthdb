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
    /// A directory bucket's zone id (its name's `--<zone>--x-s3`); none for
    /// a general purpose bucket.
    pub zone: Option<String>,
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
/// starts, its owning account, whether Object Lock is enabled, and a
/// directory bucket's zone.
pub(crate) struct BucketRow {
    pub created_ms: u64,
    pub versioning: Versioning,
    pub journal_from: u64,
    pub owner: String,
    pub object_lock: bool,
    pub zone: Option<String>,
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
        // Only a bucket with Object Lock (1), or a directory bucket (2, then
        // its zone), has the byte: the others stay readable by a release
        // from before it. A directory bucket has no Object Lock.
        if self.object_lock {
            out.push(1);
        } else if let Some(zone) = &self.zone {
            out.push(2);
            out.extend_from_slice(zone.as_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let corrupt = || StorageError::InvalidFile("corrupt bucket row".to_string());
        // A row written before the journal has no journal start: all of the
        // journal is the bucket's. A row written before accounts has no owner:
        // the default account owns it. Only a row with Object Lock enabled,
        // or of a directory bucket, has its flag byte.
        if ![9, 17, 29].contains(&bytes.len()) && bytes.len() < 30 {
            return Err(corrupt());
        }
        let (object_lock, zone) = match bytes.get(29) {
            None => (false, None),
            Some(1) if bytes.len() == 30 => (true, None),
            Some(2) if bytes.len() > 30 => (
                false,
                Some(String::from_utf8(bytes[30..].to_vec()).map_err(|_| corrupt())?),
            ),
            Some(_) => return Err(corrupt()),
        };
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
            object_lock,
            zone,
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

/// AWS's directory bucket naming rules: `<base>--<zone>--x-s3`, 3 to 63
/// characters in all, of lowercase letters, digits and hyphens, beginning
/// and ending with a letter or digit, and no reserved prefix. The zone id.
pub fn validate_directory_bucket_name(name: &str) -> Result<&str, BucketError> {
    let bytes = name.as_bytes();
    if !(3..=63).contains(&bytes.len()) {
        return Err(BucketError::InvalidName("must be 3 to 63 characters long"));
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
    {
        return Err(BucketError::InvalidName(
            "may contain only lowercase letters, digits and hyphens",
        ));
    }
    if !bytes[0].is_ascii_alphanumeric() {
        return Err(BucketError::InvalidName(
            "must begin with a letter or digit",
        ));
    }
    for prefix in ["xn--", "sthree-", "amzn-s3-demo-"] {
        if name.starts_with(prefix) {
            return Err(BucketError::InvalidName("uses a reserved prefix"));
        }
    }
    let unsuffixed = name.strip_suffix("--x-s3").ok_or(BucketError::InvalidName(
        "a directory bucket's name ends with --<zone-id>--x-s3",
    ))?;
    match unsuffixed.rsplit_once("--") {
        Some((base, zone))
            if !base.is_empty()
                && !zone.is_empty()
                && base.as_bytes()[base.len() - 1].is_ascii_alphanumeric()
                && zone.as_bytes()[0].is_ascii_alphanumeric()
                && zone.as_bytes()[zone.len() - 1].is_ascii_alphanumeric() =>
        {
            Ok(zone)
        }
        _ => Err(BucketError::InvalidName(
            "a directory bucket's name is <base-name>--<zone-id>--x-s3",
        )),
    }
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
        self.insert_bucket(name, owner, None, now_ms)
    }

    /// Creates directory bucket `name` (its name's zone is the one given:
    /// the caller checks they agree), owned by account `owner`.
    pub fn create_directory_bucket(
        &mut self,
        name: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<Result<(), BucketError>, StorageError> {
        let zone = match validate_directory_bucket_name(name) {
            Ok(zone) => zone.to_string(),
            Err(err) => return Ok(Err(err)),
        };
        self.insert_bucket(name, owner, Some(zone), now_ms)
    }

    fn insert_bucket(
        &mut self,
        name: &str,
        owner: &str,
        zone: Option<String>,
        now_ms: u64,
    ) -> Result<Result<(), BucketError>, StorageError> {
        let row = BucketRow {
            created_ms: now_ms,
            versioning: Versioning::Unversioned,
            journal_from: self.last_change()?,
            owner: owner.to_string(),
            object_lock: false,
            zone,
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
        if row.zone.is_some() {
            return Ok(Err(BucketError::InvalidBucketState(
                "A directory bucket has no versioning",
            )));
        }
        if row.object_lock && versioning != Versioning::Enabled {
            return Ok(Err(BucketError::InvalidBucketState(
                "Object Lock is enabled on the bucket, so its versioning cannot be suspended",
            )));
        }
        if versioning != Versioning::Enabled
            && self.has_bucket_config(name, crate::BUCKET_REPLICATION, "")?
        {
            return Ok(Err(BucketError::InvalidBucketState(
                "Replication is configured on the bucket, so its versioning cannot be suspended",
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
        // Its changes not yet sent are sent as configured when made.
        self.end_notifications(name)?;
        self.delete_bucket_configs(name)?;
        self.forget_pass(name)?;
        self.drop_fulltext(name)?;
        self.forget_usage(name)?;
        self.forget_metrics(name)?;
        self.forget_inventories(name)?;
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
                    zone: row.zone,
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
                    zone: row.zone,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_bucket_names_follow_the_aws_rules() {
        assert_eq!(
            validate_directory_bucket_name("data--use1-az4--x-s3"),
            Ok("use1-az4")
        );
        assert_eq!(
            validate_directory_bucket_name("a-b--usw2-az1--x-s3"),
            Ok("usw2-az1")
        );
        for bad in [
            "data",
            "data--x-s3",
            "--use1-az4--x-s3",
            "data---use1-az4--x-s3",
            "Data--use1-az4--x-s3",
            "da.ta--use1-az4--x-s3",
            "-data--use1-az4--x-s3",
            "xn--a--use1-az4--x-s3",
            &format!("{}--use1-az4--x-s3", "a".repeat(48)),
        ] {
            assert!(validate_directory_bucket_name(bad).is_err(), "{bad}");
        }
        // Not a general purpose bucket's.
        assert!(validate_bucket_name("data--use1-az4--x-s3").is_err());
    }

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
