//! Bucket replication on this server (S3 Replication). A bucket's
//! replication configuration names rules: each, while enabled, replicates
//! the new versions of the keys under its prefix to its destination bucket.
//! A version written in the bucket is matched against the rules in the same
//! transaction: the rule that applies (the highest priority of those whose
//! prefix the key has) marks it PENDING and queues its replication. A
//! runner beside the S3 listener takes the queue in order and writes each
//! replica: the same version number and time, attributes, tags and lock,
//! the body shared (see `insert_replica`). Then the source is COMPLETED, or
//! FAILED when the destination cannot take it, and the replica is REPLICA.
//! A replica is never queued itself. A change to a queued or replicated
//! version's tags or lock is queued for its replica too.
//!
//! A version's status is kept in the replication status tree under its row
//! key in the objects tree; it goes with the version.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::{Reader, Writer, corrupt};
use crate::documents::BUCKET_REPLICATION;
use crate::objects::{NewVersion, ObjectError, ObjectInfo, version_key};
use crate::{
    ObjFile, ObjTxn, ObjectAttrs, ObjectLock, REPLICATION_QUEUE_TREE, REPLICATION_STATUS_TREE,
    StorageClass, Versioning, next_counter,
};

/// META key of the next queue entry's number.
const NEXT_REPLICATION_KEY: &[u8] = b"next_replication";
const FORMAT: u8 = 1;

/// A bucket's replication configuration (as PutBucketReplication sets it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// The role S3 would assume; kept as given (there are no roles here).
    pub role: String,
    pub rules: Vec<ReplicationRule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationRule {
    /// Empty when the rule has none.
    pub id: String,
    pub priority: u32,
    pub enabled: bool,
    /// The keys it applies to: those under this prefix (none: every key).
    pub prefix: Option<String>,
    pub destination: ReplicationDestination,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationDestination {
    pub bucket: String,
    pub account: Option<String>,
    /// The replicas' class (none: the source version's).
    pub storage_class: Option<StorageClass>,
}

impl ReplicationConfig {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(FORMAT);
        out.str(&self.role);
        out.u32(self.rules.len() as u32);
        for rule in &self.rules {
            out.str(&rule.id);
            out.u32(rule.priority);
            out.u8(rule.enabled as u8);
            match &rule.prefix {
                None => out.u8(0),
                Some(prefix) => {
                    out.u8(1);
                    out.str(prefix);
                }
            }
            let destination = &rule.destination;
            out.str(&destination.bucket);
            match &destination.account {
                None => out.u8(0),
                Some(account) => {
                    out.u8(1);
                    out.str(account);
                }
            }
            out.u8(destination
                .storage_class
                .map_or(0, |class| 1 + class.code()));
        }
        out.0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "replication configuration");
        if input.u8()? != FORMAT {
            return Err(input.corrupt());
        }
        let role = input.str()?;
        let mut rules = Vec::new();
        for _ in 0..input.count()? {
            let id = input.str()?;
            let priority = input.u32()?;
            let enabled = input.present()?;
            let prefix = input.present()?.then(|| input.str()).transpose()?;
            let bucket = input.str()?;
            let account = input.present()?.then(|| input.str()).transpose()?;
            let storage_class = match input.u8()? {
                0 => None,
                code @ 1..=8 => Some(StorageClass::from_code(code - 1)),
                _ => return Err(input.corrupt()),
            };
            rules.push(ReplicationRule {
                id,
                priority,
                enabled,
                prefix,
                destination: ReplicationDestination {
                    bucket,
                    account,
                    storage_class,
                },
            });
        }
        if !input.rest().is_empty() {
            return Err(input.corrupt());
        }
        Ok(ReplicationConfig { role, rules })
    }

    /// The rule that replicates a new version of `key`: of the enabled rules
    /// whose prefix it has, the one of highest priority (the first listed of
    /// those of equal priority).
    pub fn rule_for(&self, key: &str) -> Option<&ReplicationRule> {
        self.rules
            .iter()
            .filter(|rule| {
                rule.enabled
                    && rule
                        .prefix
                        .as_deref()
                        .is_none_or(|prefix| key.starts_with(prefix))
            })
            .fold(None, |best: Option<&ReplicationRule>, rule| match best {
                Some(best) if best.priority >= rule.priority => Some(best),
                _ => Some(rule),
            })
    }
}

/// A version's replication status, as `x-amz-replication-status` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationStatus {
    Pending,
    Completed,
    Failed,
    Replica,
}

impl ReplicationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplicationStatus::Pending => "PENDING",
            ReplicationStatus::Completed => "COMPLETED",
            ReplicationStatus::Failed => "FAILED",
            ReplicationStatus::Replica => "REPLICA",
        }
    }

    fn code(self) -> u8 {
        match self {
            ReplicationStatus::Pending => 1,
            ReplicationStatus::Completed => 2,
            ReplicationStatus::Failed => 3,
            ReplicationStatus::Replica => 4,
        }
    }

    fn from_row(row: &[u8]) -> Result<Self, StorageError> {
        Ok(match row {
            [1] => ReplicationStatus::Pending,
            [2] => ReplicationStatus::Completed,
            [3] => ReplicationStatus::Failed,
            [4] => ReplicationStatus::Replica,
            _ => return Err(corrupt("replication status")),
        })
    }
}

/// What a queued replication does to the replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationWork {
    /// Writes it.
    Create,
    /// Gives it the source's tags and lock as they are now.
    Metadata,
}

/// One queued replication of a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedReplication {
    /// Its place in the queue.
    pub number: u64,
    pub work: ReplicationWork,
    pub bucket: String,
    pub key: String,
    pub version: u64,
    pub destination: String,
    /// The replica's class (none: the source version's).
    pub storage_class: Option<StorageClass>,
}

impl QueuedReplication {
    fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(match self.work {
            ReplicationWork::Create => 0,
            ReplicationWork::Metadata => 1,
        });
        out.str(&self.bucket);
        out.str(&self.key);
        out.u64(self.version);
        out.str(&self.destination);
        out.u8(self.storage_class.map_or(0, |class| 1 + class.code()));
        out.0
    }

    fn decode(number: u64, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "queued replication");
        let work = match input.u8()? {
            0 => ReplicationWork::Create,
            1 => ReplicationWork::Metadata,
            _ => return Err(input.corrupt()),
        };
        let queued = QueuedReplication {
            number,
            work,
            bucket: input.str()?,
            key: input.str()?,
            version: input.u64()?,
            destination: input.str()?,
            storage_class: match input.u8()? {
                0 => None,
                code @ 1..=8 => Some(StorageClass::from_code(code - 1)),
                _ => return Err(input.corrupt()),
            },
        };
        if !input.rest().is_empty() {
            return Err(input.corrupt());
        }
        Ok(queued)
    }
}

impl ObjTxn<'_, '_> {
    /// Sets the bucket's replication configuration. The caller checks the
    /// bucket exists.
    pub fn put_replication(
        &mut self,
        bucket: &str,
        config: &ReplicationConfig,
    ) -> Result<(), StorageError> {
        self.put_bucket_config(bucket, BUCKET_REPLICATION, "", &config.encode())
    }

    /// Deletes the bucket's replication configuration; whether it had one.
    pub fn delete_replication(&mut self, bucket: &str) -> Result<bool, StorageError> {
        self.delete_bucket_config(bucket, BUCKET_REPLICATION, "")
    }

    fn replication_config(
        &mut self,
        bucket: &str,
    ) -> Result<Option<ReplicationConfig>, StorageError> {
        self.bucket_config_in(bucket, BUCKET_REPLICATION, "")?
            .map(|bytes| ReplicationConfig::decode(&bytes))
            .transpose()
    }

    /// The replication status of version `version` of `bucket`'s `key`.
    pub fn replication_status_of(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<Option<ReplicationStatus>, StorageError> {
        self.trees
            .replication_status()
            .get(self.ctx, &version_key(bucket, key, version))?
            .map(|row| ReplicationStatus::from_row(&row))
            .transpose()
    }

    fn set_replication_status(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        status: ReplicationStatus,
    ) -> Result<(), StorageError> {
        let tree = self.trees.replication_status();
        let row_key = version_key(bucket, key, version);
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, &row_key)?;
        tree.insert_unique(self.ctx, mode, &row_key, &[status.code()])?;
        Ok(())
    }

    /// Forgets the replication status of a version being removed.
    pub(crate) fn forget_replication_status(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<(), StorageError> {
        self.trees.replication_status().delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &version_key(bucket, key, version),
        )?;
        Ok(())
    }

    fn enqueue(&mut self, mut queued: QueuedReplication) -> Result<(), StorageError> {
        queued.number = next_counter(self, NEXT_REPLICATION_KEY)?;
        self.trees.replication_queue().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &queued.number.to_be_bytes(),
            &queued.encode(),
        )?;
        Ok(())
    }

    /// Queues the replication of `info`, just written in `bucket` as `key`,
    /// if a rule of the bucket's configuration applies to it: it is then
    /// PENDING.
    pub(crate) fn queue_replication(
        &mut self,
        bucket: &str,
        key: &str,
        info: &ObjectInfo,
    ) -> Result<(), StorageError> {
        let Some(config) = self.replication_config(bucket)? else {
            return Ok(());
        };
        let Some(rule) = config.rule_for(key) else {
            return Ok(());
        };
        let queued = QueuedReplication {
            number: 0,
            work: ReplicationWork::Create,
            bucket: bucket.to_string(),
            key: key.to_string(),
            version: info.version,
            destination: rule.destination.bucket.clone(),
            storage_class: rule.destination.storage_class,
        };
        self.set_replication_status(bucket, key, info.version, ReplicationStatus::Pending)?;
        self.enqueue(queued)
    }

    /// Queues a change of a version's tags or lock for its replica, if it
    /// is queued or replicated (not a replica, nor one that failed) and a
    /// rule still applies to it.
    pub(crate) fn queue_metadata_replication(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<(), StorageError> {
        if !matches!(
            self.replication_status_of(bucket, key, version)?,
            Some(ReplicationStatus::Pending | ReplicationStatus::Completed)
        ) {
            return Ok(());
        }
        let Some(config) = self.replication_config(bucket)? else {
            return Ok(());
        };
        let Some(rule) = config.rule_for(key) else {
            return Ok(());
        };
        let queued = QueuedReplication {
            number: 0,
            work: ReplicationWork::Metadata,
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            destination: rule.destination.bucket.clone(),
            storage_class: None,
        };
        self.enqueue(queued)
    }

    /// Takes queued replication `number` off the queue, setting its source
    /// version's status to `status` if given (and the version is there).
    pub fn finish_replication(
        &mut self,
        queued: &QueuedReplication,
        status: Option<ReplicationStatus>,
    ) -> Result<(), StorageError> {
        self.trees.replication_queue().delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &queued.number.to_be_bytes(),
        )?;
        if let Some(status) = status
            && self
                .replication_status_of(&queued.bucket, &queued.key, queued.version)?
                .is_some()
        {
            self.set_replication_status(&queued.bucket, &queued.key, queued.version, status)?;
        }
        Ok(())
    }

    /// Writes the replica of `source` in `bucket` as `key`: the same version
    /// number and time, with `attrs`, in `class`, locked by `lock`, sharing
    /// the source's body; its status is REPLICA. The bucket must have
    /// versioning enabled (the caller checks it). Returns whether it was
    /// written (false: the replica is there already).
    #[allow(clippy::too_many_arguments)]
    pub fn insert_replica(
        &mut self,
        bucket: &str,
        key: &str,
        source: &ObjectInfo,
        attrs: &ObjectAttrs,
        class: StorageClass,
        lock: &ObjectLock,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<Result<bool, ObjectError>, StorageError> {
        let Some(bucket_row) = self.bucket_row(bucket)? else {
            return Ok(Err(ObjectError::NoSuchBucket));
        };
        if bucket_row.versioning != Versioning::Enabled {
            return Err(StorageError::InvalidFile(
                "a replica is written only in a bucket with versioning enabled".to_string(),
            ));
        }
        if self
            .trees
            .objects()
            .get(self.ctx, &version_key(bucket, key, source.version))?
            .is_some()
        {
            return Ok(Ok(false));
        }
        self.add_ref(source.body)?;
        self.insert_version_as(
            bucket,
            key,
            Versioning::Enabled,
            NewVersion {
                body: source.body,
                size: source.size,
                md5: source.md5,
                parts: source.parts,
                storage_class: class,
            },
            attrs,
            Some((source.version, source.mtime_ms)),
            now_ms,
            grace_ms,
        )?;
        self.set_lock(bucket, key, source.version, lock)?;
        self.set_replication_status(bucket, key, source.version, ReplicationStatus::Replica)?;
        Ok(Ok(true))
    }
}

impl ObjFile<'_> {
    /// The bucket's replication configuration, if it has one.
    pub fn replication(&mut self, bucket: &str) -> Result<Option<ReplicationConfig>, StorageError> {
        self.bucket_config(bucket, BUCKET_REPLICATION, "")?
            .map(|bytes| ReplicationConfig::decode(&bytes))
            .transpose()
    }

    /// The replication status of version `version` of `bucket`'s `key`.
    pub fn replication_status(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<Option<ReplicationStatus>, StorageError> {
        let Some(tree) = self.existing_tree(REPLICATION_STATUS_TREE)? else {
            return Ok(None);
        };
        tree.get(
            &mut self.core.page_ctx(0),
            &version_key(bucket, key, version),
        )?
        .map(|row| ReplicationStatus::from_row(&row))
        .transpose()
    }

    /// Up to `limit` queued replications, in order.
    pub fn replication_queue(
        &mut self,
        limit: usize,
    ) -> Result<Vec<QueuedReplication>, StorageError> {
        let Some(tree) = self.existing_tree(REPLICATION_QUEUE_TREE)? else {
            return Ok(Vec::new());
        };
        tree.scan_range_limit(&mut self.core.page_ctx(0), None, None, limit)?
            .into_iter()
            .map(|(key, row)| {
                let number = u64::from_be_bytes(
                    key.as_slice()
                        .try_into()
                        .map_err(|_| corrupt("replication queue key"))?,
                );
                QueuedReplication::decode(number, &row)
            })
            .collect()
    }
}
