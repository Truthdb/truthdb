//! Bucket replication on this server (S3 Replication). A bucket's
//! replication configuration names rules: each, while enabled, replicates
//! the new versions of the keys under its prefix to its destination bucket.
//! A version written in the bucket is matched against the rules in the same
//! transaction: for each destination, the rule that applies (the highest
//! priority of those to it whose filter names the version) queues its
//! replication there, and the version is PENDING. A runner beside the S3
//! listener takes the queue in order and writes each replica: the same
//! version number and time, attributes, tags and lock, the body shared (see
//! `insert_replica`). The source's status tallies its destinations: FAILED
//! once one cannot take it, PENDING until every one has it, then COMPLETED;
//! the replica is REPLICA.
//! A replica is never queued itself. A change to a queued or replicated
//! version's tags is queued for its replica too. A change to its lock is
//! not: the replica's lock is the source's when it is written, and then the
//! destination's own (merging two locks would either weaken one or lock
//! beyond both).
//!
//! A version's status is kept in the replication status tree under its row
//! key in the objects tree; it goes with the version. A version in an
//! archive class (GLACIER, DEEP_ARCHIVE) is not replicated, as S3 does not
//! replicate one until it is restored and copied.
//!
//! A configuration is replicated as the one who put it (the account, and
//! its user), as S3 replicates as the role the configuration names. A
//! configuration stored as XML by an earlier build of this release is read
//! as none until the runner stores it anew (see `truthdb-s3`'s
//! `replication_run`).

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
/// The configuration's stored form: 1 has no owner override nor
/// destination owner, 2 filters by prefix only, 3 has tag filters and
/// replica modification sync.
const FORMAT: u8 = 3;

/// A bucket's replication configuration (as PutBucketReplication sets it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// The role S3 would assume; kept as given (there are no roles here).
    pub role: String,
    pub rules: Vec<ReplicationRule>,
    /// Who put it: the account and its user (none for the account's root);
    /// replicas are written as them.
    pub account: String,
    pub user: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationRule {
    /// Empty when the rule has none.
    pub id: String,
    pub priority: u32,
    pub enabled: bool,
    /// The versions it applies to.
    pub filter: ReplicationFilter,
    /// `SourceSelectionCriteria` `ReplicaModifications` `Enabled`: a tag
    /// change of a replica in this bucket is sent back to the bucket its
    /// rule names.
    pub replica_modifications: bool,
    pub destination: ReplicationDestination,
}

/// The versions a rule applies to, as its `Filter` names them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationFilter {
    /// An empty Filter: every version.
    All,
    /// Those of keys under the prefix.
    Prefix(String),
    /// Those with the tag in their tag set.
    Tag(String, String),
    /// Those under the prefix (if any) with every one of the tags.
    And {
        prefix: Option<String>,
        tags: Vec<(String, String)>,
    },
}

impl ReplicationFilter {
    /// Whether a version of `key` is one this filter names, its tags left
    /// aside (its prefix, if any).
    pub fn names_key(&self, key: &str) -> bool {
        match self {
            ReplicationFilter::Prefix(prefix)
            | ReplicationFilter::And {
                prefix: Some(prefix),
                ..
            } => key.starts_with(prefix.as_str()),
            _ => true,
        }
    }

    /// Whether a version of `key` with `tags` is one this filter names.
    pub fn matches(&self, key: &str, tags: &[(String, String)]) -> bool {
        let tagged = |name: &str, value: &str| {
            tags.iter()
                .any(|(tag, tag_value)| tag == name && tag_value == value)
        };
        match self {
            ReplicationFilter::All => true,
            ReplicationFilter::Prefix(prefix) => key.starts_with(prefix.as_str()),
            ReplicationFilter::Tag(name, value) => tagged(name, value),
            ReplicationFilter::And { prefix, tags } => {
                prefix
                    .as_deref()
                    .is_none_or(|prefix| key.starts_with(prefix))
                    && tags.iter().all(|(name, value)| tagged(name, value))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationDestination {
    pub bucket: String,
    pub account: Option<String>,
    /// The replicas' class (none: the source version's).
    pub storage_class: Option<StorageClass>,
    /// `AccessControlTranslation` `Owner` `Destination`: the replicas are
    /// the destination owner's (as every object is under the bucket owner
    /// enforced ownership a bucket here has; kept to be answered).
    pub owner_override: bool,
    /// The owner of the destination (a bucket of this server) when the
    /// configuration was put: replicas go there only while it has that
    /// owner (a bucket deleted and made again by another account takes
    /// none). None for a bucket on another service, and in a configuration
    /// stored before it was kept (its destination must be the source
    /// owner's).
    pub owner: Option<String>,
}

impl ReplicationConfig {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(FORMAT);
        out.str(&self.role);
        out.str(&self.account);
        match &self.user {
            None => out.u8(0),
            Some(user) => {
                out.u8(1);
                out.str(user);
            }
        }
        out.u32(self.rules.len() as u32);
        for rule in &self.rules {
            out.str(&rule.id);
            out.u32(rule.priority);
            out.u8(rule.enabled as u8);
            match &rule.filter {
                ReplicationFilter::All => out.u8(0),
                ReplicationFilter::Prefix(prefix) => {
                    out.u8(1);
                    out.str(prefix);
                }
                ReplicationFilter::Tag(name, value) => {
                    out.u8(2);
                    out.str(name);
                    out.str(value);
                }
                ReplicationFilter::And { prefix, tags } => {
                    out.u8(3);
                    match prefix {
                        None => out.u8(0),
                        Some(prefix) => {
                            out.u8(1);
                            out.str(prefix);
                        }
                    }
                    out.u32(tags.len() as u32);
                    for (name, value) in tags {
                        out.str(name);
                        out.str(value);
                    }
                }
            }
            out.u8(rule.replica_modifications as u8);
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
            out.u8(destination.owner_override as u8);
            match &destination.owner {
                None => out.u8(0),
                Some(owner) => {
                    out.u8(1);
                    out.str(owner);
                }
            }
        }
        out.0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "replication configuration");
        let format = input.u8()?;
        if !(1..=FORMAT).contains(&format) {
            return Err(input.corrupt());
        }
        let role = input.str()?;
        let account = input.str()?;
        let user = input.present()?.then(|| input.str()).transpose()?;
        let mut rules = Vec::new();
        for _ in 0..input.count()? {
            let id = input.str()?;
            let priority = input.u32()?;
            let enabled = input.present()?;
            let filter = match format {
                1 | 2 => match input.present()? {
                    true => ReplicationFilter::Prefix(input.str()?),
                    false => ReplicationFilter::All,
                },
                _ => match input.u8()? {
                    0 => ReplicationFilter::All,
                    1 => ReplicationFilter::Prefix(input.str()?),
                    2 => ReplicationFilter::Tag(input.str()?, input.str()?),
                    3 => {
                        let prefix = input.present()?.then(|| input.str()).transpose()?;
                        let mut tags = Vec::new();
                        for _ in 0..input.count()? {
                            tags.push((input.str()?, input.str()?));
                        }
                        ReplicationFilter::And { prefix, tags }
                    }
                    _ => return Err(input.corrupt()),
                },
            };
            let replica_modifications = format >= 3 && input.present()?;
            let bucket = input.str()?;
            let account = input.present()?.then(|| input.str()).transpose()?;
            let storage_class = match input.u8()? {
                0 => None,
                code @ 1..=8 => Some(StorageClass::from_code(code - 1)),
                _ => return Err(input.corrupt()),
            };
            let owner_override = format >= 2 && input.present()?;
            let owner = match format >= 2 && input.present()? {
                true => Some(input.str()?),
                false => None,
            };
            rules.push(ReplicationRule {
                id,
                priority,
                enabled,
                filter,
                replica_modifications,
                destination: ReplicationDestination {
                    bucket,
                    account,
                    storage_class,
                    owner_override,
                    owner,
                },
            });
        }
        if !input.rest().is_empty() {
            return Err(input.corrupt());
        }
        Ok(ReplicationConfig {
            role,
            rules,
            account,
            user,
        })
    }

    /// A stored configuration; none for one an earlier build stored as XML.
    fn decode_stored(bytes: &[u8]) -> Result<Option<Self>, StorageError> {
        match bytes.first() {
            Some(b'<') => Ok(None),
            _ => Self::decode(bytes).map(Some),
        }
    }

    /// The rule that replicates a version of `key` with `tags` to bucket
    /// `destination`: of the enabled rules naming it whose filter names the
    /// version, the one of highest priority (the first listed of those of
    /// equal priority). Priority settles only between rules of the same
    /// destination, as S3's does.
    pub fn rule_to(
        &self,
        key: &str,
        tags: &[(String, String)],
        destination: &str,
    ) -> Option<&ReplicationRule> {
        self.rules
            .iter()
            .filter(|rule| {
                rule.enabled
                    && rule.destination.bucket == destination
                    && rule.filter.matches(key, tags)
            })
            .fold(None, |best: Option<&ReplicationRule>, rule| match best {
                Some(best) if best.priority >= rule.priority => Some(best),
                _ => Some(rule),
            })
    }

    /// The rules that replicate a version of `key` with `tags`: one for each
    /// destination (see [`Self::rule_to`]), in the order they are first
    /// named.
    pub fn rules_for(&self, key: &str, tags: &[(String, String)]) -> Vec<&ReplicationRule> {
        let mut destinations: Vec<&str> = Vec::new();
        for rule in &self.rules {
            if !destinations.contains(&rule.destination.bucket.as_str()) {
                destinations.push(&rule.destination.bucket);
            }
        }
        destinations
            .into_iter()
            .filter_map(|destination| self.rule_to(key, tags, destination))
            .collect()
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

    fn from_row(row: &[u8]) -> Result<Option<Self>, StorageError> {
        Ok(match StatusRow::decode(row)? {
            StatusRow::Replica => Some(ReplicationStatus::Replica),
            StatusRow::Source(tally) => tally.status(),
        })
    }
}

/// A source version's replications: to how many of its destinations each
/// is yet to be made, was made, or failed. As S3 answers it, the version is
/// FAILED once one failed, PENDING until every one is made, then COMPLETED.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Tally {
    pending: u16,
    completed: u16,
    failed: u16,
}

impl Tally {
    fn status(self) -> Option<ReplicationStatus> {
        if self.failed > 0 {
            Some(ReplicationStatus::Failed)
        } else if self.pending > 0 {
            Some(ReplicationStatus::Pending)
        } else if self.completed > 0 {
            Some(ReplicationStatus::Completed)
        } else {
            None
        }
    }
}

/// A status row: a replica's, or a source version's tally. A row of one
/// byte (1 PENDING, 2 COMPLETED, 3 FAILED) was written for a single
/// destination; 4 is a replica; 0x10 heads a tally.
enum StatusRow {
    Replica,
    Source(Tally),
}

const TALLY_ROW: u8 = 0x10;

impl StatusRow {
    fn decode(row: &[u8]) -> Result<Self, StorageError> {
        let one = |pending, completed, failed| {
            StatusRow::Source(Tally {
                pending,
                completed,
                failed,
            })
        };
        Ok(match row {
            [1] => one(1, 0, 0),
            [2] => one(0, 1, 0),
            [3] => one(0, 0, 1),
            [4] => StatusRow::Replica,
            [TALLY_ROW, p0, p1, c0, c1, f0, f1] => one(
                u16::from_le_bytes([*p0, *p1]),
                u16::from_le_bytes([*c0, *c1]),
                u16::from_le_bytes([*f0, *f1]),
            ),
            _ => return Err(corrupt("replication status")),
        })
    }

    fn encode(&self) -> Vec<u8> {
        match self {
            StatusRow::Replica => vec![4],
            StatusRow::Source(tally) => {
                let mut out = vec![TALLY_ROW];
                for count in [tally.pending, tally.completed, tally.failed] {
                    out.extend_from_slice(&count.to_le_bytes());
                }
                out
            }
        }
    }
}

/// What a queued replication does to the replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationWork {
    /// Writes it.
    Create,
    /// Gives it the source's tags as they are now.
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
        Ok(self
            .bucket_config_in(bucket, BUCKET_REPLICATION, "")?
            .map(|bytes| ReplicationConfig::decode_stored(&bytes))
            .transpose()?
            .flatten())
    }

    /// The replication status of version `version` of `bucket`'s `key`.
    pub fn replication_status_of(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<Option<ReplicationStatus>, StorageError> {
        Ok(self
            .status_row(bucket, key, version)?
            .and_then(|row| match row {
                StatusRow::Replica => Some(ReplicationStatus::Replica),
                StatusRow::Source(tally) => tally.status(),
            }))
    }

    fn status_row(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<Option<StatusRow>, StorageError> {
        self.trees
            .replication_status()
            .get(self.ctx, &version_key(bucket, key, version))?
            .map(|row| StatusRow::decode(&row))
            .transpose()
    }

    fn set_status_row(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        row: &StatusRow,
    ) -> Result<(), StorageError> {
        let tree = self.trees.replication_status();
        let row_key = version_key(bucket, key, version);
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, &row_key)?;
        tree.insert_unique(self.ctx, mode, &row_key, &row.encode())?;
        Ok(())
    }

    /// Changes the tally of the source version of `queued` by `change`, if
    /// it has one; a tally left empty goes.
    fn count_replication(
        &mut self,
        queued: &QueuedReplication,
        change: impl FnOnce(&mut Tally),
    ) -> Result<(), StorageError> {
        let (bucket, key, version) = (&queued.bucket, &queued.key, queued.version);
        let Some(StatusRow::Source(mut tally)) = self.status_row(bucket, key, version)? else {
            return Ok(());
        };
        change(&mut tally);
        match tally == Tally::default() {
            true => self.forget_replication_status(bucket, key, version),
            false => self.set_status_row(bucket, key, version, &StatusRow::Source(tally)),
        }
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
        let rules = config.rules_for(key, &info.attrs.tags);
        if rules.is_empty() || info.storage_class.is_archived() {
            return Ok(());
        }
        let tally = Tally {
            pending: rules.len() as u16,
            ..Tally::default()
        };
        self.set_status_row(bucket, key, info.version, &StatusRow::Source(tally))?;
        for rule in rules {
            self.enqueue(QueuedReplication {
                number: 0,
                work: ReplicationWork::Create,
                bucket: bucket.to_string(),
                key: key.to_string(),
                version: info.version,
                destination: rule.destination.bucket.clone(),
                storage_class: rule.destination.storage_class,
            })?;
        }
        Ok(())
    }

    /// Queues a change of a version's tags (now `tags`) for its replica,
    /// if it is queued or replicated (not one that failed) and a rule still
    /// applies to it; or, for a replica, back to the bucket its rule names
    /// when the rule enables replica modification sync.
    pub(crate) fn queue_metadata_replication(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        tags: &[(String, String)],
    ) -> Result<(), StorageError> {
        // A source version sends to the destinations holding (or about to
        // hold) its replica, whatever another destination did: one may
        // have failed while others have it.
        let row = self.status_row(bucket, key, version)?;
        let Some(config) = self.replication_config(bucket)? else {
            return Ok(());
        };
        for rule in config.rules_for(key, tags) {
            let sent = match &row {
                Some(StatusRow::Source(tally)) => tally.pending > 0 || tally.completed > 0,
                Some(StatusRow::Replica) => rule.replica_modifications,
                None => false,
            };
            if sent {
                self.enqueue(QueuedReplication {
                    number: 0,
                    work: ReplicationWork::Metadata,
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version,
                    destination: rule.destination.bucket.clone(),
                    storage_class: None,
                })?;
            }
        }
        Ok(())
    }

    /// Takes queued replication `number` off the queue, counting it in its
    /// source version's tally as `status` says (COMPLETED or FAILED), if
    /// given and the version has a tally.
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
        let Some(status) = status else {
            return Ok(());
        };
        self.count_replication(queued, |tally| {
            tally.pending = tally.pending.saturating_sub(1);
            match status {
                ReplicationStatus::Failed => tally.failed = tally.failed.saturating_add(1),
                _ => tally.completed = tally.completed.saturating_add(1),
            }
        })
    }

    /// Takes queued replication `number` off the queue unmade, as no rule
    /// sends its version there any more: it no longer counts in its
    /// version's tally (a version no destination is left for has no
    /// status).
    pub fn drop_replication(&mut self, queued: &QueuedReplication) -> Result<(), StorageError> {
        self.trees.replication_queue().delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &queued.number.to_be_bytes(),
        )?;
        if queued.work != ReplicationWork::Create {
            return Ok(());
        }
        self.count_replication(queued, |tally| {
            tally.pending = tally.pending.saturating_sub(1);
        })
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
        self.set_status_row(bucket, key, source.version, &StatusRow::Replica)?;
        Ok(Ok(true))
    }
}

impl ObjFile<'_> {
    /// The bucket's replication configuration, if it has one.
    pub fn replication(&mut self, bucket: &str) -> Result<Option<ReplicationConfig>, StorageError> {
        Ok(self
            .bucket_config(bucket, BUCKET_REPLICATION, "")?
            .map(|bytes| ReplicationConfig::decode_stored(&bytes))
            .transpose()?
            .flatten())
    }

    /// The bucket's replication configuration as an earlier build stored it
    /// (XML), if it is stored so.
    pub fn replication_xml(&mut self, bucket: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self
            .bucket_config(bucket, BUCKET_REPLICATION, "")?
            .filter(|bytes| bytes.first() == Some(&b'<')))
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
        .map(Option::flatten)
    }

    /// Up to `limit` queued replications, in order, those numbered after
    /// `after` (none: from the first).
    pub fn replication_queue(
        &mut self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<QueuedReplication>, StorageError> {
        let Some(tree) = self.existing_tree(REPLICATION_QUEUE_TREE)? else {
            return Ok(Vec::new());
        };
        let lower = after.map(|number| (number.saturating_add(1)).to_be_bytes());
        if after == Some(u64::MAX) {
            return Ok(Vec::new());
        }
        tree.scan_range_limit(
            &mut self.core.page_ctx(0),
            lower.as_ref().map(|key| &key[..]),
            None,
            limit,
        )?
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
