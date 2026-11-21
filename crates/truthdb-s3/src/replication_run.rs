//! The replication runner: a task beside the S3 listener writing the
//! replicas the object engine queues (see `truthdb_object::ReplicationConfig`),
//! in order. A replica keeps its source's version number, time, attributes
//! and tags, and its lock (or, with no retention of its own, the
//! destination's default retention); it is in the rule's storage class, or
//! the source's. Its body is the source's, shared.
//!
//! Each replication is checked when it is made, not only when the
//! configuration was put. A rule of the source's configuration must still
//! send the version there (or it is not replicated, and has no status). The
//! destination must still exist, be the source owner's, have versioning
//! enabled, and have Object Lock when the source has it, and the one who put
//! the configuration must be allowed `s3:GetObjectVersionForReplication` on
//! the source version and `s3:ReplicateObject` there. Otherwise the source
//! version is FAILED: S3 retries it no more (Batch Replication does). A
//! change of the source's tags gives the replica the same tags; a change of
//! its lock does not reach the replica, whose lock is the destination's own
//! once written. Replicas are
//! written only once every version's state is keyed by its bucket and key
//! (see `truthdb_object::ObjFile::versions_keyed`): a replica's number is
//! its source's. A standby writes none (its log is the primary's).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use tokio::sync::watch;
use truthdb_core::storage::StorageError;
use truthdb_object::{
    BucketInfo, ObjFile, ObjectInfo, ObjectLock, Origin, QueuedReplication, RemoteBucket,
    ReplicationConfig, ReplicationStatus, ReplicationWork, VersionSel, Versioning,
};

use crate::authz::{Authz, Resource};
use crate::batch_run::object;
use crate::objects::{DROP_GRACE_MS, now_ms};
use crate::server::{Principal, S3State};

/// How long the runner waits when the queue is empty, or after an error.
const IDLE: Duration = Duration::from_secs(1);
/// Queued replications made a lock hold.
const BATCH: usize = 64;
/// Queue entries a step looks through for ones to make (those for a remote
/// bucket being waited for are passed over).
const SCAN: usize = 1024;
/// A replica sent to another service larger than this goes in parts of
/// this size (held in memory one at a time).
const PART: usize = 8 * 1024 * 1024;
/// The longest a remote bucket that failed for a while is waited for.
const MAX_WAIT: Duration = Duration::from_secs(300);

/// Remote buckets whose last replica could not be sent for a while: when
/// they are tried again, and their wait (doubling from a second).
type Waits = HashMap<String, (Instant, Duration)>;

/// A replica to send to another S3 service, its source's body leased.
struct RemoteJob {
    queued: QueuedReplication,
    remote: RemoteBucket,
    body: u64,
    size: u64,
    layout: Vec<(u64, u64, u64)>,
    headers: Vec<(String, String)>,
}

/// Writes replicas until `shutdown` turns true.
pub async fn run(state: Arc<S3State>, mut shutdown: watch::Receiver<bool>) {
    let mut stored_anew = false;
    let mut runner = Runner::default();
    loop {
        if *shutdown.borrow() {
            return;
        }
        if !stored_anew && !state.storage.is_standby() {
            stored_anew = store_xml_anew(&state)
                .await
                .inspect_err(|err| eprintln!("replication: {err}"))
                .is_ok();
        }
        let more = stored_anew
            && !state.storage.is_standby()
            && step(&state, &mut runner)
                .await
                .inspect_err(|err| eprintln!("replication: {err}"))
                .unwrap_or(false);
        if !more {
            tokio::select! {
                _ = tokio::time::sleep(IDLE) => {}
                _ = shutdown.changed() => {}
            }
        }
    }
}

/// Stores anew the configurations an earlier build stored as XML, as put
/// by their bucket's owner (until then they replicate nothing).
async fn store_xml_anew(state: &S3State) -> Result<(), String> {
    object(state, store_anew).await
}

/// [`store_xml_anew`] in `file`.
/// A bucket whose configuration cannot be read or stored is passed over
/// (and said so), the others stored.
fn store_anew(file: &mut ObjFile<'_>) -> Result<(), StorageError> {
    for bucket in file.list_buckets()? {
        let stored = (|| {
            let Some(xml) = file.replication_xml(&bucket.name)? else {
                return Ok(());
            };
            match crate::replication_config::parse(&xml) {
                Ok(mut config) => {
                    config.account = bucket.owner.clone();
                    file.transaction(|tx| tx.put_replication(&bucket.name, &config))
                }
                Err(err) => {
                    eprintln!(
                        "replication: the configuration of {} cannot be read: {}",
                        bucket.name, err.message
                    );
                    Ok(())
                }
            }
        })();
        if let Err(err) = stored {
            eprintln!(
                "replication: the configuration of {} cannot be stored anew: {err}",
                bucket.name
            );
        }
    }
    Ok(())
}

/// Why a replica was not sent.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Fail {
    /// The service refused it, and would again: the source is FAILED.
    Lasting(String),
    /// The service could not be reached or answered with a passing error:
    /// it stays PENDING, the bucket is waited for, as long as it takes.
    Passing(String),
    /// The source's body could not be read whole: it stays PENDING, until
    /// it happened [`MAX_TRIES`] times.
    Unreadable(String),
    /// Not tried: an older replica for the same bucket is not sent yet.
    Skipped,
}

/// What the runner keeps between steps: the remote buckets being waited
/// for; where the next step looks in the queue; the remote buckets held,
/// each with the first queued replication of theirs left queued (theirs
/// are sent in the queue's order, so no later one goes before it); and how
/// often each queued replication's body could not be read.
#[derive(Default)]
struct Runner {
    waits: Waits,
    after: Option<u64>,
    held: HashMap<String, u64>,
    tries: HashMap<u64, u32>,
    /// Where the queue was read up to when it was last read again from a
    /// released bucket's first entry: no bucket is released again before
    /// it is reached, so the reading always gets further.
    rewound_from: Option<u64>,
}

/// How often a replication's body may fail to be read before it is FAILED.
const MAX_TRIES: u32 = 10;

impl Runner {
    /// Makes remote bucket `name` wait (doubling from [`IDLE`]).
    fn wait(&mut self, name: &str) {
        let wait = self
            .waits
            .get(name)
            .map_or(IDLE, |(_, wait)| (*wait * 2).clamp(IDLE, MAX_WAIT));
        self.waits
            .insert(name.to_string(), (Instant::now() + wait, wait));
    }

    /// Holds remote bucket `name` from queued replication `number` on.
    fn hold(&mut self, name: &str, number: u64) {
        let first = self.held.entry(name.to_string()).or_insert(number);
        *first = (*first).min(number);
    }

    /// The remote buckets held for this step: those being waited for, and
    /// those held before and still waited for. A bucket no longer waited
    /// for is let go, and the queue is read again from its first entry
    /// left queued: every entry between it and where the step would go on
    /// is finished, or a held bucket's (passed over again).
    fn held_now(&mut self) -> HashSet<String> {
        if self.after.is_none() {
            self.held.clear();
            self.rewound_from = None;
        }
        let now = Instant::now();
        let waited: HashSet<String> = self
            .waits
            .iter()
            .filter(|(_, (until, _))| *until > now)
            .map(|(name, _)| name.clone())
            .collect();
        // Not before the last reading again has caught up.
        let caught_up = match (self.rewound_from, self.after) {
            (Some(from), Some(after)) => after > from,
            _ => true,
        };
        let released: Vec<(String, u64)> = match caught_up {
            true => self
                .held
                .iter()
                .filter(|(name, _)| !waited.contains(*name))
                .map(|(name, first)| (name.clone(), *first))
                .collect(),
            false => Vec::new(),
        };
        let from = self.after;
        for (name, first) in released {
            self.held.remove(&name);
            let back = first.saturating_sub(1);
            self.after = Some(self.after.map_or(back, |after| after.min(back)));
        }
        if self.after != from {
            self.rewound_from = from;
        }
        let mut held = waited;
        held.extend(self.held.keys().cloned());
        held
    }

    /// Notes how sending queued replication `number` to remote bucket
    /// `name` went, and returns how it is to be finished. One left queued
    /// holds the bucket for the rest of the pass; a passing failure makes
    /// the bucket wait (doubling); a body not read [`MAX_TRIES`] times is
    /// FAILED.
    fn record(&mut self, name: &str, number: u64, outcome: Result<(), Fail>) -> Result<(), Fail> {
        match outcome {
            Ok(()) | Err(Fail::Lasting(_)) => {
                self.waits.remove(name);
                self.tries.remove(&number);
                outcome
            }
            Err(Fail::Passing(why)) => {
                self.hold(name, number);
                self.wait(name);
                Err(Fail::Passing(why))
            }
            Err(Fail::Unreadable(why)) => {
                let tries = self.tries.entry(number).or_default();
                *tries += 1;
                if *tries >= MAX_TRIES {
                    self.tries.remove(&number);
                    return Err(Fail::Lasting(format!("{why} (tried {MAX_TRIES} times)")));
                }
                self.hold(name, number);
                self.wait(name);
                Err(Fail::Unreadable(why))
            }
            Err(Fail::Skipped) => {
                self.hold(name, number);
                Err(Fail::Skipped)
            }
        }
    }
}

/// The size of the parts a replica of `size` bytes is sent in: [`PART`],
/// or larger, as S3 takes at most 10,000 parts.
fn part_size(size: u64) -> usize {
    PART.max(size.div_ceil(10_000) as usize)
}

/// Makes the next queued replications; returns whether more may be queued.
/// Those in this server are made in one lock hold; those for another
/// service are prepared there (their bodies leased), sent without the lock,
/// and finished in a hold each. A step goes on in the queue where the last
/// one stopped, and from its start once it reaches the end (a new pass),
/// so the work for a remote bucket being waited for holds no other back.
async fn step(state: &S3State, runner: &mut Runner) -> Result<bool, String> {
    let held = runner.held_now();
    let after = runner.after;
    let (jobs, made, next, passed) = object(state, move |file| {
        if !file.versions_keyed()? {
            return Ok((Vec::new(), 0, after, HashMap::new()));
        }
        let mut jobs = Vec::new();
        let mut passed = HashMap::new();
        match prepare(file, after, &held, &mut jobs, &mut passed) {
            Ok((made, next)) => Ok((jobs, made, next, passed)),
            Err(err) => {
                // Nothing prepared is sent: its leases go.
                for job in &jobs {
                    file.release_lease(job.body);
                }
                Err(err)
            }
        }
    })
    .await?;
    runner.after = next;
    for (name, first) in passed {
        runner.hold(&name, first);
    }
    let mut failed = None;
    for job in jobs {
        let name = job.remote.name.clone();
        let number = job.queued.number;
        // After one of the bucket's was left queued, its later ones wait.
        let outcome = match runner.held.contains_key(&name) {
            true => Err(Fail::Skipped),
            false => send(state, &job).await,
        };
        let outcome = runner.record(&name, number, outcome);
        // Every job is finished (its lease released), whatever one did. One
        // not finished is still queued: its bucket is held.
        if let Err(err) = object(state, move |file| finish_remote(file, &job, outcome)).await {
            runner.hold(&name, number);
            runner.wait(&name);
            failed.get_or_insert(err);
        }
    }
    match failed {
        Some(err) => Err(err),
        // A full scan with nothing to make goes on at once.
        None => Ok(made == BATCH || next.is_some()),
    }
}

/// Prepares the queued replications after `after`, making those in this
/// server, up to [`BATCH`], those for a remote bucket in `held` passed
/// over (the first of each noted in `passed`); returns how many were made
/// or prepared, and where the next step goes on (none: from the start).
fn prepare(
    file: &mut ObjFile<'_>,
    after: Option<u64>,
    held: &HashSet<String>,
    jobs: &mut Vec<RemoteJob>,
    passed: &mut HashMap<String, u64>,
) -> Result<(usize, Option<u64>), StorageError> {
    let queue = file.replication_queue(after, SCAN)?;
    let mut next = if queue.len() < SCAN {
        None
    } else {
        queue.last().map(|queued| queued.number)
    };
    let mut made = 0;
    for queued in &queue {
        if made == BATCH {
            next = Some(queued.number - 1);
            break;
        }
        if held.contains(&queued.destination) {
            passed
                .entry(queued.destination.clone())
                .or_insert(queued.number);
            continue;
        }
        made += 1;
        if let Some(job) = replicate(file, queued, now_ms())? {
            jobs.push(job);
        }
    }
    Ok((made, next))
}

/// Sends `job`'s replica: in one PUT, or in parts past [`PART`].
async fn send(state: &S3State, job: &RemoteJob) -> Result<(), Fail> {
    let remote = &job.remote;
    let client = truthdb_s3client::Client::new(truthdb_s3client::Config {
        endpoint: remote.endpoint.clone(),
        region: remote.region.clone(),
        access_key: remote.access_key.clone(),
        secret: remote.secret.clone(),
        ca_pem: remote.ca_pem.clone(),
    })
    .map_err(service)?;
    let (tx, mut rx) = mpsc::channel(4);
    let engine = state.engine.clone();
    let (layout, size) = (job.layout.clone(), job.size);
    let reader = tokio::spawn(async move {
        crate::objects::stream_plaintext(&engine, &layout, None, 0, size, &tx).await
    });
    // However the send ends, the reader ends with it (before the body's
    // lease is released).
    let _reader_ends = StopOnDrop(reader.abort_handle());
    // S3 takes at most 10,000 parts: a larger object takes larger parts.
    let part_size = part_size(size);
    let (bucket, key) = (&remote.name, &job.queued.key);
    let headers: Vec<(&str, String)> = job
        .headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.clone()))
        .collect();
    let mut buffer = BytesMut::new();
    if job.size as usize <= PART {
        while let Some(chunk) = rx.recv().await {
            buffer.extend_from_slice(&chunk);
        }
        read_whole(reader, buffer.len() as u64, size).await?;
        client
            .put_object(bucket, key, &headers, buffer.freeze())
            .await
            .map(|_| ())
            .map_err(service)
    } else {
        let upload = client
            .create_multipart_upload(bucket, key, &headers)
            .await
            .map_err(service)?;
        let mut parts: Vec<(u32, String)> = Vec::new();
        let mut read = 0u64;
        let parted = async {
            loop {
                let chunk = rx.recv().await;
                if let Some(chunk) = &chunk {
                    read += chunk.len() as u64;
                    buffer.extend_from_slice(chunk);
                }
                while buffer.len() >= part_size || (chunk.is_none() && !buffer.is_empty()) {
                    let take = buffer.len().min(part_size);
                    let part: Bytes = buffer.split_to(take).freeze();
                    let number = parts.len() as u32 + 1;
                    let etag = client
                        .upload_part(bucket, key, &upload, number, part)
                        .await
                        .map_err(service)?;
                    parts.push((number, etag));
                }
                if chunk.is_none() {
                    break;
                }
            }
            read_whole(reader, read, size).await?;
            client
                .complete_multipart_upload(bucket, key, &upload, &parts)
                .await
                .map(|_| ())
                .map_err(service)
        }
        .await;
        if parted.is_err() {
            // Best effort: the service's own cleanup takes what is left.
            let _ = client.abort_multipart_upload(bucket, key, &upload).await;
        }
        parted
    }
}

/// A service's error, as a reason a replica was not sent.
fn service(err: truthdb_s3client::Error) -> Fail {
    match err.permanent() {
        true => Fail::Lasting(err.to_string()),
        false => Fail::Passing(err.to_string()),
    }
}

/// Stops a task when dropped.
struct StopOnDrop(tokio::task::AbortHandle);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Whether the reader of a replica's body read all `size` bytes (`read`):
/// a body that could not be read is sent again later.
async fn read_whole(
    reader: tokio::task::JoinHandle<bool>,
    read: u64,
    size: u64,
) -> Result<(), Fail> {
    match reader.await {
        Ok(true) if read == size => Ok(()),
        _ => Err(Fail::Unreadable(
            "the source's body could not be read whole".to_string(),
        )),
    }
}

/// Releases `job`'s lease and takes it off the queue as `outcome` says:
/// sent, COMPLETED; refused for good, FAILED; else left to be sent again.
fn finish_remote(
    file: &mut ObjFile<'_>,
    job: &RemoteJob,
    outcome: Result<(), Fail>,
) -> Result<(), StorageError> {
    file.release_lease(job.body);
    let queued = &job.queued;
    let why = match &outcome {
        Ok(()) | Err(Fail::Skipped) => None,
        Err(Fail::Lasting(why) | Fail::Passing(why) | Fail::Unreadable(why)) => Some(why),
    };
    if let Some(why) = why {
        eprintln!(
            "replication: {}/{} version {} was not sent to {}: {why}",
            queued.bucket, queued.key, queued.version, queued.destination
        );
    }
    match outcome {
        Ok(()) => {
            file.transaction(|tx| tx.finish_replication(queued, Some(ReplicationStatus::Completed)))
        }
        Err(Fail::Lasting(_)) => {
            file.transaction(|tx| tx.finish_replication(queued, Some(ReplicationStatus::Failed)))
        }
        Err(_) => Ok(()),
    }
}

/// The headers of the PUT that sends `source`'s replica: its system
/// headers, user metadata and tags, the class, and the lock it has.
fn remote_headers(
    source: &ObjectInfo,
    class: truthdb_object::StorageClass,
    lock: &ObjectLock,
    now: u64,
) -> Vec<(String, String)> {
    let attrs = &source.attrs;
    let mut headers: Vec<(String, String)> = Vec::new();
    for (name, value) in [
        ("content-type", &attrs.content_type),
        ("cache-control", &attrs.cache_control),
        ("content-disposition", &attrs.content_disposition),
        ("content-encoding", &attrs.content_encoding),
        ("content-language", &attrs.content_language),
        ("expires", &attrs.expires),
        ("x-amz-website-redirect-location", &attrs.website_redirect),
    ] {
        if let Some(value) = value {
            headers.push((name.to_string(), value.clone()));
        }
    }
    for (name, value) in &attrs.user {
        headers.push((format!("x-amz-meta-{name}"), value.clone()));
    }
    if !attrs.tags.is_empty() {
        // RFC 3986's unreserved characters are left as they are.
        const ESCAPED: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
            .remove(b'-')
            .remove(b'_')
            .remove(b'.')
            .remove(b'~');
        let encode = |text: &str| percent_encoding::utf8_percent_encode(text, ESCAPED).to_string();
        let tagging: Vec<String> = attrs
            .tags
            .iter()
            .map(|(name, value)| format!("{}={}", encode(name), encode(value)))
            .collect();
        headers.push(("x-amz-tagging".to_string(), tagging.join("&")));
    }
    headers.push((
        "x-amz-storage-class".to_string(),
        class.as_str().to_string(),
    ));
    // An expired retention is not sent (S3 refuses a date past).
    if let Some(retention) = lock
        .retention
        .filter(|retention| retention.effective_until(now) > now)
    {
        headers.push((
            "x-amz-object-lock-mode".to_string(),
            match retention.mode {
                truthdb_object::RetentionMode::Governance => "GOVERNANCE",
                truthdb_object::RetentionMode::Compliance => "COMPLIANCE",
            }
            .to_string(),
        ));
        headers.push((
            "x-amz-object-lock-retain-until-date".to_string(),
            truthdb_object::time::iso8601(retention.effective_until(now)),
        ));
    }
    if lock.legal_hold == Some(true) {
        headers.push(("x-amz-object-lock-legal-hold".to_string(), "ON".to_string()));
    }
    headers
}

/// Why the destination of `queued` cannot take its replica now, if it
/// cannot.
fn refused(
    file: &mut ObjFile<'_>,
    source: &BucketInfo,
    config: &ReplicationConfig,
    queued: &QueuedReplication,
) -> Result<Option<&'static str>, StorageError> {
    let Some(destination) = file.bucket(&queued.destination)? else {
        return Ok(Some("the destination bucket does not exist"));
    };
    Ok(if destination.owner != source.owner {
        Some("the destination bucket is another account's")
    } else if destination.versioning != Versioning::Enabled {
        Some("the destination bucket does not have versioning enabled")
    } else if source.object_lock && !destination.object_lock {
        Some("the destination bucket does not have Object Lock")
    } else {
        // As S3's role: it reads the source version and writes the replica.
        let authz = creator_authz(file, config, queued)?;
        if !may_read(&authz, queued) {
            Some(MAY_NOT_READ)
        } else {
            authz
                .check(
                    "s3:ReplicateObject",
                    Resource::Object(&queued.destination, &queued.key),
                )
                .err()
                .map(|_| "the configuration's creator may not s3:ReplicateObject there")
        }
    })
}

const MAY_NOT_READ: &str =
    "the configuration's creator may not s3:GetObjectVersionForReplication the source";

/// The authorization of the configuration's creator, with the source
/// version's tags (as a read of it would carry them).
fn creator_authz(
    file: &mut ObjFile<'_>,
    config: &ReplicationConfig,
    queued: &QueuedReplication,
) -> Result<Authz, StorageError> {
    let principal = Principal {
        access_key: String::new(),
        account: config.account.clone(),
        user: config.user.clone(),
    };
    Authz::load_now(
        file,
        &principal,
        vec![queued.destination.clone(), queued.bucket.clone()],
        vec![(
            queued.bucket.clone(),
            queued.key.clone(),
            VersionSel::Seq(queued.version),
        )],
        now_ms(),
    )
}

/// Whether the configuration's creator may read the source version for
/// replication.
fn may_read(authz: &Authz, queued: &QueuedReplication) -> bool {
    authz
        .with_keys(&[("s3:VersionId", queued.version.to_string())])
        .check(
            "s3:GetObjectVersionForReplication",
            Resource::Object(&queued.bucket, &queued.key),
        )
        .is_ok()
}

/// Prepares the replica of `source` for remote bucket `remote`, or finishes
/// the queued replication when it cannot be sent: a tag change (a remote
/// replica's version is not known here), a source the creator may not read,
/// or one under a customer-provided key (which is not kept). The body is
/// leased until the replica is sent.
fn remote_job(
    file: &mut ObjFile<'_>,
    config: &ReplicationConfig,
    queued: &QueuedReplication,
    source: &ObjectInfo,
    remote: RemoteBucket,
    class: Option<truthdb_object::StorageClass>,
    now: u64,
) -> Result<Option<RemoteJob>, StorageError> {
    if queued.work == ReplicationWork::Metadata {
        return file
            .transaction(|tx| tx.finish_replication(queued, None))
            .map(|()| None);
    }
    let owner = file.bucket(&queued.bucket)?.map(|bucket| bucket.owner);
    let authz = creator_authz(file, config, queued)?;
    let refusal = if owner.as_deref() != Some(remote.account.as_str()) {
        Some("the remote bucket is recorded for another account")
    } else if !may_read(&authz, queued) {
        Some(MAY_NOT_READ)
    } else if source.attrs.customer_key.is_some() {
        Some("an object under a customer-provided key is not sent to another service")
    } else {
        None
    };
    if let Some(why) = refusal {
        eprintln!(
            "replication: {}/{} version {} is not replicated to {}: {why}",
            queued.bucket, queued.key, queued.version, queued.destination
        );
        return file
            .transaction(|tx| tx.finish_replication(queued, Some(ReplicationStatus::Failed)))
            .map(|()| None);
    }
    let lock = file.object_lock(&queued.bucket, &queued.key, queued.version)?;
    let layout = file.body_layout(source.body)?;
    file.take_lease(source.body)?;
    Ok(Some(RemoteJob {
        queued: queued.clone(),
        remote,
        body: source.body,
        size: source.size,
        layout,
        headers: remote_headers(source, class.unwrap_or(source.storage_class), &lock, now),
    }))
}

/// Makes one queued replication in this server and takes it off the
/// queue, or prepares one for another service.
fn replicate(
    file: &mut ObjFile<'_>,
    queued: &QueuedReplication,
    now: u64,
) -> Result<Option<RemoteJob>, StorageError> {
    let found = match file.bucket(&queued.bucket)? {
        Some(bucket) => file
            .object_version(&queued.bucket, &queued.key, VersionSel::Seq(queued.version))?
            .ok()
            .filter(|info| !info.delete_marker)
            .map(|info| (bucket, info)),
        None => None,
    };
    // The version is gone: nothing to replicate.
    let Some((bucket, source)) = found else {
        return file
            .transaction(|tx| tx.finish_replication(queued, None))
            .map(|()| None);
    };
    // A rule must still send the version there; its class is the one it
    // names now.
    let config = file.replication(&queued.bucket)?.filter(|config| {
        config
            .rule_for(&queued.key)
            .is_some_and(|rule| rule.destination.bucket == queued.destination)
    });
    let Some(config) = config else {
        return file
            .transaction(|tx| tx.drop_replication(queued))
            .map(|()| None);
    };
    let rule_class = config
        .rule_for(&queued.key)
        .and_then(|rule| rule.destination.storage_class);
    // A bucket of this server first, then one on another service.
    if file.bucket(&queued.destination)?.is_none()
        && let Some(remote) = file.remote_bucket(&queued.destination)?
    {
        return remote_job(file, &config, queued, &source, remote, rule_class, now);
    }
    if let Some(why) = refused(file, &bucket, &config, queued)? {
        eprintln!(
            "replication: {}/{} version {} is not replicated to {}: {why}",
            queued.bucket, queued.key, queued.version, queued.destination
        );
        let status = match queued.work {
            ReplicationWork::Create => Some(ReplicationStatus::Failed),
            ReplicationWork::Metadata => None,
        };
        return file
            .transaction(|tx| tx.finish_replication(queued, status))
            .map(|()| None);
    }
    let source_lock = file.object_lock(&queued.bucket, &queued.key, queued.version)?;
    let lock = match queued.work {
        // A new replica without a retention of its own takes the
        // destination's default one.
        ReplicationWork::Create if source_lock.retention.is_none() => {
            let (enabled, default) = crate::object_lock::bucket_state(file, &queued.destination)?;
            ObjectLock {
                retention: crate::object_lock::new_object_lock(None, enabled, default, now)
                    .ok()
                    .flatten()
                    .and_then(|default| default.retention),
                ..source_lock
            }
        }
        // A tag change only: the lock is not used.
        ReplicationWork::Create | ReplicationWork::Metadata => source_lock,
    };
    file.set_origin(Origin {
        operation: "Replicate".to_string(),
        request_id: format!("replication-{}", queued.number),
        account: Some(bucket.owner.clone()),
        ..Origin::default()
    });
    let destination = queued.destination.clone();
    let key = queued.key.clone();
    let made = match queued.work {
        ReplicationWork::Create => file.transaction(|tx| {
            let class = rule_class.unwrap_or(source.storage_class);
            match tx.insert_replica(
                &destination,
                &key,
                &source,
                &source.attrs,
                class,
                &lock,
                now,
                DROP_GRACE_MS,
            )? {
                // Checked above, in the same lock hold.
                Err(err) => Err(StorageError::InvalidFile(format!(
                    "the destination of a replication is gone: {err:?}"
                ))),
                Ok(_) => tx.finish_replication(queued, Some(ReplicationStatus::Completed)),
            }
        }),
        ReplicationWork::Metadata => {
            let replica = file.replication_status(&destination, &key, queued.version)?
                == Some(ReplicationStatus::Replica);
            file.transaction(|tx| {
                if replica {
                    tx.set_tags(
                        &destination,
                        &key,
                        VersionSel::Seq(queued.version),
                        source.attrs.tags.clone(),
                        now,
                    )?
                    .map_err(|err| {
                        StorageError::InvalidFile(format!("a replica cannot be tagged: {err:?}"))
                    })?;
                }
                tx.finish_replication(queued, None)
            })
        }
    };
    made.map(|()| None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(path: &std::path::Path) -> truthdb_core::storage::Storage {
        const RING: u64 = 8 * 1024 * 1024;
        truthdb_core::storage::Storage::create_with_wal_bounds(
            path.to_path_buf(),
            truthdb_core::storage::StorageOptions {
                size_bytes: 64 << 20,
                wal_ratio: 0.05,
                metadata_ratio: 0.08,
                snapshot_ratio: 0.02,
                allocator_ratio: 0.02,
                reserved_ratio: 0.17,
                default_collation: None,
            },
            RING,
            RING,
            truthdb_engine::database_engines,
        )
        .expect("create")
    }

    #[test]
    fn a_replication_is_checked_against_the_configuration_and_its_creator_when_made() {
        use truthdb_object::{
            BodyWriter, ObjectAttrs, ReplicationDestination, ReplicationRule, StorageClass,
        };
        let path = std::env::temp_dir().join(format!(
            "truthdb-s3-replication-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let storage = create(&path);
        let me = "111111111111";
        let config = |user: Option<&str>| ReplicationConfig {
            role: "role".into(),
            rules: vec![ReplicationRule {
                id: String::new(),
                priority: 1,
                enabled: true,
                prefix: None,
                destination: ReplicationDestination {
                    bucket: "dst".into(),
                    account: None,
                    storage_class: None,
                },
            }],
            account: me.into(),
            user: user.map(str::to_string),
        };
        let put_tagged = |key: &str, tags: &[(&str, &str)]| {
            let mut writer = BodyWriter::begin(&storage, 1).unwrap();
            writer.write(&storage, b"data", 1).unwrap();
            let sealed = writer.finish(&storage, 1).unwrap();
            let attrs = ObjectAttrs {
                tags: tags
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.to_string()))
                    .collect(),
                ..ObjectAttrs::default()
            };
            ObjFile::new(&mut storage.lock())
                .transaction(|tx| {
                    tx.put_object("src", key, &sealed, &attrs, StorageClass::Standard, 1, 0)
                })
                .unwrap()
                .unwrap()
                .version
        };
        let put = |key: &str| put_tagged(key, &[]);
        let step = || {
            let mut lock = storage.lock();
            let mut file = ObjFile::new(&mut lock);
            for queued in file.replication_queue(None, 100).unwrap() {
                replicate(&mut file, &queued, now_ms()).unwrap();
            }
        };
        let status = |bucket: &str, key: &str, version: u64| {
            ObjFile::new(&mut storage.lock())
                .replication_status(bucket, key, version)
                .unwrap()
        };
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.create_account(me, "me", 1)?.unwrap();
                tx.create_user(me, "dev", "AIDADEV", 1)?.unwrap();
                // dev may write to src only.
                tx.put_user_policy(
                    me,
                    "dev",
                    "src-only",
                    br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                        "Action":["s3:PutObject","s3:PutReplicationConfiguration"],
                        "Resource":["arn:aws:s3:::src","arn:aws:s3:::src/*"]},
                        {"Effect":"Allow","Action":"s3:ReplicateObject",
                        "Resource":"arn:aws:s3:::dst/*"}]}"#,
                )?;
                for bucket in ["src", "dst"] {
                    tx.create_bucket(bucket, me, 1)?.unwrap();
                    tx.set_versioning(bucket, Versioning::Enabled)?.unwrap();
                }
                tx.put_replication("src", &config(Some("dev")))
            })
            .unwrap();
        // Put by a user who may write replicas in the destination but not
        // read the source's versions for replication: FAILED.
        let by_dev = put("a");
        step();
        assert_eq!(status("src", "a", by_dev), Some(ReplicationStatus::Failed));
        assert_eq!(status("dst", "a", by_dev), None);
        // Put by the account's root: replicated.
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_replication("src", &config(None)))
            .unwrap();
        let by_root = put("b");
        step();
        assert_eq!(
            status("src", "b", by_root),
            Some(ReplicationStatus::Completed)
        );
        assert_eq!(
            status("dst", "b", by_root),
            Some(ReplicationStatus::Replica)
        );
        // Queued, then the configuration is deleted: not replicated, and no
        // status.
        // A read of the source denied by a condition on its tags denies its
        // replication too.
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_bucket_config(
                    "src",
                    truthdb_object::BUCKET_POLICY,
                    "",
                    br#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*",
                        "Action":"s3:GetObjectVersionForReplication","Resource":"arn:aws:s3:::src/*",
                        "Condition":{"StringEquals":{"s3:ExistingObjectTag/classified":"true"}}}]}"#,
                )
            })
            .unwrap();
        let classified = put_tagged("secret", &[("classified", "true")]);
        let open_one = put_tagged("open", &[("classified", "false")]);
        step();
        assert_eq!(
            status("src", "secret", classified),
            Some(ReplicationStatus::Failed)
        );
        assert_eq!(
            status("src", "open", open_one),
            Some(ReplicationStatus::Completed)
        );
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_bucket_config("src", truthdb_object::BUCKET_POLICY, ""))
            .unwrap();
        // The rule's class is the one it names when the replica is made.
        let queued_standard = put("classed");
        let mut cold = config(None);
        cold.rules[0].destination.storage_class = Some(StorageClass::StandardIa);
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.put_replication("src", &cold))
            .unwrap();
        step();
        assert_eq!(
            ObjFile::new(&mut storage.lock())
                .object_version(
                    "dst",
                    "classed",
                    truthdb_object::VersionSel::Seq(queued_standard)
                )
                .unwrap()
                .unwrap()
                .storage_class,
            StorageClass::StandardIa
        );
        let orphan = put("c");
        assert_eq!(status("src", "c", orphan), Some(ReplicationStatus::Pending));
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.delete_replication("src"))
            .unwrap();
        step();
        assert_eq!(status("src", "c", orphan), None);
        assert_eq!(status("dst", "c", orphan), None);
        assert!(
            ObjFile::new(&mut storage.lock())
                .replication_queue(None, 10)
                .unwrap()
                .is_empty()
        );
        // A configuration an earlier build stored as XML: writes go on,
        // replicating nothing, until it is stored anew as put by the owner.
        let xml = format!(
            "<ReplicationConfiguration><Role>r</Role><Rule><Priority>1</Priority>\
             <Status>Enabled</Status><Filter></Filter><DeleteMarkerReplication><Status>\
             Disabled</Status></DeleteMarkerReplication><Destination><Bucket>arn:aws:s3:::dst\
             </Bucket></Destination></Rule></ReplicationConfiguration>"
        );
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_bucket_config(
                    "src",
                    truthdb_object::BUCKET_REPLICATION,
                    "",
                    xml.as_bytes(),
                )
            })
            .unwrap();
        assert_eq!(
            ObjFile::new(&mut storage.lock())
                .replication("src")
                .unwrap(),
            None
        );
        let before = put("d");
        assert_eq!(status("src", "d", before), None);
        // Until then it blocks enabling Object Lock on the bucket.
        assert!(
            crate::replication_config::blocks_object_lock(
                &mut ObjFile::new(&mut storage.lock()),
                "src",
                me
            )
            .unwrap()
        );
        store_anew(&mut ObjFile::new(&mut storage.lock())).unwrap();
        let stored = ObjFile::new(&mut storage.lock())
            .replication("src")
            .unwrap()
            .expect("stored anew");
        assert_eq!((stored.account.as_str(), stored.user.clone()), (me, None));
        let after = put("e");
        step();
        assert_eq!(
            status("src", "e", after),
            Some(ReplicationStatus::Completed)
        );
        drop(storage);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_replica_for_another_service_is_prepared_with_its_headers_or_failed() {
        use truthdb_object::{
            BodyWriter, CustomerKey, ObjectAttrs, ReplicationDestination, ReplicationRule,
            StorageClass,
        };
        let path = std::env::temp_dir().join(format!(
            "truthdb-s3-remote-replication-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let storage = create(&path);
        let me = "111111111111";
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.create_account(me, "me", 1)?.unwrap();
                tx.create_bucket("src", me, 1)?.unwrap();
                tx.set_versioning("src", Versioning::Enabled)?.unwrap();
                tx.create_remote_bucket(&RemoteBucket {
                    name: "far".into(),
                    account: me.into(),
                    endpoint: "http://127.0.0.1:1".into(),
                    region: "us-east-1".into(),
                    access_key: "k".into(),
                    secret: "s".into(),
                    ca_pem: None,
                    created_ms: 1,
                })?;
                tx.put_replication(
                    "src",
                    &ReplicationConfig {
                        role: "role".into(),
                        rules: vec![ReplicationRule {
                            id: String::new(),
                            priority: 1,
                            enabled: true,
                            prefix: None,
                            destination: ReplicationDestination {
                                bucket: "far".into(),
                                account: None,
                                storage_class: Some(StorageClass::StandardIa),
                            },
                        }],
                        account: me.into(),
                        user: None,
                    },
                )
            })
            .unwrap();
        let put = |key: &str, attrs: ObjectAttrs| {
            let mut writer = BodyWriter::begin(&storage, 1).unwrap();
            writer.write(&storage, b"data", 1).unwrap();
            let sealed = writer.finish(&storage, 1).unwrap();
            ObjFile::new(&mut storage.lock())
                .transaction(|tx| {
                    tx.put_object("src", key, &sealed, &attrs, StorageClass::Standard, 1, 0)
                })
                .unwrap()
                .unwrap()
                .version
        };
        let status = |key: &str, version: u64| {
            ObjFile::new(&mut storage.lock())
                .replication_status("src", key, version)
                .unwrap()
        };
        let next = || {
            let mut lock = storage.lock();
            let mut file = ObjFile::new(&mut lock);
            let queued = file
                .replication_queue(None, 1)
                .unwrap()
                .pop()
                .expect("queued");
            replicate(&mut file, &queued, now_ms()).unwrap()
        };
        // A plain version: prepared with its headers, its body leased, then
        // COMPLETED once sent.
        let plain = put(
            "plain",
            ObjectAttrs {
                content_type: Some("text/plain".into()),
                user: vec![("m".into(), "v".into())],
                tags: vec![("a b".into(), "c&d".into())],
                ..ObjectAttrs::default()
            },
        );
        let job = next().expect("prepared for the other service");
        assert_eq!(job.remote.name, "far");
        let header = |name: &str| {
            job.headers
                .iter()
                .find(|(header, _)| header == name)
                .map(|(_, value)| value.clone())
        };
        assert_eq!(header("content-type").as_deref(), Some("text/plain"));
        assert_eq!(header("x-amz-meta-m").as_deref(), Some("v"));
        assert_eq!(header("x-amz-tagging").as_deref(), Some("a%20b=c%26d"));
        assert_eq!(
            header("x-amz-storage-class").as_deref(),
            Some("STANDARD_IA")
        );
        finish_remote(&mut ObjFile::new(&mut storage.lock()), &job, Ok(())).unwrap();
        assert_eq!(status("plain", plain), Some(ReplicationStatus::Completed));
        // A tag change is not sent to another service: taken off the queue.
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| tx.set_tags("src", "plain", VersionSel::Seq(plain), Vec::new(), 1))
            .unwrap()
            .unwrap();
        assert!(next().is_none());
        assert!(
            ObjFile::new(&mut storage.lock())
                .replication_queue(None, 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(status("plain", plain), Some(ReplicationStatus::Completed));
        // A version under a customer-provided key: FAILED, nothing sent.
        let sealed = put(
            "sealed",
            ObjectAttrs {
                customer_key: Some(CustomerKey {
                    key_md5: "md5".into(),
                    verifier: "v".into(),
                }),
                ..ObjectAttrs::default()
            },
        );
        assert!(next().is_none());
        assert_eq!(status("sealed", sealed), Some(ReplicationStatus::Failed));
        // One that could not be sent for a while stays PENDING and queued.
        let later = put("later", ObjectAttrs::default());
        let job = next().expect("prepared");
        finish_remote(
            &mut ObjFile::new(&mut storage.lock()),
            &job,
            Err(Fail::Passing("connection refused".into())),
        )
        .unwrap();
        assert_eq!(status("later", later), Some(ReplicationStatus::Pending));
        assert_eq!(
            ObjFile::new(&mut storage.lock())
                .replication_queue(None, 10)
                .unwrap()
                .len(),
            1
        );
        // A remote bucket recorded for another account: FAILED.
        {
            let mut lock = storage.lock();
            let mut file = ObjFile::new(&mut lock);
            file.release_lease(job.body);
            file.transaction(|tx| {
                tx.finish_replication(&job.queued, None)?;
                tx.drop_remote_bucket("far")?;
                tx.create_remote_bucket(&RemoteBucket {
                    name: "far".into(),
                    account: "222222222222".into(),
                    endpoint: "http://127.0.0.1:1".into(),
                    region: "us-east-1".into(),
                    access_key: "k".into(),
                    secret: "s".into(),
                    ca_pem: None,
                    created_ms: 1,
                })
            })
            .unwrap();
        }
        let theirs = put("theirs", ObjectAttrs::default());
        assert!(next().is_none());
        assert_eq!(status("theirs", theirs), Some(ReplicationStatus::Failed));
        drop(storage);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_send_failing_for_a_while_waits_then_fails_and_parts_fit_s3s_count() {
        let mut runner = Runner::default();
        // The service down: PENDING however long it lasts; the bucket waits
        // and is held for the rest of the pass.
        let down = || Err(Fail::Passing("connection refused".to_string()));
        for _ in 0..3 * MAX_TRIES {
            assert_eq!(runner.record("far", 7, down()), down());
        }
        assert!(runner.waits["far"].0 > Instant::now(), "the bucket waits");
        assert!(runner.held.contains_key("far"));
        // A body not read: FAILED after MAX_TRIES.
        let unread = || Err(Fail::Unreadable("short".to_string()));
        for _ in 1..MAX_TRIES {
            assert_eq!(runner.record("near", 9, unread()), unread());
        }
        assert!(matches!(
            runner.record("near", 9, unread()),
            Err(Fail::Lasting(_))
        ));
        assert!(!runner.tries.contains_key(&9));
        // A skipped send holds the bucket; a sent one ends its wait.
        let mut fresh = Runner::default();
        assert_eq!(fresh.record("x", 1, Err(Fail::Skipped)), Err(Fail::Skipped));
        assert!(fresh.held.contains_key("x"));
        // A hold keeps the first entry left queued.
        fresh.hold("x", 10);
        fresh.hold("x", 4);
        fresh.hold("x", 9);
        assert_eq!(fresh.held["x"], 1);
        fresh.hold("y", 10);
        fresh.hold("y", 4);
        fresh.hold("y", 9);
        assert_eq!(fresh.held["y"], 4);
        assert_eq!(runner.record("far", 8, Ok(())), Ok(()));
        assert!(!runner.waits.contains_key("far"));
        // Its wait over, the bucket is let go and the queue read again from
        // the first of its entries left queued (7).
        runner.after = Some(100);
        assert!(!runner.held_now().contains("far"));
        assert_eq!(runner.after, Some(6));
        // No other bucket is let go before the reading again has caught up
        // with where it was (100): the reading always gets further.
        let mut again = Runner {
            after: Some(6),
            rewound_from: Some(100),
            ..Runner::default()
        };
        again.hold("near", 20);
        assert!(again.held_now().contains("near"));
        assert_eq!(again.after, Some(6));
        again.after = Some(100);
        assert!(
            again.held_now().contains("near"),
            "back where it was: no gain yet"
        );
        again.after = Some(101);
        assert!(!again.held_now().contains("near"));
        assert_eq!((again.after, again.rewound_from), (Some(19), Some(101)));
        // Held while waited for; a new pass starts with nothing else held.
        runner
            .waits
            .insert("down".into(), (Instant::now() + MAX_WAIT, MAX_WAIT));
        runner.hold("down", 40);
        runner.after = Some(50);
        assert!(runner.held_now().contains("down"));
        assert_eq!(runner.after, Some(50), "not read again while waited for");
        runner.hold("gone", 3);
        runner.after = None;
        let held = runner.held_now();
        assert!(held.contains("down") && !held.contains("gone"));
        // Parts: at least 8 MiB, at most 10,000 of them.
        assert_eq!(part_size(1), PART);
        assert_eq!(part_size(10_000 * PART as u64), PART);
        let huge = 5u64 << 40;
        assert!(huge.div_ceil(part_size(huge) as u64) <= 10_000);
        assert!(part_size(huge) > PART);
    }

    #[test]
    fn an_expired_retention_is_not_sent() {
        use truthdb_object::{Retention, RetentionMode, StorageClass};
        let info = ObjectInfo {
            version: 1,
            null_version: false,
            delete_marker: false,
            body: 1,
            size: 0,
            md5: [0; 16],
            parts: 0,
            mtime_ms: 0,
            storage_class: StorageClass::Standard,
            attrs: truthdb_object::ObjectAttrs::default(),
        };
        let lock = |until_ms| ObjectLock {
            retention: Some(Retention {
                mode: RetentionMode::Governance,
                until_ms,
                event: None,
            }),
            legal_hold: None,
        };
        let has = |headers: &[(String, String)]| {
            headers
                .iter()
                .any(|(name, _)| name == "x-amz-object-lock-retain-until-date")
        };
        assert!(has(&remote_headers(
            &info,
            StorageClass::Standard,
            &lock(2_000),
            1_000
        )));
        assert!(!has(&remote_headers(
            &info,
            StorageClass::Standard,
            &lock(500),
            1_000
        )));
    }

    #[test]
    fn a_step_goes_past_the_work_for_a_bucket_being_waited_for() {
        use truthdb_object::{
            BodyWriter, ObjectAttrs, ReplicationDestination, ReplicationRule, StorageClass,
        };
        let path = std::env::temp_dir().join(format!(
            "truthdb-s3-replication-scan-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let storage = create(&path);
        let me = "111111111111";
        let rule = |prefix: &str, bucket: &str| ReplicationRule {
            id: prefix.into(),
            priority: 1,
            enabled: true,
            prefix: Some(prefix.into()),
            destination: ReplicationDestination {
                bucket: bucket.into(),
                account: None,
                storage_class: None,
            },
        };
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.create_account(me, "me", 1)?.unwrap();
                for bucket in ["src", "near"] {
                    tx.create_bucket(bucket, me, 1)?.unwrap();
                    tx.set_versioning(bucket, Versioning::Enabled)?.unwrap();
                }
                tx.create_remote_bucket(&RemoteBucket {
                    name: "far".into(),
                    account: me.into(),
                    endpoint: "http://127.0.0.1:1".into(),
                    region: "us-east-1".into(),
                    access_key: "k".into(),
                    secret: "s".into(),
                    ca_pem: None,
                    created_ms: 1,
                })?;
                // A version first replicated far and nowhere else: each
                // configuration in turn.
                tx.put_replication(
                    "src",
                    &ReplicationConfig {
                        role: "role".into(),
                        rules: vec![rule("f/", "far")],
                        account: me.into(),
                        user: None,
                    },
                )
            })
            .unwrap();
        let mut writer = BodyWriter::begin(&storage, 1).unwrap();
        writer.write(&storage, b"x", 1).unwrap();
        let sealed = writer.finish(&storage, 1).unwrap();
        let body = ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_object(
                    "src",
                    "f/0",
                    &sealed,
                    &ObjectAttrs::default(),
                    StorageClass::Standard,
                    1,
                    0,
                )
            })
            .unwrap()
            .unwrap();
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                for i in 1..SCAN + 10 {
                    tx.copy_object(
                        "src",
                        &format!("f/{i}"),
                        &body,
                        &ObjectAttrs::default(),
                        StorageClass::Standard,
                        1,
                        0,
                    )?
                    .unwrap();
                }
                tx.put_replication(
                    "src",
                    &ReplicationConfig {
                        role: "role".into(),
                        rules: vec![rule("f/", "far"), rule("n/", "near")],
                        account: me.into(),
                        user: None,
                    },
                )
            })
            .unwrap();
        // Behind more than a scan of work for "far", being waited for.
        let local = ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.copy_object(
                    "src",
                    "n/1",
                    &body,
                    &ObjectAttrs::default(),
                    StorageClass::Standard,
                    1,
                    0,
                )
            })
            .unwrap()
            .unwrap()
            .version;
        let waiting: HashSet<String> = ["far".to_string()].into();
        let mut after = None;
        let mut steps = 0;
        loop {
            let mut lock = storage.lock();
            let mut file = ObjFile::new(&mut lock);
            let mut jobs = Vec::new();
            let mut passed = HashMap::new();
            let (_, next) = prepare(&mut file, after, &waiting, &mut jobs, &mut passed).unwrap();
            assert!(jobs.is_empty());
            if steps == 0 {
                // The first of far's entries passed over is noted.
                assert_eq!(passed.get("far"), Some(&1));
            }
            steps += 1;
            after = next;
            if after.is_none() || steps > 3 {
                break;
            }
        }
        assert_eq!(
            ObjFile::new(&mut storage.lock())
                .replication_status("src", "n/1", local)
                .unwrap(),
            Some(ReplicationStatus::Completed),
            "the local replica is made though far's work comes first"
        );
        assert_eq!(steps, 2);
        drop(storage);
        std::fs::remove_file(&path).ok();
    }
}
