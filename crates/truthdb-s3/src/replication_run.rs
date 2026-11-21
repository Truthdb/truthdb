//! The replication runner: a task beside the S3 listener writing the
//! replicas the object engine queues (see `truthdb_object::ReplicationConfig`),
//! in order. A replica keeps its source's version number, time, attributes
//! and tags, and its lock (or, with no retention of its own, the
//! destination's default retention); it is in the rule's storage class, or
//! the source's. Its body is the source's, shared.
//!
//! Each replication is checked when it is made, not only when the
//! configuration was put: the destination must still exist, be the source
//! owner's, have versioning enabled, and have Object Lock when the source
//! has it, and the source owner must be allowed `s3:ReplicateObject` there
//! (by the destination's bucket policy). Otherwise the source version is
//! FAILED: S3 retries it no more (Batch Replication does). Replicas are
//! written only once every version's state is keyed by its bucket and key
//! (see `truthdb_object::ObjFile::versions_keyed`): a replica's number is
//! its source's. A standby writes none (its log is the primary's).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use truthdb_core::storage::StorageError;
use truthdb_object::{
    BucketInfo, ObjFile, Origin, QueuedReplication, ReplicationStatus, ReplicationWork, VersionSel,
    Versioning,
};

use crate::authz::{Authz, Resource};
use crate::batch_run::object;
use crate::objects::{DROP_GRACE_MS, now_ms};
use crate::server::{Principal, S3State};

/// How long the runner waits when the queue is empty, or after an error.
const IDLE: Duration = Duration::from_secs(1);
/// Queued replications made a lock hold.
const BATCH: usize = 64;

/// Writes replicas until `shutdown` turns true.
pub async fn run(state: Arc<S3State>, mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let more = !state.storage.is_standby()
            && step(&state)
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

/// Makes the next queued replications; returns whether more may be queued.
async fn step(state: &S3State) -> Result<bool, String> {
    object(state, |file| {
        if !file.versions_keyed()? {
            return Ok(false);
        }
        let queue = file.replication_queue(BATCH)?;
        for queued in &queue {
            replicate(file, queued, now_ms())?;
        }
        Ok(queue.len() == BATCH)
    })
    .await
}

/// Why the destination of `queued` cannot take its replica now, if it
/// cannot.
fn refused(
    file: &mut ObjFile<'_>,
    source: &BucketInfo,
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
        let principal = Principal {
            access_key: String::new(),
            account: source.owner.clone(),
            user: None,
        };
        let authz = Authz::load_now(
            file,
            &principal,
            vec![queued.destination.clone()],
            Vec::new(),
            now_ms(),
        )?;
        authz
            .check(
                "s3:ReplicateObject",
                Resource::Object(&queued.destination, &queued.key),
            )
            .err()
            .map(|_| "the destination's bucket policy does not allow s3:ReplicateObject")
    })
}

/// Makes one queued replication and takes it off the queue.
fn replicate(
    file: &mut ObjFile<'_>,
    queued: &QueuedReplication,
    now: u64,
) -> Result<(), StorageError> {
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
        return file.transaction(|tx| tx.finish_replication(queued, None));
    };
    if let Some(why) = refused(file, &bucket, queued)? {
        eprintln!(
            "replication: {}/{} version {} is not replicated to {}: {why}",
            queued.bucket, queued.key, queued.version, queued.destination
        );
        let status = match queued.work {
            ReplicationWork::Create => Some(ReplicationStatus::Failed),
            ReplicationWork::Metadata => None,
        };
        return file.transaction(|tx| tx.finish_replication(queued, status));
    }
    let mut lock = file.object_lock(&queued.bucket, &queued.key, queued.version)?;
    if lock.retention.is_none() {
        lock.retention = match queued.work {
            // A new replica without a retention of its own takes the
            // destination's default one.
            ReplicationWork::Create => {
                let (enabled, default) =
                    crate::object_lock::bucket_state(file, &queued.destination)?;
                crate::object_lock::new_object_lock(None, enabled, default, now)
                    .ok()
                    .flatten()
                    .and_then(|default| default.retention)
            }
            // A replica keeps the retention it has.
            ReplicationWork::Metadata => {
                file.object_lock(&queued.destination, &queued.key, queued.version)?
                    .retention
            }
        };
    }
    file.set_origin(Origin {
        operation: "Replicate".to_string(),
        request_id: format!("replication-{}", queued.number),
        account: Some(bucket.owner.clone()),
        ..Origin::default()
    });
    let destination = queued.destination.clone();
    let key = queued.key.clone();
    match queued.work {
        ReplicationWork::Create => file.transaction(|tx| {
            let class = queued.storage_class.unwrap_or(source.storage_class);
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
                    tx.set_lock(&destination, &key, queued.version, &lock)?;
                }
                tx.finish_replication(queued, None)
            })
        }
    }
}
