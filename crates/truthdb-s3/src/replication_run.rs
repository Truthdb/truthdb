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
//! the configuration must be allowed `s3:ReplicateObject` there. Otherwise
//! the source version is FAILED: S3 retries it no more (Batch Replication
//! does). A change of the source's tags gives the replica the same tags; a
//! change of its lock only adds to the replica's (a legal hold, a longer
//! retention of the same mode), never takes from it. Replicas are
//! written only once every version's state is keyed by its bucket and key
//! (see `truthdb_object::ObjFile::versions_keyed`): a replica's number is
//! its source's. A standby writes none (its log is the primary's).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use truthdb_core::storage::StorageError;
use truthdb_object::{
    BucketInfo, ObjFile, ObjectLock, Origin, QueuedReplication, ReplicationConfig,
    ReplicationStatus, ReplicationWork, VersionSel, Versioning,
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
    let mut stored_anew = false;
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

/// Stores anew the configurations an earlier build stored as XML, as put
/// by their bucket's owner (until then they replicate nothing).
async fn store_xml_anew(state: &S3State) -> Result<(), String> {
    object(state, store_anew).await
}

/// [`store_xml_anew`] in `file`.
fn store_anew(file: &mut ObjFile<'_>) -> Result<(), StorageError> {
    for bucket in file.list_buckets()? {
        let Some(xml) = file.replication_xml(&bucket.name)? else {
            continue;
        };
        match crate::replication_config::parse(&xml) {
            Ok(mut config) => {
                config.account = bucket.owner.clone();
                file.transaction(|tx| tx.put_replication(&bucket.name, &config))?;
            }
            Err(err) => eprintln!(
                "replication: the configuration of {} cannot be read: {}",
                bucket.name, err.message
            ),
        }
    }
    Ok(())
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
        let principal = Principal {
            access_key: String::new(),
            account: config.account.clone(),
            user: config.user.clone(),
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
            .map(|_| "the configuration's creator may not s3:ReplicateObject there")
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
    // A rule must still send the version there.
    let config = file.replication(&queued.bucket)?.filter(|config| {
        config
            .rule_for(&queued.key)
            .is_some_and(|rule| rule.destination.bucket == queued.destination)
    });
    let Some(config) = config else {
        return file.transaction(|tx| tx.drop_replication(queued));
    };
    if let Some(why) = refused(file, &bucket, &config, queued)? {
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
        ReplicationWork::Create => source_lock,
        ReplicationWork::Metadata => added(
            file.object_lock(&queued.destination, &queued.key, queued.version)?,
            source_lock,
            now,
        ),
    };
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

/// The replica's lock `replica` with what the source's lock `source` adds
/// to it: a legal hold, a retention where it has none, or a later date in
/// the same mode. Nothing it has is taken away or shortened.
fn added(replica: ObjectLock, source: ObjectLock, now: u64) -> ObjectLock {
    let mut lock = replica;
    if source.legal_hold == Some(true) {
        lock.legal_hold = Some(true);
    }
    if let Some(new) = source.retention {
        match lock.retention {
            None => lock.retention = Some(new),
            Some(current)
                if current.mode == new.mode
                    && new.effective_until(now) > current.effective_until(now) =>
            {
                lock.retention = Some(new)
            }
            Some(_) => {}
        }
    }
    lock
}

#[cfg(test)]
mod tests {
    use super::*;
    use truthdb_object::{Retention, RetentionMode};

    fn retention(mode: RetentionMode, until_ms: u64) -> Option<Retention> {
        Some(Retention {
            mode,
            until_ms,
            event: None,
        })
    }

    #[test]
    fn a_source_lock_only_adds_to_its_replicas() {
        let lock = |retention, legal_hold| ObjectLock {
            retention,
            legal_hold,
        };
        let compliance = |until| retention(RetentionMode::Compliance, until);
        let governance = |until| retention(RetentionMode::Governance, until);
        for (replica, source, expected) in [
            // A hold placed on the replica stays; one on the source is added.
            (
                lock(None, Some(true)),
                lock(None, None),
                lock(None, Some(true)),
            ),
            (
                lock(None, Some(true)),
                lock(None, Some(false)),
                lock(None, Some(true)),
            ),
            (
                lock(None, None),
                lock(None, Some(true)),
                lock(None, Some(true)),
            ),
            // A retention is never shortened, nor its mode changed.
            (
                lock(compliance(40), None),
                lock(compliance(30), None),
                lock(compliance(40), None),
            ),
            (
                lock(compliance(30), None),
                lock(compliance(40), None),
                lock(compliance(40), None),
            ),
            (
                lock(compliance(40), None),
                lock(governance(50), None),
                lock(compliance(40), None),
            ),
            (
                lock(None, None),
                lock(governance(50), None),
                lock(governance(50), None),
            ),
            (
                lock(governance(50), None),
                lock(None, None),
                lock(governance(50), None),
            ),
        ] {
            assert_eq!(
                added(replica, source, 0),
                expected,
                "{replica:?} {source:?}"
            );
        }
    }

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
        let put = |key: &str| {
            let mut writer = BodyWriter::begin(&storage, 1).unwrap();
            writer.write(&storage, b"data", 1).unwrap();
            let sealed = writer.finish(&storage, 1).unwrap();
            ObjFile::new(&mut storage.lock())
                .transaction(|tx| {
                    tx.put_object(
                        "src",
                        key,
                        &sealed,
                        &ObjectAttrs::default(),
                        StorageClass::Standard,
                        1,
                        0,
                    )
                })
                .unwrap()
                .unwrap()
                .version
        };
        let step = || {
            let mut lock = storage.lock();
            let mut file = ObjFile::new(&mut lock);
            for queued in file.replication_queue(100).unwrap() {
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
                        "Resource":["arn:aws:s3:::src","arn:aws:s3:::src/*"]}]}"#,
                )?;
                for bucket in ["src", "dst"] {
                    tx.create_bucket(bucket, me, 1)?.unwrap();
                    tx.set_versioning(bucket, Versioning::Enabled)?.unwrap();
                }
                tx.put_replication("src", &config(Some("dev")))
            })
            .unwrap();
        // Put by a user who may not write in the destination: FAILED.
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
                .replication_queue(10)
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
}
