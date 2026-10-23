//! Bucket event notifications (see `truthdb_object::NotificationConfig`),
//! sent by the journal consumer `notifications` on the indexer thread: each
//! step reads a batch of changes, appends the event message of every
//! configuration a change matches to that configuration's stream topic, and
//! then moves the consumer past the batch. A change is sent as its bucket
//! was configured when the change was made.
//!
//! The appends and the consumer's place are two transactions (of two
//! engines). Each event is appended under the source `s3-notifications`
//! with its change's sequence number, so a batch appended again after a
//! crash between the two writes nothing twice. An event whose topic is
//! gone, or is not owned by the bucket's owner, is dropped. A partition is
//! chosen by the event's bucket and key, so one key's events stay in
//! order.

use std::collections::BTreeMap;

use truthdb_object::{event_message, event_type};
use truthdb_streams::{MAX_APPEND_CHUNKS, NewRecord, Topic};

use super::{Engine, EngineError};

/// Changes one step reads at most.
const NOTIFY_BATCH: usize = 64;
/// The source every event is appended as.
const SOURCE: &str = "s3-notifications";

/// The partition of `bucket`'s `key` in a topic of `partitions` (FNV-1a).
fn partition_of(bucket: &str, key: &str, partitions: u32) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in bucket.bytes().chain([0]).chain(key.bytes()) {
        hash = (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193);
    }
    hash % partitions.max(1)
}

impl Engine {
    /// One step of the notifications consumer; returns whether changes
    /// remain.
    pub(crate) fn notifications_step(&self) -> Result<bool, EngineError> {
        if self.storage.is_standby() || !self.runs_objects() {
            return Ok(false);
        }
        let Some(cursor) = self.object(|file| file.notifications_cursor())? else {
            return Ok(false);
        };
        let Some((through, more)) = self.notifications_send(cursor)? else {
            return Ok(false);
        };
        self.object(|file| file.transaction(|tx| tx.advance_notifications(through)))?;
        Ok(more)
    }

    /// Sends the events of a batch of changes after `cursor`: returns the
    /// last change sent and whether changes remain (`None`: no change).
    /// Sending a batch again sends nothing twice.
    pub(super) fn notifications_send(
        &self,
        cursor: u64,
    ) -> Result<Option<(u64, bool)>, EngineError> {
        // The changes, each with the notification configuration and owner
        // its bucket had when it was made.
        let changes = self.object(|file| {
            file.journal(cursor, NOTIFY_BATCH)?
                .into_iter()
                .map(|record| {
                    let config = file.notification_at(&record.bucket, record.sequence)?;
                    Ok((record, config))
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        let Some(last) = changes.last().map(|(record, _)| record.sequence) else {
            return Ok(None);
        };
        // Per change, its events by topic and partition, stopping at an
        // append's worth of chunks (so the transaction's log stays within
        // its budget), with at least one change.
        let topics: BTreeMap<String, Topic> = self.streams(|file| {
            Ok(file
                .topics()?
                .into_iter()
                .map(|(topic, _)| (topic.name.clone(), topic))
                .collect())
        })?;
        let mut appends: BTreeMap<(u64, String, u32), Vec<NewRecord>> = BTreeMap::new();
        let mut chunks = 0;
        let mut through = last;
        for (record, config) in &changes {
            if chunks >= MAX_APPEND_CHUNKS {
                through = record.sequence - 1;
                break;
            }
            let Some((config, owner)) = config else {
                continue;
            };
            let Some(event) = event_type(record) else {
                continue;
            };
            for rule in config
                .rules
                .iter()
                .filter(|rule| rule.matches(event, &record.key))
            {
                // A topic since dropped, or not the bucket owner's: the
                // event is dropped.
                let topic = rule.destination.topic();
                let Some(partitions) = topics
                    .get(topic)
                    .filter(|topic| topic.account.as_deref() == Some(owner.as_str()))
                    .map(|topic| topic.partitions)
                else {
                    continue;
                };
                let message = NewRecord {
                    key: Some(format!("{}/{}", record.bucket, record.key).into_bytes()),
                    value: event_message(record, event, config, rule, owner).into_bytes(),
                };
                chunks += message.chunks();
                // One append per change and partition: a second append of
                // the same sequence number would be taken for a repeat.
                appends
                    .entry((
                        record.sequence,
                        topic.to_string(),
                        partition_of(&record.bucket, &record.key, partitions),
                    ))
                    .or_default()
                    .push(message);
            }
        }
        if !appends.is_empty() {
            let now_ms = super::now_ms();
            self.streams(|file| {
                file.transaction(|tx| {
                    for ((sequence, topic, partition), events) in &appends {
                        // A refusal (the topic dropped meanwhile, an event
                        // too large) drops the events.
                        let _ = tx.append(
                            topic,
                            *partition,
                            events,
                            Some((SOURCE, *sequence)),
                            now_ms,
                        )?;
                    }
                    Ok(())
                })
            })?;
        }
        Ok(Some((
            through,
            changes.len() == NOTIFY_BATCH || through < last,
        )))
    }
}
