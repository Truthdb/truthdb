//! Webhooks: a topic's records POSTed to a URL, in order per partition.
//! A webhook keeps, for each partition, the next offset to deliver, how
//! many times it was tried and when to try it next: a delivery that fails
//! is tried again later (holding back only its partition), and after
//! [`MAX_ATTEMPTS`] the record goes to the topic's dead-letter topic and
//! delivery moves on. Delivery is at least once: a crash after a POST and
//! before its outcome is recorded posts the record again.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::store::{NewRecord, corrupt, read_record};
use crate::{StreamFile, StreamTxn, TOPICS_TREE, WEBHOOKS_TREE};

/// Tries of a record before it is dead-lettered.
pub const MAX_ATTEMPTS: u32 = 5;
/// The first retry's wait; each next one doubles it, up to the most.
const FIRST_BACKOFF_MS: u64 = 1_000;
const MAX_BACKOFF_MS: u64 = 5 * 60_000;
/// The dead-letter topic's name: the topic's with this after it.
pub const DEAD_LETTER_SUFFIX: &str = ".dead-letter";

/// The dead-letter topic of `topic`.
pub fn dead_letter_topic(topic: &str) -> String {
    format!("{topic}{DEAD_LETTER_SUFFIX}")
}

/// A webhook on a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Webhook {
    pub topic: String,
    pub topic_id: u32,
    pub url: String,
    /// The HMAC key its POSTs are signed with.
    pub secret: String,
    pub partitions: Vec<WebhookPartition>,
    pub delivered: u64,
    pub dead_lettered: u64,
    /// The last failure (empty: none yet).
    pub last_error: String,
}

/// Where a webhook's delivery of one partition stands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WebhookPartition {
    /// The next offset to deliver.
    pub offset: u64,
    /// Failed tries of that record.
    pub attempts: u32,
    /// When it may be tried (again).
    pub next_attempt_ms: u64,
}

/// Why a webhook operation was refused (the storage itself worked).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookError {
    NoSuchTopic,
    WebhookExists,
    NoSuchWebhook,
    /// The topic's dead-letter topic name would be too long.
    NameTooLong,
}

/// A record due for delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub topic: String,
    pub topic_id: u32,
    pub partition: u32,
    pub offset: u64,
    pub url: String,
    pub secret: String,
    pub value: Vec<u8>,
}

fn put_str(out: &mut Vec<u8>, text: &str) {
    out.extend_from_slice(&(text.len() as u32).to_le_bytes());
    out.extend_from_slice(text.as_bytes());
}

/// `[topic][url][secret][delivered u64][dead u64][last error]` then per
/// partition `[offset u64][attempts u32][next attempt ms u64]`; texts as
/// `[len u32][bytes]`.
fn encode(webhook: &Webhook) -> Vec<u8> {
    let mut out = Vec::new();
    for text in [&webhook.topic, &webhook.url, &webhook.secret] {
        put_str(&mut out, text);
    }
    out.extend_from_slice(&webhook.delivered.to_le_bytes());
    out.extend_from_slice(&webhook.dead_lettered.to_le_bytes());
    put_str(&mut out, &webhook.last_error);
    for partition in &webhook.partitions {
        out.extend_from_slice(&partition.offset.to_le_bytes());
        out.extend_from_slice(&partition.attempts.to_le_bytes());
        out.extend_from_slice(&partition.next_attempt_ms.to_le_bytes());
    }
    out
}

/// Reads a row front to back.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], StorageError> {
        let (head, rest) = self
            .0
            .split_at_checked(n)
            .ok_or_else(|| corrupt("webhook"))?;
        self.0 = rest;
        Ok(head)
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn text(&mut self) -> Result<String, StorageError> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| corrupt("webhook"))
    }
}

fn decode(topic_id: u32, row: &[u8]) -> Result<Webhook, StorageError> {
    let mut input = Cursor(row);
    let (topic, url, secret) = (input.text()?, input.text()?, input.text()?);
    let (delivered, dead_lettered) = (input.u64()?, input.u64()?);
    let last_error = input.text()?;
    let mut partitions = Vec::new();
    while !input.0.is_empty() {
        partitions.push(WebhookPartition {
            offset: input.u64()?,
            attempts: input.u32()?,
            next_attempt_ms: input.u64()?,
        });
    }
    Ok(Webhook {
        topic,
        topic_id,
        url,
        secret,
        partitions,
        delivered,
        dead_lettered,
        last_error,
    })
}

/// The wait before try `attempts + 1`.
fn backoff_ms(attempts: u32) -> u64 {
    FIRST_BACKOFF_MS
        .saturating_mul(1u64 << attempts.saturating_sub(1).min(20))
        .min(MAX_BACKOFF_MS)
}

impl StreamTxn<'_, '_> {
    /// Sets a webhook on `topic` posting to `url`, signed with `secret`,
    /// from the topic's oldest record on; makes its dead-letter topic (the
    /// topic's partitions, retention and account) unless it is there.
    pub fn create_webhook(
        &mut self,
        topic: &str,
        url: &str,
        secret: &str,
        now_ms: u64,
    ) -> Result<Result<(), WebhookError>, StorageError> {
        let Some(row) = self.topic_row(topic)? else {
            return Ok(Err(WebhookError::NoSuchTopic));
        };
        let key = row.id.to_be_bytes();
        if self.trees.webhooks().get(self.ctx, &key)?.is_some() {
            return Ok(Err(WebhookError::WebhookExists));
        }
        let dead_letter = dead_letter_topic(topic);
        if self.topic_row(&dead_letter)?.is_none()
            && self
                .create_topic(
                    &dead_letter,
                    row.partitions,
                    row.retention_ms,
                    row.account.as_deref(),
                    now_ms,
                )?
                .is_err()
        {
            return Ok(Err(WebhookError::NameTooLong));
        }
        let mut partitions = Vec::new();
        for partition in 0..row.partitions {
            partitions.push(WebhookPartition {
                offset: self.partition_row(row.id, partition)?.start,
                ..WebhookPartition::default()
            });
        }
        let webhook = Webhook {
            topic: topic.to_string(),
            topic_id: row.id,
            url: url.to_string(),
            secret: secret.to_string(),
            partitions,
            delivered: 0,
            dead_lettered: 0,
            last_error: String::new(),
        };
        self.trees.webhooks().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key,
            &encode(&webhook),
        )?;
        Ok(Ok(()))
    }

    /// Removes `topic`'s webhook (its dead-letter topic stays).
    pub fn drop_webhook(&mut self, topic: &str) -> Result<Result<(), WebhookError>, StorageError> {
        let Some(row) = self.topic_row(topic)? else {
            return Ok(Err(WebhookError::NoSuchTopic));
        };
        Ok(
            match self.trees.webhooks().delete(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &row.id.to_be_bytes(),
            )? {
                Some(_) => Ok(()),
                None => Err(WebhookError::NoSuchWebhook),
            },
        )
    }

    fn webhook(&mut self, topic_id: u32) -> Result<Option<Webhook>, StorageError> {
        self.trees
            .webhooks()
            .get(self.ctx, &topic_id.to_be_bytes())?
            .map(|row| decode(topic_id, &row))
            .transpose()
    }

    fn put_webhook(&mut self, webhook: &Webhook) -> Result<(), StorageError> {
        self.trees.webhooks().update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &webhook.topic_id.to_be_bytes(),
            &encode(webhook),
        )?;
        Ok(())
    }

    /// Records that `delivery` was posted: its partition goes on after it.
    /// Nothing when the webhook moved on meanwhile (dropped, made again).
    pub fn webhook_delivered(&mut self, delivery: &Delivery) -> Result<(), StorageError> {
        let Some(mut webhook) = self.current(delivery)? else {
            return Ok(());
        };
        webhook.partitions[delivery.partition as usize] = WebhookPartition {
            offset: delivery.offset + 1,
            ..WebhookPartition::default()
        };
        webhook.delivered += 1;
        self.put_webhook(&webhook)
    }

    /// Records that posting `delivery` failed with `error` at `now_ms`: it
    /// is tried again after a wait, or after [`MAX_ATTEMPTS`] goes to the
    /// dead-letter topic (once, whatever crash follows) and its partition
    /// goes on. Returns whether it was dead-lettered.
    pub fn webhook_failed(
        &mut self,
        delivery: &Delivery,
        error: &str,
        now_ms: u64,
    ) -> Result<bool, StorageError> {
        let Some(mut webhook) = self.current(delivery)? else {
            return Ok(false);
        };
        webhook.last_error = error.chars().take(1024).collect();
        let state = &mut webhook.partitions[delivery.partition as usize];
        state.attempts += 1;
        let dead = state.attempts >= MAX_ATTEMPTS;
        if dead {
            *state = WebhookPartition {
                offset: delivery.offset + 1,
                ..WebhookPartition::default()
            };
            webhook.dead_lettered += 1;
            let record = NewRecord {
                key: None,
                value: delivery.value.clone(),
            };
            // The dead-letter topic may be gone (dropped): the record is
            // then dropped too.
            let _ = self.append(
                &dead_letter_topic(&delivery.topic),
                delivery.partition,
                &[record],
                Some(("webhook", delivery.offset + 1)),
                now_ms,
            )?;
        } else {
            state.next_attempt_ms = now_ms.saturating_add(backoff_ms(state.attempts));
        }
        self.put_webhook(&webhook)?;
        Ok(dead)
    }

    /// The webhook `delivery` came from, when it has not gone past it (it
    /// may stand before it: records retention removed are passed over).
    fn current(&mut self, delivery: &Delivery) -> Result<Option<Webhook>, StorageError> {
        Ok(self.webhook(delivery.topic_id)?.filter(|webhook| {
            webhook
                .partitions
                .get(delivery.partition as usize)
                .is_some_and(|state| state.offset <= delivery.offset)
        }))
    }
}

impl StreamFile<'_> {
    /// Every webhook, by topic id.
    pub fn webhooks(&mut self) -> Result<Vec<Webhook>, StorageError> {
        let Some(tree) = self.existing_tree(WEBHOOKS_TREE)? else {
            return Ok(Vec::new());
        };
        tree.scan(&mut self.page_ctx())?
            .into_iter()
            .map(|(key, row)| {
                let id = u32::from_be_bytes(key.try_into().map_err(|_| corrupt("webhook key"))?);
                decode(id, &row)
            })
            .collect()
    }

    /// Up to `limit` records due for delivery at `now_ms`: each webhook
    /// partition's next one, once its wait is over. A record retention
    /// removed is passed over.
    pub fn webhook_due(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<Delivery>, StorageError> {
        let webhooks = self.webhooks()?;
        let (Some(records), Some(_)) = (
            self.existing_tree(crate::RECORDS_TREE)?,
            self.existing_tree(TOPICS_TREE)?,
        ) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for webhook in webhooks {
            let Some((_, partitions)) = self.topic(&webhook.topic)? else {
                continue;
            };
            for (partition, state) in (0..).zip(&webhook.partitions) {
                if out.len() == limit {
                    return Ok(out);
                }
                let Some(log) = partitions.get(partition as usize) else {
                    continue;
                };
                if state.next_attempt_ms > now_ms {
                    continue;
                }
                let offset = state.offset.max(log.start);
                if offset >= log.next {
                    continue;
                }
                let mut ctx = self.page_ctx();
                let record = read_record(&mut ctx, &records, webhook.topic_id, partition, offset)?
                    .ok_or_else(|| corrupt("partition missing a record"))?;
                out.push(Delivery {
                    topic: webhook.topic.clone(),
                    topic_id: webhook.topic_id,
                    partition,
                    offset,
                    url: webhook.url.clone(),
                    secret: webhook.secret.clone(),
                    value: record.value,
                });
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_webhook_round_trips_and_corruption_is_refused() {
        let webhook = Webhook {
            topic: "events".into(),
            topic_id: 7,
            url: "https://hooks.example/s3".into(),
            secret: "s3cret".into(),
            partitions: vec![
                WebhookPartition {
                    offset: 3,
                    attempts: 2,
                    next_attempt_ms: 9,
                },
                WebhookPartition::default(),
            ],
            delivered: 4,
            dead_lettered: 1,
            last_error: "HTTP 500".into(),
        };
        let row = encode(&webhook);
        assert_eq!(decode(7, &row).unwrap(), webhook);
        for cut in 1..row.len() {
            let decoded = decode(7, &row[..cut]);
            assert!(
                decoded.is_err() || decoded.unwrap().partitions.len() < 2,
                "cut at {cut}"
            );
        }
        assert_eq!(backoff_ms(1), 1_000);
        assert_eq!(backoff_ms(2), 2_000);
        assert_eq!(backoff_ms(4), 8_000);
        assert_eq!(backoff_ms(40), MAX_BACKOFF_MS);
    }
}
