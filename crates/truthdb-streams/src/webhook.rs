//! Webhooks: a topic's records POSTed to a URL, in order per partition.
//! A webhook's row holds where it posts, its secret and its counts; each
//! partition's row holds the next offset to deliver, how many times it was
//! tried and when to try it next. A delivery that fails is tried again later
//! (holding back only its partition), and after [`MAX_ATTEMPTS`] the record
//! goes to the topic's dead-letter topic and delivery moves on. Delivery is
//! at least once: a crash after a POST and before its outcome is recorded
//! posts the record again. Each webhook made has a generation of its own,
//! so the outcome of a POST made for one dropped since is not taken for its
//! successor's.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::store::{NewRecord, corrupt, partition_key, read_record, u64_at};
use crate::{
    NEXT_WEBHOOK_KEY, StreamFile, StreamTxn, TOPICS_TREE, WEBHOOK_PARTITIONS_TREE, WEBHOOKS_TREE,
};

/// Tries of a record before it is dead-lettered.
pub const MAX_ATTEMPTS: u32 = 5;
/// The longest URL and secret a webhook takes, in bytes (its row fits a
/// tree cell with the longest topic name and error kept).
pub const MAX_URL_BYTES: usize = 1024;
pub const MAX_SECRET_BYTES: usize = 256;
/// The last error is kept to this many bytes.
const MAX_ERROR_BYTES: usize = 256;
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
    pub generation: u64,
    pub url: String,
    /// The HMAC key its POSTs are signed with.
    pub secret: String,
    pub partitions: Vec<WebhookPartition>,
    pub delivered: u64,
    pub dead_lettered: u64,
    /// The last failure since the last delivery (empty: none).
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
    /// The dead-letter topic there has fewer partitions than the topic.
    DeadLetterTooSmall,
    UrlTooLong,
    SecretTooLong,
}

/// A record due for delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub topic: String,
    pub topic_id: u32,
    pub generation: u64,
    pub partition: u32,
    pub offset: u64,
    pub url: String,
    pub secret: String,
    pub value: Vec<u8>,
}

/// The webhook's own row, without its partitions.
struct Head {
    topic: String,
    generation: u64,
    url: String,
    secret: String,
    delivered: u64,
    dead_lettered: u64,
    last_error: String,
}

fn put_str(out: &mut Vec<u8>, text: &str) {
    out.extend_from_slice(&(text.len() as u32).to_le_bytes());
    out.extend_from_slice(text.as_bytes());
}

/// `[generation u64][topic][url][secret][delivered u64][dead u64][last
/// error]`, texts as `[len u32][bytes]`: at most 249 + 1,024 + 256 + 256 +
/// 40 bytes.
fn encode_head(head: &Head) -> Vec<u8> {
    let mut out = head.generation.to_le_bytes().to_vec();
    for text in [&head.topic, &head.url, &head.secret] {
        put_str(&mut out, text);
    }
    out.extend_from_slice(&head.delivered.to_le_bytes());
    out.extend_from_slice(&head.dead_lettered.to_le_bytes());
    put_str(&mut out, &head.last_error);
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

    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn text(&mut self) -> Result<String, StorageError> {
        let len = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| corrupt("webhook"))
    }
}

fn decode_head(row: &[u8]) -> Result<Head, StorageError> {
    let mut input = Cursor(row);
    let generation = input.u64()?;
    let (topic, url, secret) = (input.text()?, input.text()?, input.text()?);
    let (delivered, dead_lettered) = (input.u64()?, input.u64()?);
    let last_error = input.text()?;
    if !input.0.is_empty() {
        return Err(corrupt("webhook"));
    }
    Ok(Head {
        topic,
        generation,
        url,
        secret,
        delivered,
        dead_lettered,
        last_error,
    })
}

/// `[offset u64][attempts u32][next attempt ms u64]`.
fn encode_partition(state: &WebhookPartition) -> [u8; 20] {
    let mut out = [0u8; 20];
    out[..8].copy_from_slice(&state.offset.to_le_bytes());
    out[8..12].copy_from_slice(&state.attempts.to_le_bytes());
    out[12..].copy_from_slice(&state.next_attempt_ms.to_le_bytes());
    out
}

fn decode_partition(row: &[u8]) -> Result<WebhookPartition, StorageError> {
    if row.len() != 20 {
        return Err(corrupt("webhook partition"));
    }
    Ok(WebhookPartition {
        offset: u64_at(row, 0)?,
        attempts: u32::from_le_bytes(row[8..12].try_into().unwrap()),
        next_attempt_ms: u64_at(row, 12)?,
    })
}

/// The wait before try `attempts + 1`.
fn backoff_ms(attempts: u32) -> u64 {
    FIRST_BACKOFF_MS
        .saturating_mul(1u64 << attempts.saturating_sub(1).min(20))
        .min(MAX_BACKOFF_MS)
}

/// `text` cut to at most `max` bytes, on a character boundary.
fn cut(text: &str, max: usize) -> String {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
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
        if url.len() > MAX_URL_BYTES {
            return Ok(Err(WebhookError::UrlTooLong));
        }
        if secret.len() > MAX_SECRET_BYTES {
            return Ok(Err(WebhookError::SecretTooLong));
        }
        let Some(row) = self.topic_row(topic)? else {
            return Ok(Err(WebhookError::NoSuchTopic));
        };
        let key = row.id.to_be_bytes();
        if self.trees.webhooks().get(self.ctx, &key)?.is_some() {
            return Ok(Err(WebhookError::WebhookExists));
        }
        let dead_letter = dead_letter_topic(topic);
        match self.topic_row(&dead_letter)? {
            Some(dead) if dead.partitions < row.partitions => {
                return Ok(Err(WebhookError::DeadLetterTooSmall));
            }
            Some(_) => {}
            None => {
                if self
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
            }
        }
        let head = Head {
            topic: topic.to_string(),
            generation: self.next_counter(NEXT_WEBHOOK_KEY)?,
            url: url.to_string(),
            secret: secret.to_string(),
            delivered: 0,
            dead_lettered: 0,
            last_error: String::new(),
        };
        let mode = &mut OpMode::Txn(self.txn);
        self.trees
            .webhooks()
            .insert_unique(self.ctx, mode, &key, &encode_head(&head))?;
        for partition in 0..row.partitions {
            let state = WebhookPartition {
                offset: self.partition_row(row.id, partition)?.start,
                ..WebhookPartition::default()
            };
            self.trees.webhook_partitions().insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &partition_key(row.id, partition),
                &encode_partition(&state),
            )?;
        }
        Ok(Ok(()))
    }

    /// Removes `topic`'s webhook (its dead-letter topic stays).
    pub fn drop_webhook(&mut self, topic: &str) -> Result<Result<(), WebhookError>, StorageError> {
        let Some(row) = self.topic_row(topic)? else {
            return Ok(Err(WebhookError::NoSuchTopic));
        };
        let mode = &mut OpMode::Txn(self.txn);
        if self
            .trees
            .webhooks()
            .delete(self.ctx, mode, &row.id.to_be_bytes())?
            .is_none()
        {
            return Ok(Err(WebhookError::NoSuchWebhook));
        }
        for partition in 0..row.partitions {
            self.trees.webhook_partitions().delete(
                self.ctx,
                mode,
                &partition_key(row.id, partition),
            )?;
        }
        Ok(Ok(()))
    }

    /// The webhook and partition state `delivery` came from, when it is
    /// the same webhook and has not gone past the record (it may stand
    /// before it: records retention removed are passed over).
    fn current(
        &mut self,
        delivery: &Delivery,
    ) -> Result<Option<(Head, WebhookPartition)>, StorageError> {
        let Some(row) = self
            .trees
            .webhooks()
            .get(self.ctx, &delivery.topic_id.to_be_bytes())?
        else {
            return Ok(None);
        };
        let head = decode_head(&row)?;
        if head.generation != delivery.generation {
            return Ok(None);
        }
        let Some(row) = self.trees.webhook_partitions().get(
            self.ctx,
            &partition_key(delivery.topic_id, delivery.partition),
        )?
        else {
            return Ok(None);
        };
        let state = decode_partition(&row)?;
        Ok((state.offset <= delivery.offset).then_some((head, state)))
    }

    fn put_outcome(
        &mut self,
        delivery: &Delivery,
        head: &Head,
        state: &WebhookPartition,
    ) -> Result<(), StorageError> {
        let mode = &mut OpMode::Txn(self.txn);
        self.trees.webhooks().update(
            self.ctx,
            mode,
            &delivery.topic_id.to_be_bytes(),
            &encode_head(head),
        )?;
        self.trees.webhook_partitions().update(
            self.ctx,
            mode,
            &partition_key(delivery.topic_id, delivery.partition),
            &encode_partition(state),
        )?;
        Ok(())
    }

    /// Records that `delivery` was posted: its partition goes on after it.
    /// Nothing when the webhook moved on meanwhile (dropped, made again).
    pub fn webhook_delivered(&mut self, delivery: &Delivery) -> Result<(), StorageError> {
        let Some((mut head, _)) = self.current(delivery)? else {
            return Ok(());
        };
        head.delivered += 1;
        head.last_error.clear();
        let state = WebhookPartition {
            offset: delivery.offset + 1,
            ..WebhookPartition::default()
        };
        self.put_outcome(delivery, &head, &state)
    }

    /// Records that posting `delivery` failed with `error` at `now_ms`: it
    /// is tried again after a wait, or after [`MAX_ATTEMPTS`] goes to the
    /// dead-letter topic and its partition goes on (in one transaction:
    /// once, whatever crash follows). Returns whether it was dead-lettered.
    pub fn webhook_failed(
        &mut self,
        delivery: &Delivery,
        error: &str,
        now_ms: u64,
    ) -> Result<bool, StorageError> {
        let Some((mut head, mut state)) = self.current(delivery)? else {
            return Ok(false);
        };
        head.last_error = cut(error, MAX_ERROR_BYTES);
        // The record before it went with retention: its tries were not
        // this one's.
        if state.offset < delivery.offset {
            state = WebhookPartition {
                offset: delivery.offset,
                ..WebhookPartition::default()
            };
        }
        state.attempts += 1;
        let dead = state.attempts >= MAX_ATTEMPTS;
        if dead {
            state = WebhookPartition {
                offset: delivery.offset + 1,
                ..WebhookPartition::default()
            };
            head.dead_lettered += 1;
            self.dead_letter(delivery, now_ms, &mut head)?;
        } else {
            state.next_attempt_ms = now_ms.saturating_add(backoff_ms(state.attempts));
        }
        self.put_outcome(delivery, &head, &state)?;
        Ok(dead)
    }

    /// Appends a failed record to the dead-letter topic: in its partition,
    /// or (that topic made again with fewer) the partition its number wraps
    /// to. A dead-letter topic that is gone, or refuses it, is noted in the
    /// last error (delivery goes on all the same).
    fn dead_letter(
        &mut self,
        delivery: &Delivery,
        now_ms: u64,
        head: &mut Head,
    ) -> Result<(), StorageError> {
        let topic = dead_letter_topic(&delivery.topic);
        let note = |head: &mut Head, why: &str| {
            head.last_error = cut(&format!("{}; {why}", head.last_error), MAX_ERROR_BYTES);
        };
        let Some(row) = self.topic_row(&topic)? else {
            note(head, "the dead-letter topic is gone");
            return Ok(());
        };
        let record = NewRecord {
            key: None,
            value: delivery.value.clone(),
        };
        match self.append(
            &topic,
            delivery.partition % row.partitions,
            &[record],
            None,
            now_ms,
        )? {
            Ok(_) => {}
            Err(err) => note(head, &format!("the dead-letter topic refused it: {err:?}")),
        }
        Ok(())
    }
}

impl StreamFile<'_> {
    /// Every webhook, by topic id.
    pub fn webhooks(&mut self) -> Result<Vec<Webhook>, StorageError> {
        let (Some(heads), Some(states)) = (
            self.existing_tree(WEBHOOKS_TREE)?,
            self.existing_tree(WEBHOOK_PARTITIONS_TREE)?,
        ) else {
            return Ok(Vec::new());
        };
        let mut ctx = self.page_ctx();
        let mut out = Vec::new();
        for (key, row) in heads.scan(&mut ctx)? {
            let topic_id = u32::from_be_bytes(key.try_into().map_err(|_| corrupt("webhook key"))?);
            let head = decode_head(&row)?;
            let partitions = states
                .scan_range(
                    &mut ctx,
                    Some(&partition_key(topic_id, 0)),
                    Some(&partition_key(topic_id, u32::MAX)),
                )?
                .iter()
                .map(|(_, row)| decode_partition(row))
                .collect::<Result<_, _>>()?;
            out.push(Webhook {
                topic: head.topic,
                topic_id,
                generation: head.generation,
                url: head.url,
                secret: head.secret,
                partitions,
                delivered: head.delivered,
                dead_lettered: head.dead_lettered,
                last_error: head.last_error,
            });
        }
        Ok(out)
    }

    /// Up to `limit` records due for delivery at `now_ms`: each webhook
    /// partition's next one, once its wait is over, taken in turn from the
    /// `round`th webhook partition on (so a step that cannot take them all
    /// leaves out different ones each round). A record retention removed is
    /// passed over.
    pub fn webhook_due(
        &mut self,
        now_ms: u64,
        limit: usize,
        round: usize,
    ) -> Result<Vec<Delivery>, StorageError> {
        let webhooks = self.webhooks()?;
        let (Some(records), Some(_)) = (
            self.existing_tree(crate::RECORDS_TREE)?,
            self.existing_tree(TOPICS_TREE)?,
        ) else {
            return Ok(Vec::new());
        };
        let mut candidates = Vec::new();
        for webhook in &webhooks {
            let Some((_, logs)) = self.topic(&webhook.topic)? else {
                continue;
            };
            for (partition, state) in (0..).zip(&webhook.partitions) {
                let Some(log) = logs.get(partition as usize) else {
                    continue;
                };
                let offset = state.offset.max(log.start);
                if state.next_attempt_ms <= now_ms && offset < log.next {
                    candidates.push((webhook, partition, offset));
                }
            }
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let first = round % candidates.len();
        candidates.rotate_left(first);
        let mut out = Vec::new();
        let mut ctx = self.page_ctx();
        for (webhook, partition, offset) in candidates.into_iter().take(limit) {
            let record = read_record(&mut ctx, &records, webhook.topic_id, partition, offset)?
                .ok_or_else(|| corrupt("partition missing a record"))?;
            out.push(Delivery {
                topic: webhook.topic.clone(),
                topic_id: webhook.topic_id,
                generation: webhook.generation,
                partition,
                offset,
                url: webhook.url.clone(),
                secret: webhook.secret.clone(),
                value: record.value,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_round_trip_and_corruption_is_refused() {
        let head = Head {
            topic: "events".into(),
            generation: 3,
            url: "https://hooks.example/s3".into(),
            secret: "s3cret".into(),
            delivered: 4,
            dead_lettered: 1,
            last_error: "HTTP 500".into(),
        };
        let row = encode_head(&head);
        let decoded = decode_head(&row).unwrap();
        assert_eq!(
            (
                decoded.topic,
                decoded.generation,
                decoded.url,
                decoded.secret
            ),
            (head.topic, head.generation, head.url, head.secret)
        );
        assert_eq!(
            (decoded.delivered, decoded.dead_lettered, decoded.last_error),
            (4, 1, "HTTP 500".to_string())
        );
        for cut in 0..row.len() {
            assert!(decode_head(&row[..cut]).is_err(), "cut at {cut}");
        }
        assert!(decode_head(&[row.clone(), vec![0]].concat()).is_err());
        let state = WebhookPartition {
            offset: 3,
            attempts: 2,
            next_attempt_ms: 9,
        };
        assert_eq!(decode_partition(&encode_partition(&state)).unwrap(), state);
        assert!(decode_partition(&[0; 19]).is_err());
        // The longest a row gets fits a tree cell.
        let longest = Head {
            topic: "t".repeat(249),
            generation: u64::MAX,
            url: "u".repeat(MAX_URL_BYTES),
            secret: "s".repeat(MAX_SECRET_BYTES),
            delivered: u64::MAX,
            dead_lettered: u64::MAX,
            last_error: cut(&"é".repeat(1000), MAX_ERROR_BYTES),
        };
        assert!(encode_head(&longest).len() + 4 <= truthdb_core::relstore::btree::TREE_MAX_CELL);
        assert_eq!(cut("aé", 2), "a");
        assert_eq!(backoff_ms(1), 1_000);
        assert_eq!(backoff_ms(2), 2_000);
        assert_eq!(backoff_ms(4), 8_000);
        assert_eq!(backoff_ms(40), MAX_BACKOFF_MS);
    }
}
