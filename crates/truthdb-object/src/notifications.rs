//! Bucket event notifications (`PutBucketNotificationConfiguration`): which
//! events of a bucket's objects go where. The change journal's consumer
//! `notifications` reads each change, names its event (`s3:ObjectCreated:Put`,
//! ...), and sends the S3 event message of every configuration it matches to
//! that configuration's destination. Events come from the journal, so a
//! change and its event are recorded together; the consumer only moves past
//! a change once its events are delivered.

use truthdb_core::storage::StorageError;

use crate::codec::{Reader, Writer};
use crate::consumers::Consumer;
use crate::documents::{BUCKET_NOTIFICATION, read};
use crate::journal::{JournalRecord, RecordType};
use truthdb_core::relstore::btree::BTree;
use truthdb_core::relstore::ctx::{OpMode, RelCtx};

use crate::{NOTIFY_HISTORY_TREE, ObjFile, ObjTxn};

/// The journal consumer that sends event notifications.
pub const NOTIFICATIONS_CONSUMER: &str = "notifications";

/// The event types a configuration may name (S3's list).
pub const EVENT_TYPES: &[&str] = &[
    "s3:ReducedRedundancyLostObject",
    "s3:ObjectCreated:*",
    "s3:ObjectCreated:Put",
    "s3:ObjectCreated:Post",
    "s3:ObjectCreated:Copy",
    "s3:ObjectCreated:CompleteMultipartUpload",
    "s3:ObjectRemoved:*",
    "s3:ObjectRemoved:Delete",
    "s3:ObjectRemoved:DeleteMarkerCreated",
    "s3:ObjectRestore:*",
    "s3:ObjectRestore:Post",
    "s3:ObjectRestore:Completed",
    "s3:ObjectRestore:Delete",
    "s3:Replication:*",
    "s3:Replication:OperationFailedReplication",
    "s3:Replication:OperationNotTracked",
    "s3:Replication:OperationMissedThreshold",
    "s3:Replication:OperationReplicatedAfterThreshold",
    "s3:LifecycleTransition",
    "s3:IntelligentTiering",
    "s3:ObjectAcl:Put",
    "s3:LifecycleExpiration:*",
    "s3:LifecycleExpiration:Delete",
    "s3:LifecycleExpiration:DeleteMarkerCreated",
    "s3:ObjectTagging:*",
    "s3:ObjectTagging:Put",
    "s3:ObjectTagging:Delete",
    "s3:ObjectAnnotation:*",
    "s3:ObjectAnnotation:Put",
    "s3:ObjectAnnotation:Delete",
    "s3:ObjectRetention:Put",
];

/// Where a configuration's events go, by the ARN it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    /// `arn:aws:sns:<region>:<account>:<name>`: the stream topic `name`.
    Topic(String),
    /// `arn:aws:sqs:<region>:<account>:<name>`: the stream topic `name`,
    /// read as a queue (`sys.sp_s3_queue_*`).
    Queue(String),
}

impl Destination {
    pub fn arn(&self) -> &str {
        match self {
            Destination::Topic(arn) | Destination::Queue(arn) => arn,
        }
    }

    /// The stream topic the ARN names (its last field).
    pub fn topic(&self) -> &str {
        self.arn().rsplit(':').next().unwrap_or_default()
    }
}

/// One configuration: events of keys matching its filter go to its
/// destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationRule {
    pub id: String,
    /// Event types as configured (`s3:ObjectCreated:*`, ...).
    pub events: Vec<String>,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
    pub destination: Destination,
}

/// A bucket's notification configuration, with the region its events
/// name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationConfig {
    pub region: String,
    pub rules: Vec<NotificationRule>,
}

const FORMAT: u8 = 1;

fn corrupt() -> StorageError {
    StorageError::InvalidFile("corrupt notification configuration".to_string())
}

impl NotificationConfig {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(FORMAT);
        out.str(&self.region);
        out.u32(self.rules.len() as u32);
        for rule in &self.rules {
            out.str(&rule.id);
            out.u32(rule.events.len() as u32);
            for event in &rule.events {
                out.str(event);
            }
            for affix in [&rule.prefix, &rule.suffix] {
                match affix {
                    None => out.u8(0),
                    Some(text) => {
                        out.u8(1);
                        out.str(text);
                    }
                }
            }
            match &rule.destination {
                Destination::Topic(arn) => {
                    out.u8(0);
                    out.str(arn);
                }
                Destination::Queue(arn) => {
                    out.u8(1);
                    out.str(arn);
                }
            }
        }
        out.0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "notification configuration");
        if input.u8()? != FORMAT {
            return Err(corrupt());
        }
        let region = input.str()?;
        let mut rules = Vec::new();
        for _ in 0..input.count()? {
            let id = input.str()?;
            let mut events = Vec::new();
            for _ in 0..input.count()? {
                events.push(input.str()?);
            }
            let mut affix = || -> Result<Option<String>, StorageError> {
                match input.u8()? {
                    0 => Ok(None),
                    1 => Ok(Some(input.str()?)),
                    _ => Err(corrupt()),
                }
            };
            let (prefix, suffix) = (affix()?, affix()?);
            let destination = match input.u8()? {
                0 => Destination::Topic(input.str()?),
                1 => Destination::Queue(input.str()?),
                _ => return Err(corrupt()),
            };
            rules.push(NotificationRule {
                id,
                events,
                prefix,
                suffix,
                destination,
            });
        }
        if !input.rest().is_empty() {
            return Err(corrupt());
        }
        Ok(NotificationConfig { region, rules })
    }

    /// Two configurations that could both send one event: one event type in
    /// common (a wildcard covers its events), and filters some key matches
    /// both of (S3 refuses such a configuration).
    pub fn overlapping(&self) -> Option<(&str, &str)> {
        for (at, first) in self.rules.iter().enumerate() {
            for second in &self.rules[at + 1..] {
                let common_event = first
                    .events
                    .iter()
                    .any(|a| second.events.iter().any(|b| covers(a, b) || covers(b, a)));
                let prefixes =
                    affixes_overlap(&first.prefix, &second.prefix, |a, b| a.starts_with(b));
                let suffixes =
                    affixes_overlap(&first.suffix, &second.suffix, |a, b| a.ends_with(b));
                if common_event && prefixes && suffixes {
                    return Some((&first.id, &second.id));
                }
            }
        }
        None
    }
}

/// Whether configured event type `pattern` covers event type `event`.
fn covers(pattern: &str, event: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(stem) => event.starts_with(stem),
        None => pattern == event,
    }
}

/// Whether some key matches both affixes (none matches every key): one is
/// a prefix (or suffix) of the other.
fn affixes_overlap(a: &Option<String>, b: &Option<String>, within: fn(&str, &str) -> bool) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => within(a, b) || within(b, a),
        _ => true,
    }
}

impl NotificationRule {
    /// Whether this configuration sends event `event` (`s3:...`) of `key`.
    pub fn matches(&self, event: &str, key: &str) -> bool {
        self.events.iter().any(|pattern| covers(pattern, event))
            && self.prefix.as_deref().is_none_or(|p| key.starts_with(p))
            && self.suffix.as_deref().is_none_or(|s| key.ends_with(s))
    }
}

/// The event a journal row is: a notice's own; a change's by the operation
/// that made it (a change without one, by its kind). `None` for a change
/// no event names.
pub fn event_type(record: &JournalRecord) -> Option<&str> {
    let operation = record
        .origin
        .as_ref()
        .map_or("", |origin| origin.operation.as_str());
    Some(match (record.record_type, record.is_delete_marker) {
        (RecordType::Notice, _) => {
            return record
                .notice
                .as_ref()
                .map(|notice| notice.event.as_str())
                .filter(|event| EVENT_TYPES.contains(event));
        }
        (RecordType::Create, _) => match operation {
            "PostObject" => "s3:ObjectCreated:Post",
            "CopyObject" => "s3:ObjectCreated:Copy",
            "CompleteMultipartUpload" => "s3:ObjectCreated:CompleteMultipartUpload",
            _ => "s3:ObjectCreated:Put",
        },
        (RecordType::UpdateMetadata, _) => match operation {
            "PutObjectTagging" => "s3:ObjectTagging:Put",
            "DeleteObjectTagging" => "s3:ObjectTagging:Delete",
            "LifecycleTransition" => "s3:LifecycleTransition",
            _ => return None,
        },
        (RecordType::Delete, Some(true)) => match operation {
            "LifecycleExpiration" => "s3:LifecycleExpiration:DeleteMarkerCreated",
            _ => "s3:ObjectRemoved:DeleteMarkerCreated",
        },
        (RecordType::Delete, _) => match operation {
            "LifecycleExpiration" => "s3:LifecycleExpiration:Delete",
            _ => "s3:ObjectRemoved:Delete",
        },
    })
}

/// A key as S3 writes it in an event: form-encoded
/// (`application/x-www-form-urlencoded`: a space is `+`, a `/` is `%2F`).
fn form_encode(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The S3 event message of journal row `record` for configuration `rule`
/// of `config` (`event` is [`event_type`]'s), for a bucket owned by
/// `owner`.
pub fn event_message(
    record: &JournalRecord,
    event: &str,
    config: &NotificationConfig,
    rule: &NotificationRule,
    owner: &str,
) -> String {
    use serde_json::{Map, Value, json};
    let origin = record.origin.clone().unwrap_or_default();
    // A lifecycle action is S3's own, as is a notice made without a request
    // (a restore completing, or expiring): one with no request id (an
    // anonymous request has one, and no account).
    let by_s3 = event.starts_with("s3:Lifecycle")
        || (record.record_type == RecordType::Notice && origin.request_id.is_empty());
    let principal = match (&origin.account, &origin.user) {
        _ if by_s3 => "s3.amazonaws.com".to_string(),
        (Some(account), Some(user)) => format!("{account}:{user}"),
        (Some(account), None) => account.clone(),
        (None, _) => "ANONYMOUS".to_string(),
    };
    let source_ip = match origin.source_ip {
        _ if by_s3 => "s3.amazonaws.com".to_string(),
        ip => ip.unwrap_or_default(),
    };
    let mut object = Map::new();
    object.insert("key".into(), json!(form_encode(&record.key)));
    // A removal names the version only (a delete marker's size and digest
    // are not an object's).
    if let Some(version) = record
        .object
        .as_ref()
        .filter(|_| record.record_type != RecordType::Delete)
    {
        object.insert("size".into(), json!(version.size));
        let md5: String = version.md5.iter().map(|b| format!("{b:02x}")).collect();
        let etag = match version.parts {
            0 => md5,
            parts => format!("{md5}-{parts}"),
        };
        object.insert("eTag".into(), json!(etag));
    }
    if !record.null_version {
        object.insert("versionId".into(), json!(record.version.to_string()));
    }
    object.insert(
        "sequencer".into(),
        json!(format!("{:016X}", record.sequence)),
    );
    // 2.1 for creations, removals and restores; 2.3 for the event types
    // added with it (lifecycle, tagging, a restored copy expiring); a
    // retention change only exists in the unified schema (2.4 on), 2.6 now.
    let version = if event.starts_with("s3:ObjectCreated")
        || event.starts_with("s3:ObjectRemoved")
        || event == "s3:ObjectRestore:Post"
        || event == "s3:ObjectRestore:Completed"
    {
        "2.1"
    } else if event == "s3:ObjectRetention:Put" {
        "2.6"
    } else {
        "2.3"
    };
    let mut message = json!({
        "Records": [{
            "eventVersion": version,
            "eventSource": "aws:s3",
            "awsRegion": config.region,
            "eventTime": crate::time::iso8601(record.timestamp_ms),
            "eventName": event.trim_start_matches("s3:"),
            "userIdentity": { "principalId": principal },
            "requestParameters": { "sourceIPAddress": source_ip },
            "responseElements": { "x-amz-request-id": origin.request_id },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": rule.id,
                "bucket": {
                    "name": record.bucket,
                    "ownerIdentity": { "principalId": owner },
                    "arn": format!("arn:aws:s3:::{}", record.bucket),
                },
                "object": Value::Object(object),
            },
        }],
    });
    if let Some(Value::Object(fields)) = record
        .notice
        .as_ref()
        .and_then(|notice| notice.detail.as_deref())
        .and_then(|detail| serde_json::from_str(detail).ok())
    {
        for (name, value) in fields {
            message["Records"][0][name] = value;
        }
    }
    if event == "s3:LifecycleTransition"
        && let Some(version) = &record.object
    {
        message["Records"][0]["lifecycleEventData"] = json!({
            "transitionEventData": {
                "destinationStorageClass": version.storage_class.as_str(),
            },
        });
    }
    message.to_string()
}

/// The test message S3 sends a destination when a configuration names it.
pub fn test_message(bucket: &str, request_id: &str, host_id: &str, now_ms: u64) -> String {
    serde_json::json!({
        "Service": "Amazon S3",
        "Event": "s3:TestEvent",
        "Time": crate::time::iso8601(now_ms),
        "Bucket": bucket,
        "RequestId": request_id,
        "HostId": host_id,
    })
    .to_string()
}

/// A history entry's document prefix: `(bucket, 0, journal position, 0)`
/// (its chunks follow, see `documents`).
fn history_prefix(bucket: &str, position: u64) -> Vec<u8> {
    [bucket.as_bytes(), &[0], &position.to_be_bytes(), &[0]].concat()
}

/// The positions of `bucket`'s history entries up to `last`, oldest first.
fn history_positions(
    ctx: &mut RelCtx<'_>,
    tree: &BTree,
    bucket: &str,
    last: u64,
) -> Result<Vec<u64>, StorageError> {
    let at = bucket.len() + 1;
    let mut positions: Vec<u64> = Vec::new();
    for (key, _) in tree.scan_range(
        ctx,
        Some(&history_prefix(bucket, 0)),
        Some(&[&history_prefix(bucket, last)[..], &u16::MAX.to_be_bytes()].concat()),
    )? {
        let position = key
            .get(at..at + 8)
            .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
            .ok_or_else(corrupt)?;
        if positions.last() != Some(&position) {
            positions.push(position);
        }
    }
    Ok(positions)
}

/// A bucket dropped while it had a history, at a journal position: once
/// the consumer is past it, its history up to it goes. Keyed
/// `(1, position, bucket)`, apart from every bucket's entries (a bucket
/// name starts with a letter or digit).
fn ended_key(position: u64, bucket: &str) -> Vec<u8> {
    [&[1u8][..], &position.to_be_bytes(), bucket.as_bytes()].concat()
}

/// `[owner][0: none | 1: configuration]`.
fn encode_history(owner: &str, config: Option<&NotificationConfig>) -> Vec<u8> {
    let mut out = Writer::default();
    out.str(owner);
    match config {
        None => out.u8(0),
        Some(config) => {
            out.u8(1);
            out.0.extend_from_slice(&config.encode());
        }
    }
    out.0
}

fn decode_history(bytes: &[u8]) -> Result<Option<(NotificationConfig, String)>, StorageError> {
    let mut input = Reader::new(bytes, "notification history");
    let owner = input.str()?;
    match input.u8()? {
        0 if input.rest().is_empty() => Ok(None),
        1 => Ok(Some((NotificationConfig::decode(input.rest())?, owner))),
        _ => Err(corrupt()),
    }
}

impl ObjTxn<'_, '_> {
    /// Keeps `config` as `bucket`'s notification configuration (no
    /// configurations: removes it), for the changes made from now on. The
    /// first one starts the consumer at the journal's end: only changes
    /// after it are sent.
    pub fn put_notification(
        &mut self,
        bucket: &str,
        config: &NotificationConfig,
    ) -> Result<(), StorageError> {
        let owner = self
            .bucket_row(bucket)?
            .ok_or_else(|| StorageError::InvalidConfig(format!("no bucket {bucket}")))?
            .owner;
        if config.rules.is_empty() {
            self.delete_bucket_config(bucket, BUCKET_NOTIFICATION, "")?;
            if self.has_notification_history(bucket)? {
                self.record_history(bucket, &owner, None)?;
            }
            return Ok(());
        }
        self.put_bucket_config(bucket, BUCKET_NOTIFICATION, "", &config.encode())?;
        if !self
            .consumers()?
            .iter()
            .any(|consumer| consumer.name == NOTIFICATIONS_CONSUMER)
        {
            let cursor = self.last_change()?;
            self.put_consumer(&Consumer {
                name: NOTIFICATIONS_CONSUMER.to_string(),
                cursor,
                dropped: 0,
            })?;
        }
        self.record_history(bucket, &owner, Some(config))
    }

    /// A bucket being dropped: no configuration from now on (its changes
    /// before are sent as configured when made), and its history forgotten
    /// once they are.
    pub(crate) fn end_notifications(&mut self, bucket: &str) -> Result<(), StorageError> {
        if !self.has_notification_history(bucket)? {
            return Ok(());
        }
        let owner = self
            .bucket_row(bucket)?
            .map(|row| row.owner)
            .unwrap_or_default();
        self.record_history(bucket, &owner, None)?;
        let position = self.last_change()?;
        // Dropped again at the same position (made again with no change
        // between): one mark.
        let tree = self.trees.notify_history();
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, &ended_key(position, bucket))?;
        tree.insert_unique(self.ctx, mode, &ended_key(position, bucket), &[])?;
        let cursor = self.notifications_place()?;
        self.forget_ended(cursor)
    }

    fn has_notification_history(&mut self, bucket: &str) -> Result<bool, StorageError> {
        let tree = self.trees.notify_history();
        Ok(!history_positions(self.ctx, &tree, bucket, u64::MAX)?.is_empty())
    }

    /// The notifications consumer's cursor (the journal's end when there is
    /// none yet).
    fn notifications_place(&mut self) -> Result<u64, StorageError> {
        match self
            .consumers()?
            .into_iter()
            .find(|consumer| consumer.name == NOTIFICATIONS_CONSUMER)
        {
            Some(consumer) => Ok(consumer.cursor),
            None => self.last_change(),
        }
    }

    /// Records `bucket`'s configuration from the journal's end on, and
    /// forgets those no change left to send can use: of the entries the
    /// consumer is past, all but the newest (and that one too when it is
    /// none).
    fn record_history(
        &mut self,
        bucket: &str,
        owner: &str,
        config: Option<&NotificationConfig>,
    ) -> Result<(), StorageError> {
        let position = self.last_change()?;
        let cursor = self.notifications_place()?;
        let tree = self.trees.notify_history();
        let past = history_positions(self.ctx, &tree, bucket, cursor.min(position))?;
        if let Some((&newest, older)) = past.split_last() {
            for &at in older {
                self.delete_document(&tree, &history_prefix(bucket, at))?;
            }
            let row =
                read(self.ctx, &tree, &history_prefix(bucket, newest))?.ok_or_else(corrupt)?;
            if decode_history(&row)?.is_none() {
                self.delete_document(&tree, &history_prefix(bucket, newest))?;
            }
        }
        self.put_document(
            tree,
            &history_prefix(bucket, position),
            &encode_history(owner, config),
        )
    }

    /// Forgets the history of each dropped bucket the consumer is past
    /// (`cursor`): its entries before the drop, and the none it ended with
    /// (a bucket made again since has entries from then on).
    fn forget_ended(&mut self, cursor: u64) -> Result<(), StorageError> {
        let tree = self.trees.notify_history();
        let ended = tree.scan_range(
            self.ctx,
            Some(&ended_key(0, "")),
            Some(&[&ended_key(cursor, "")[..], &[0xFF]].concat()),
        )?;
        for (key, _) in ended {
            let position = key
                .get(1..9)
                .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
                .ok_or_else(corrupt)?;
            let bucket = std::str::from_utf8(&key[9..])
                .map_err(|_| corrupt())?
                .to_string();
            for at in history_positions(self.ctx, &tree, &bucket, position)? {
                let prefix = history_prefix(&bucket, at);
                let none = at == position
                    && decode_history(&read(self.ctx, &tree, &prefix)?.ok_or_else(corrupt)?)?
                        .is_none();
                if at < position || none {
                    self.delete_document(&tree, &prefix)?;
                }
            }
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        Ok(())
    }

    /// Records that the notifications consumer has sent the events of every
    /// change up to `sequence`.
    pub fn advance_notifications(&mut self, sequence: u64) -> Result<(), StorageError> {
        self.advance_consumer(NOTIFICATIONS_CONSUMER, sequence)?;
        let cursor = self.notifications_place()?;
        self.forget_ended(cursor)
    }
}

impl ObjFile<'_> {
    /// `bucket`'s notification configuration, if it has one.
    pub fn notification(
        &mut self,
        bucket: &str,
    ) -> Result<Option<NotificationConfig>, StorageError> {
        self.bucket_config(bucket, BUCKET_NOTIFICATION, "")?
            .map(|bytes| NotificationConfig::decode(&bytes))
            .transpose()
    }

    /// The notification configuration and owner `bucket` had when change
    /// `sequence` was made, if it had one.
    pub fn notification_at(
        &mut self,
        bucket: &str,
        sequence: u64,
    ) -> Result<Option<(NotificationConfig, String)>, StorageError> {
        let Some(tree) = self.existing_tree(NOTIFY_HISTORY_TREE)? else {
            return Ok(None);
        };
        let Some(before) = sequence.checked_sub(1) else {
            return Ok(None);
        };
        let mut ctx = self.core.page_ctx(0);
        let Some(&newest) = history_positions(&mut ctx, &tree, bucket, before)?.last() else {
            return Ok(None);
        };
        decode_history(
            &read(&mut ctx, &tree, &history_prefix(bucket, newest))?.ok_or_else(corrupt)?,
        )
    }

    /// The last change the notifications consumer has sent the events of;
    /// `None` while no bucket has had a configuration.
    pub fn notifications_cursor(&mut self) -> Result<Option<u64>, StorageError> {
        Ok(self
            .journal_consumers()?
            .into_iter()
            .find(|consumer| consumer.name == NOTIFICATIONS_CONSUMER)
            .map(|consumer| consumer.cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{JournalObject, Origin};
    use crate::objects::StorageClass;

    fn rule(
        id: &str,
        events: &[&str],
        prefix: Option<&str>,
        suffix: Option<&str>,
    ) -> NotificationRule {
        NotificationRule {
            id: id.into(),
            events: events.iter().map(|e| e.to_string()).collect(),
            prefix: prefix.map(str::to_string),
            suffix: suffix.map(str::to_string),
            destination: Destination::Topic("arn:aws:sns:us-east-1:123456789012:events".into()),
        }
    }

    #[test]
    fn a_configuration_round_trips_and_corruption_is_refused() {
        let config = NotificationConfig {
            region: "us-east-1".into(),
            rules: vec![
                rule("a", &["s3:ObjectCreated:*"], Some("logs/"), None),
                NotificationRule {
                    destination: Destination::Queue(
                        "arn:aws:sqs:us-east-1:123456789012:work".into(),
                    ),
                    ..rule(
                        "b",
                        &["s3:ObjectRemoved:Delete", "s3:ObjectTagging:Put"],
                        None,
                        Some(".png"),
                    )
                },
            ],
        };
        assert_eq!(config.rules[1].destination.topic(), "work");
        let bytes = config.encode();
        assert_eq!(NotificationConfig::decode(&bytes).unwrap(), config);
        for cut in 0..bytes.len() {
            assert!(
                NotificationConfig::decode(&bytes[..cut]).is_err(),
                "cut at {cut}"
            );
        }
        assert!(NotificationConfig::decode(&[bytes, vec![0]].concat()).is_err());
        assert_eq!(config.rules[0].destination.topic(), "events");
    }

    #[test]
    fn configurations_overlap_on_a_common_event_and_overlapping_filters() {
        let overlap = |a: NotificationRule, b: NotificationRule| {
            NotificationConfig {
                region: String::new(),
                rules: vec![a, b],
            }
            .overlapping()
            .is_some()
        };
        let created = &["s3:ObjectCreated:*"];
        let put = &["s3:ObjectCreated:Put"];
        // No filter matches every key.
        assert!(overlap(
            rule("a", created, None, None),
            rule("b", put, Some("x/"), None)
        ));
        // Different events never overlap.
        assert!(!overlap(
            rule("a", created, None, None),
            rule("b", &["s3:ObjectRemoved:*"], None, None)
        ));
        // Prefixes overlap; suffixes that cannot both end a key do not.
        assert!(!overlap(
            rule("a", put, Some("images/"), Some(".png")),
            rule("b", put, Some("images/"), Some(".jpg"))
        ));
        assert!(overlap(
            rule("a", put, Some("images/"), Some(".jpg")),
            rule("b", put, Some("images/2024/"), Some("x.jpg"))
        ));
        assert!(!overlap(
            rule("a", put, Some("images/"), None),
            rule("b", put, Some("logs/"), None)
        ));
    }

    #[test]
    fn a_rule_matches_its_events_and_keys() {
        let r = rule(
            "a",
            &["s3:ObjectCreated:*", "s3:ObjectRemoved:Delete"],
            Some("in/"),
            Some(".csv"),
        );
        assert!(r.matches("s3:ObjectCreated:Copy", "in/a.csv"));
        assert!(r.matches("s3:ObjectRemoved:Delete", "in/a.csv"));
        assert!(!r.matches("s3:ObjectRemoved:DeleteMarkerCreated", "in/a.csv"));
        assert!(!r.matches("s3:ObjectCreated:Put", "out/a.csv"));
        assert!(!r.matches("s3:ObjectCreated:Put", "in/a.json"));
    }

    fn record(record_type: RecordType, marker: Option<bool>, operation: &str) -> JournalRecord {
        JournalRecord {
            sequence: 0x2A,
            record_type,
            timestamp_ms: 1_369_353_600_123,
            bucket: "photos".into(),
            key: "red flower/é.jpg".into(),
            version: 7,
            null_version: false,
            is_delete_marker: marker,
            object: (record_type != RecordType::Delete).then_some(JournalObject {
                size: 1024,
                md5: [0xAB; 16],
                parts: 3,
                last_modified_ms: 5,
                storage_class: StorageClass::Standard,
            }),
            origin: Some(Origin {
                operation: operation.into(),
                request_id: "0123456789ABCDEF".into(),
                account: Some("123456789012".into()),
                user: Some("alice".into()),
                source_ip: Some("10.0.0.1".into()),
            }),
            notice: None,
        }
    }

    #[test]
    fn journal_rows_name_their_events() {
        use RecordType::*;
        for (record_type, marker, operation, event) in [
            (
                Create,
                Some(false),
                "PutObject",
                Some("s3:ObjectCreated:Put"),
            ),
            (
                Create,
                Some(false),
                "PostObject",
                Some("s3:ObjectCreated:Post"),
            ),
            (
                Create,
                Some(false),
                "CopyObject",
                Some("s3:ObjectCreated:Copy"),
            ),
            (
                Create,
                Some(false),
                "CompleteMultipartUpload",
                Some("s3:ObjectCreated:CompleteMultipartUpload"),
            ),
            (Create, Some(false), "Import", Some("s3:ObjectCreated:Put")),
            (
                UpdateMetadata,
                Some(false),
                "PutObjectTagging",
                Some("s3:ObjectTagging:Put"),
            ),
            (
                UpdateMetadata,
                Some(false),
                "DeleteObjectTagging",
                Some("s3:ObjectTagging:Delete"),
            ),
            (
                UpdateMetadata,
                Some(false),
                "LifecycleTransition",
                Some("s3:LifecycleTransition"),
            ),
            (UpdateMetadata, Some(false), "PutObject", None),
            (
                Delete,
                Some(true),
                "DeleteObject",
                Some("s3:ObjectRemoved:DeleteMarkerCreated"),
            ),
            (
                Delete,
                Some(true),
                "LifecycleExpiration",
                Some("s3:LifecycleExpiration:DeleteMarkerCreated"),
            ),
            (
                Delete,
                None,
                "DeleteObjects",
                Some("s3:ObjectRemoved:Delete"),
            ),
            (
                Delete,
                None,
                "LifecycleExpiration",
                Some("s3:LifecycleExpiration:Delete"),
            ),
        ] {
            assert_eq!(
                event_type(&record(record_type, marker, operation)),
                event,
                "{record_type:?} {marker:?} {operation}"
            );
        }
    }

    #[test]
    fn an_event_message_has_s3s_fields() {
        let config = NotificationConfig {
            region: "eu-west-1".into(),
            rules: vec![rule("cfg", &["s3:ObjectCreated:*"], None, None)],
        };
        let created = record(RecordType::Create, Some(false), "PutObject");
        let message: serde_json::Value = serde_json::from_str(&event_message(
            &created,
            "s3:ObjectCreated:Put",
            &config,
            &config.rules[0],
            "123456789012",
        ))
        .unwrap();
        let event = &message["Records"][0];
        assert_eq!(event["eventVersion"], "2.1");
        assert_eq!(event["eventSource"], "aws:s3");
        assert_eq!(event["awsRegion"], "eu-west-1");
        assert_eq!(event["eventTime"], "2013-05-24T00:00:00.123Z");
        assert_eq!(event["eventName"], "ObjectCreated:Put");
        assert_eq!(event["userIdentity"]["principalId"], "123456789012:alice");
        assert_eq!(event["requestParameters"]["sourceIPAddress"], "10.0.0.1");
        assert_eq!(
            event["responseElements"]["x-amz-request-id"],
            "0123456789ABCDEF"
        );
        let s3 = &event["s3"];
        assert_eq!(s3["s3SchemaVersion"], "1.0");
        assert_eq!(s3["configurationId"], "cfg");
        assert_eq!(s3["bucket"]["name"], "photos");
        assert_eq!(s3["bucket"]["ownerIdentity"]["principalId"], "123456789012");
        assert_eq!(s3["bucket"]["arn"], "arn:aws:s3:::photos");
        assert_eq!(s3["object"]["key"], "red+flower%2F%C3%A9.jpg");
        assert_eq!(s3["object"]["size"], 1024);
        assert_eq!(s3["object"]["eTag"], format!("{}-3", "ab".repeat(16)));
        assert_eq!(s3["object"]["versionId"], "7");
        assert_eq!(s3["object"]["sequencer"], "000000000000002A");
        // A version removed for good has no size or ETag; a null version no
        // version id.
        let mut removed = record(RecordType::Delete, None, "DeleteObject");
        removed.object = None;
        removed.null_version = true;
        let message: serde_json::Value = serde_json::from_str(&event_message(
            &removed,
            "s3:ObjectRemoved:Delete",
            &config,
            &config.rules[0],
            "123456789012",
        ))
        .unwrap();
        let object = message["Records"][0]["s3"]["object"].as_object().unwrap();
        let mut fields: Vec<&str> = object.keys().map(String::as_str).collect();
        fields.sort();
        assert_eq!(fields, ["key", "sequencer"]);
        // A delete marker: its version, no size or ETag.
        let mut marker = record(RecordType::Delete, Some(true), "DeleteObject");
        marker.object = Some(JournalObject {
            size: 0,
            md5: [0; 16],
            parts: 0,
            last_modified_ms: 5,
            storage_class: StorageClass::Standard,
        });
        let message: serde_json::Value = serde_json::from_str(&event_message(
            &marker,
            "s3:ObjectRemoved:DeleteMarkerCreated",
            &config,
            &config.rules[0],
            "123456789012",
        ))
        .unwrap();
        let object = message["Records"][0]["s3"]["object"].as_object().unwrap();
        let mut fields: Vec<&str> = object.keys().map(String::as_str).collect();
        fields.sort();
        assert_eq!(fields, ["key", "sequencer", "versionId"]);
        // A lifecycle transition is S3's, and names the class it went to.
        let mut moved = record(
            RecordType::UpdateMetadata,
            Some(false),
            "LifecycleTransition",
        );
        moved.object.as_mut().unwrap().storage_class = StorageClass::Glacier;
        let message: serde_json::Value = serde_json::from_str(&event_message(
            &moved,
            "s3:LifecycleTransition",
            &config,
            &config.rules[0],
            "123456789012",
        ))
        .unwrap();
        let event = &message["Records"][0];
        assert_eq!(event["eventVersion"], "2.3");
        assert_eq!(event["userIdentity"]["principalId"], "s3.amazonaws.com");
        assert_eq!(
            event["requestParameters"]["sourceIPAddress"],
            "s3.amazonaws.com"
        );
        assert_eq!(
            event["lifecycleEventData"]["transitionEventData"]["destinationStorageClass"],
            "GLACIER"
        );
        let created_event: serde_json::Value = serde_json::from_str(&event_message(
            &created,
            "s3:ObjectCreated:Put",
            &config,
            &config.rules[0],
            "123456789012",
        ))
        .unwrap();
        assert!(
            created_event["Records"][0]
                .get("lifecycleEventData")
                .is_none()
        );
    }

    #[test]
    fn a_notice_is_its_own_event_with_its_detail() {
        let config = NotificationConfig {
            region: "eu-west-1".into(),
            rules: vec![rule("cfg", &["s3:ObjectRestore:*"], None, None)],
        };
        let mut completed = record(RecordType::Notice, Some(false), "");
        completed.origin = Some(Origin::default());
        completed.notice = Some(crate::journal::Notice {
            event: "s3:ObjectRestore:Completed".into(),
            detail: Some(r#"{"glacierEventData":{"restoreEventData":{"x":1}}}"#.into()),
        });
        assert_eq!(event_type(&completed), Some("s3:ObjectRestore:Completed"));
        let message: serde_json::Value = serde_json::from_str(&event_message(
            &completed,
            "s3:ObjectRestore:Completed",
            &config,
            &config.rules[0],
            "123456789012",
        ))
        .unwrap();
        let event = &message["Records"][0];
        assert_eq!(event["eventVersion"], "2.1");
        assert_eq!(event["userIdentity"]["principalId"], "s3.amazonaws.com");
        assert_eq!(event["glacierEventData"]["restoreEventData"]["x"], 1);
        assert_eq!(event["s3"]["object"]["size"], 1024);
        // Made by a request: its requester.
        let mut posted = record(RecordType::Notice, Some(false), "RestoreObject");
        posted.notice = Some(crate::journal::Notice {
            event: "s3:ObjectRestore:Post".into(),
            detail: None,
        });
        let message: serde_json::Value = serde_json::from_str(&event_message(
            &posted,
            "s3:ObjectRestore:Post",
            &config,
            &config.rules[0],
            "123456789012",
        ))
        .unwrap();
        assert_eq!(
            message["Records"][0]["userIdentity"]["principalId"],
            "123456789012:alice"
        );
        // An anonymous request's: no account, but a request id and address.
        posted.origin.as_mut().unwrap().account = None;
        posted.origin.as_mut().unwrap().user = None;
        let message: serde_json::Value = serde_json::from_str(&event_message(
            &posted,
            "s3:ObjectRestore:Post",
            &config,
            &config.rules[0],
            "123456789012",
        ))
        .unwrap();
        let event = &message["Records"][0];
        assert_eq!(event["userIdentity"]["principalId"], "ANONYMOUS");
        assert_eq!(event["requestParameters"]["sourceIPAddress"], "10.0.0.1");
        // An event no configuration may name is none.
        posted.notice.as_mut().unwrap().event = "s3:Nope".into();
        assert_eq!(event_type(&posted), None);
    }
}
