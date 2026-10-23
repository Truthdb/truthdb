//! The streams engine in the server (see `truthdb_streams`): its
//! administration commands, its storage access, and its share of the
//! maintenance sweep, which removes dropped topics' records and trims
//! records past their topic's retention, a bounded number of budgeted
//! transactions per sweep. A standby does none of the maintenance (its log
//! is the primary's).

use serde_json::json;
use truthdb_core::storage::Locked;
use truthdb_object::ObjFile;
use truthdb_relational::storage::StorageError;
use truthdb_streams::{MAX_PARTITIONS, StreamError, StreamFile};

use super::{CommandError, Engine, EngineError, OBJECT_CHECKPOINT_THRESHOLD, render_json};

/// Budgeted transactions one maintenance sweep spends on each of purging
/// and trimming at most.
const STREAM_MAINTENANCE_STEPS: usize = 16;
/// Rows one purge transaction removes, and records one trim transaction
/// removes, at most.
const STREAM_STEP_ROWS: usize = 256;
/// How long a topic keeps its records unless told otherwise: Kafka's
/// default (`log.retention.hours` = 168).
const DEFAULT_RETENTION_HOURS: u64 = 168;

/// Streams administration: `stream create topic <name> [partitions <n>]
/// [retention <hours>] [account <S3 account id>]` (retention 0: for ever;
/// the account: whose buckets may send it their event notifications),
/// `stream drop topic <name>`, `stream create webhook <topic> url <url>
/// secret <secret>` (see `truthdb_streams::Webhook`) and `stream drop
/// webhook <topic>`. Gated like the S3 administration commands: a
/// `sysadmin` login over TDS, any connection on the (trusted) native port.
pub(super) enum StreamAdmin {
    CreateTopic {
        name: String,
        /// The `option value` pairs after the name, as written.
        options: Vec<String>,
    },
    DropTopic(String),
    CreateWebhook {
        topic: String,
        url: String,
        secret: String,
    },
    DropWebhook(String),
}

pub(super) fn parse_stream_admin(input: &str) -> Option<StreamAdmin> {
    // The CLI ends every command with `;` (one: a secret may end in one).
    let input = input.trim();
    let input = input.strip_suffix(';').unwrap_or(input);
    let words: Vec<&str> = input.split_whitespace().collect();
    let lower: Vec<String> = words.iter().map(|word| word.to_ascii_lowercase()).collect();
    let lower: Vec<&str> = lower.iter().map(String::as_str).collect();
    match lower.as_slice() {
        ["stream", "create", "topic", _, ..] => Some(StreamAdmin::CreateTopic {
            name: words[3].to_string(),
            options: words[4..].iter().map(|word| word.to_string()).collect(),
        }),
        ["stream", "drop", "topic", _] => Some(StreamAdmin::DropTopic(words[3].to_string())),
        ["stream", "create", "webhook", _, "url", _, "secret", _] => {
            Some(StreamAdmin::CreateWebhook {
                topic: words[3].to_string(),
                url: words[5].to_string(),
                secret: words[7].to_string(),
            })
        }
        ["stream", "drop", "webhook", _] => Some(StreamAdmin::DropWebhook(words[3].to_string())),
        _ => None,
    }
}

/// A database without one of its engines.
pub(super) fn no_engine() -> StorageError {
    StorageError::InvalidConfig("this database does not run every engine".to_string())
}

fn stream_error(topic: &str, err: StreamError) -> EngineError {
    CommandError::InvalidCommand(match err {
        StreamError::NoSuchTopic => format!("no topic '{topic}'"),
        StreamError::TopicExists => format!("topic '{topic}' already exists"),
        StreamError::InvalidName(why) => why.to_string(),
        StreamError::InvalidPartition => format!("no such partition in topic '{topic}'"),
        StreamError::InvalidPartitions => {
            format!("a topic has 1 to {MAX_PARTITIONS} partitions")
        }
        StreamError::TooLarge => "the records are too large".to_string(),
    })
    .into()
}

fn webhook_error(topic: &str, err: truthdb_streams::WebhookError) -> EngineError {
    use truthdb_streams::WebhookError;
    CommandError::InvalidCommand(match err {
        WebhookError::NoSuchTopic => format!("no topic '{topic}'"),
        WebhookError::WebhookExists => format!("topic '{topic}' already has a webhook"),
        WebhookError::NoSuchWebhook => format!("topic '{topic}' has no webhook"),
        WebhookError::NameTooLong => {
            format!("the dead-letter topic of '{topic}' would be named past 249 characters")
        }
        WebhookError::DeadLetterTooSmall => format!(
            "the dead-letter topic of '{topic}' has fewer partitions than it; drop it first"
        ),
        WebhookError::UrlTooLong => format!(
            "a webhook URL is at most {} bytes",
            truthdb_streams::MAX_URL_BYTES
        ),
        WebhookError::SecretTooLong => format!(
            "a webhook secret is at most {} bytes",
            truthdb_streams::MAX_SECRET_BYTES
        ),
    })
    .into()
}

impl Engine {
    pub(super) fn execute_stream_admin(&self, command: StreamAdmin) -> Result<String, EngineError> {
        let invalid = |why: String| EngineError::from(CommandError::InvalidCommand(why));
        let rendered = match command {
            StreamAdmin::CreateTopic { name, options } => {
                let (mut partitions, mut retention_hours, mut account) = (None, None, None);
                for pair in options.chunks(2) {
                    let [option, value] = pair else {
                        return Err(invalid(format!("'{}' has no value", pair[0])));
                    };
                    let slot = match option.to_ascii_lowercase().as_str() {
                        "partitions" => &mut partitions,
                        "retention" => &mut retention_hours,
                        "account" if account.is_some() => {
                            return Err(invalid(format!("'{option}' is given twice")));
                        }
                        "account" => {
                            account = Some(value.clone());
                            continue;
                        }
                        _ => return Err(invalid(format!("unknown topic option '{option}'"))),
                    };
                    if slot.is_some() {
                        return Err(invalid(format!("'{option}' is given twice")));
                    }
                    *slot = Some(value.parse::<u64>().map_err(|_| {
                        invalid(format!("{option} takes a whole number, not '{value}'"))
                    })?);
                }
                let partitions = u32::try_from(partitions.unwrap_or(1))
                    .map_err(|_| stream_error(&name, StreamError::InvalidPartitions))?;
                let retention_hours = retention_hours.unwrap_or(DEFAULT_RETENTION_HOURS);
                let retention_ms = retention_hours
                    .checked_mul(3_600_000)
                    // Shown as a `bigint` in `sys.stream_topics`.
                    .filter(|ms| i64::try_from(*ms).is_ok())
                    .ok_or_else(|| invalid(format!("a retention of {retention_hours} hours")))?;
                let now_ms = super::now_ms();
                // The account checked and the topic made in one hold: an
                // account dropped meanwhile cannot end up owning it.
                self.locked(|guard| {
                    if let Some(id) = &account {
                        let mut objects = ObjFile::try_new(guard).ok_or_else(no_engine)?;
                        if objects.account(id)?.is_none() {
                            return Ok(Err(None));
                        }
                    }
                    StreamFile::try_new(guard)
                        .ok_or_else(no_engine)?
                        .transaction(|tx| {
                            tx.create_topic(
                                &name,
                                partitions,
                                retention_ms,
                                account.as_deref(),
                                now_ms,
                            )
                        })
                        .map(|created| created.map_err(Some))
                })?
                .map_err(|err| match err {
                    None => invalid(format!(
                        "no S3 account '{}'",
                        account.as_deref().unwrap_or_default()
                    )),
                    Some(err) => stream_error(&name, err),
                })?;
                json!({
                    "topic": name,
                    "partitions": partitions,
                    "retention_hours": retention_hours,
                    "account": account,
                })
            }
            StreamAdmin::DropTopic(name) => {
                let topic = name.clone();
                self.streams(move |file| file.transaction(|tx| tx.drop_topic(&topic)))?
                    .map_err(|err| stream_error(&name, err))?;
                json!({ "topic": name, "dropped": true })
            }
            StreamAdmin::CreateWebhook { topic, url, secret } => {
                truthdb_s3client::post_target(&url).map_err(|err| invalid(err.to_string()))?;
                let now_ms = super::now_ms();
                let (name, target) = (topic.clone(), url.clone());
                self.streams(move |file| {
                    file.transaction(|tx| tx.create_webhook(&name, &target, &secret, now_ms))
                })?
                .map_err(|err| webhook_error(&topic, err))?;
                json!({
                    "topic": topic,
                    "url": url,
                    "dead_letter_topic": truthdb_streams::dead_letter_topic(&topic),
                })
            }
            StreamAdmin::DropWebhook(topic) => {
                let name = topic.clone();
                self.streams(move |file| file.transaction(|tx| tx.drop_webhook(&name)))?
                    .map_err(|err| webhook_error(&topic, err))?;
                json!({ "topic": topic, "webhook_dropped": true })
            }
        };
        render_json(&rendered).map_err(EngineError::from)
    }

    /// Runs `op` under the storage lock (for an operation on more than one
    /// engine in one hold), then checkpoints if the log is filling.
    pub(super) fn locked<T>(
        &self,
        op: impl FnOnce(&mut Locked) -> Result<T, StorageError>,
    ) -> Result<T, EngineError> {
        let out = {
            let core = self.storage.core_arc();
            let mut guard = core.lock();
            op(&mut guard)
        };
        if !self.storage.is_standby() {
            let meta = self.meta.read().expect("engine meta poisoned");
            if let Err(err) = self.checkpoint_at(&meta, OBJECT_CHECKPOINT_THRESHOLD) {
                eprintln!("checkpoint failed: {err}");
            }
        }
        Ok(out?)
    }

    /// Runs `op` on the streams engine under the storage lock, then
    /// checkpoints if the log is filling.
    pub fn streams<T>(
        &self,
        op: impl FnOnce(&mut StreamFile<'_>) -> Result<T, StorageError>,
    ) -> Result<T, EngineError> {
        let out = {
            let core = self.storage.core_arc();
            let mut guard = core.lock();
            match StreamFile::try_new(&mut guard) {
                Some(mut file) => op(&mut file),
                None => Err(StorageError::InvalidConfig(
                    "this database does not run the streams engine".to_string(),
                )),
            }
        };
        // Also after a failure, as for the object engine.
        if !self.storage.is_standby() {
            let meta = self.meta.read().expect("engine meta poisoned");
            if let Err(err) = self.checkpoint_at(&meta, OBJECT_CHECKPOINT_THRESHOLD) {
                eprintln!("streams checkpoint failed: {err}");
            }
        }
        Ok(out?)
    }

    /// The maintenance sweep's streams work: dropped topics' records, then
    /// records past their retention.
    pub(crate) fn streams_maintenance(&self) -> Result<(), EngineError> {
        if self.storage.is_standby() {
            return Ok(());
        }
        let runs = {
            let core = self.storage.core_arc();
            let mut guard = core.lock();
            StreamFile::try_new(&mut guard).is_some()
        };
        if !runs {
            return Ok(());
        }
        if self.streams(|file| file.purge_pending())? {
            for _ in 0..STREAM_MAINTENANCE_STEPS {
                if self.streams(|file| file.transaction(|tx| tx.purge_step(STREAM_STEP_ROWS)))? {
                    break;
                }
            }
        }
        let now_ms = super::now_ms();
        if self.streams(|file| file.trim_due(now_ms))? {
            for _ in 0..STREAM_MAINTENANCE_STEPS {
                let (_, more) = self.streams(|file| {
                    file.transaction(|tx| tx.trim_step(now_ms, STREAM_STEP_ROWS))
                })?;
                if !more {
                    break;
                }
            }
        }
        Ok(())
    }
}
