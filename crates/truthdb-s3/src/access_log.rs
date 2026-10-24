//! Server access logs: a line per request to a bucket whose logging is on
//! (see `logging_config`), in S3's format, written as objects into its
//! target bucket. Lines are kept per bucket and written every
//! `access_log_flush` (five minutes by default) or once a bucket's reach
//! 1 MiB; each write checks again that the target exists, is the bucket
//! owner's and has no Object Lock, and drops the lines otherwise. As in
//! S3, delivery is best effort: lines not yet written when the server
//! stops abruptly are lost.
//!
//! A line is written when its answer's body ends (its bytes sent and total
//! time are known then). Fields this server does not know are `-`: the TLS
//! cipher and version, access points, ACLs and the source region. The
//! operation is `REST.<METHOD>.<RESOURCE>` (`WEBSITE.<METHOD>.OBJECT` on
//! a website endpoint), with S3's resource names where its documentation
//! shows them (`OBJECT`, `BUCKET`, `VERSIONING`, `LOGGING_STATUS`,
//! `BUCKETPOLICY`) and the sub-resource's name in capitals otherwise. A
//! copy is logged once, by its destination's bucket.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use hyper::Method;
use hyper::body::Incoming;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use truthdb_object::{BUCKET_LOGGING, BodyWriter, ObjectAttrs, Origin, StorageClass};

use crate::logging_config::{DateSource, KeyFormat, LoggingConfig};
use crate::server::{Connection, S3State};

/// A key's characters written escaped (a line's fields are separated by
/// spaces).
const KEY: &AsciiSet = &CONTROLS.add(b' ').add(b'"').add(b'%');
/// A bucket's lines written at once when they reach this size.
const FLUSH_BYTES: usize = 1024 * 1024;
const GRACE_MS: u64 = 60_000;

/// What a request's line says that is known before it is routed.
pub(crate) struct Request {
    operation: String,
    request_uri: String,
    referer: Option<String>,
    user_agent: Option<String>,
    host: Option<String>,
    version_id: Option<String>,
    remote_ip: Option<String>,
}

impl Request {
    pub(crate) fn of(request: &hyper::Request<Incoming>) -> Request {
        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        };
        let query = request.uri().query().unwrap_or("");
        let version_id = query.split('&').find_map(|pair| {
            let (name, value) = pair.split_once('=')?;
            (name == "versionId").then(|| {
                percent_encoding::percent_decode_str(value)
                    .decode_utf8_lossy()
                    .into_owned()
            })
        });
        Request {
            operation: operation(request),
            request_uri: format!(
                "{} {} {:?}",
                request.method(),
                request
                    .uri()
                    .path_and_query()
                    .map_or("/", |path| path.as_str()),
                request.version()
            ),
            referer: header("referer"),
            user_agent: header("user-agent"),
            host: header("host"),
            version_id,
            remote_ip: request
                .extensions()
                .get::<Connection>()
                .map(|connection| connection.peer.ip().to_string()),
        }
    }
}

/// `REST.<METHOD>.<RESOURCE>`, or `WEBSITE.<METHOD>.OBJECT`.
fn operation(request: &hyper::Request<Incoming>) -> String {
    let method = request.method();
    if request
        .extensions()
        .get::<crate::website::WebsiteHost>()
        .is_some()
    {
        return format!("WEBSITE.{method}.OBJECT");
    }
    let object = matches!(
        crate::api::target(request),
        Ok(crate::api::Target::Object(..))
    );
    let has = |name: &str| crate::api::has_param(request, name);
    let (verb, resource) = if object
        && *method == Method::PUT
        && request.headers().contains_key("x-amz-copy-source")
        && !has("uploadId")
        && !has("tagging")
        && !has("acl")
    {
        ("COPY".to_string(), "OBJECT".to_string())
    } else {
        let resource = match crate::api::subresource(request) {
            Some("uploadId") if *method == Method::PUT => "PART".to_string(),
            Some("uploadId") => "UPLOAD".to_string(),
            Some("delete") => "MULTI_OBJECT_DELETE".to_string(),
            Some("logging") => "LOGGING_STATUS".to_string(),
            Some("policy") => "BUCKETPOLICY".to_string(),
            Some(name) => name.to_uppercase().replace('-', "_"),
            None if object => "OBJECT".to_string(),
            None => "BUCKET".to_string(),
        };
        (method.to_string(), resource)
    };
    format!("REST.{verb}.{resource}")
}

/// A request's line, but for what its body's end tells.
pub(crate) struct Entry {
    pub bucket: String,
    pub owner: String,
    pub arrived_ms: u64,
    pub requester: Option<String>,
    pub request_id: String,
    pub host_id: String,
    pub key: Option<String>,
    pub status: u16,
    pub error_code: Option<String>,
    pub object_size: Option<u64>,
    pub turnaround_ms: u64,
    /// `AuthHeader` or `QueryString` (none: anonymous).
    pub auth_type: Option<&'static str>,
    pub request: Request,
}

impl Entry {
    /// The line, with the body's bytes sent and the request's total time.
    pub(crate) fn line(&self, sent: u64, total_ms: u64) -> String {
        let dash = |value: Option<&str>| value.unwrap_or("-").to_string();
        let quoted = |value: Option<&str>| format!("\"{}\"", value.unwrap_or("-"));
        let number = |value: Option<u64>| value.map_or("-".to_string(), |value| value.to_string());
        let request = &self.request;
        let fields = [
            self.owner.clone(),
            self.bucket.clone(),
            format!("[{}]", log_time(self.arrived_ms)),
            dash(request.remote_ip.as_deref()),
            dash(self.requester.as_deref()),
            self.request_id.clone(),
            request.operation.clone(),
            self.key.as_deref().map_or("-".to_string(), |key| {
                utf8_percent_encode(key, KEY).to_string()
            }),
            format!("\"{}\"", request.request_uri),
            self.status.to_string(),
            dash(self.error_code.as_deref()),
            number(Some(sent).filter(|sent| *sent > 0)),
            number(self.object_size),
            total_ms.to_string(),
            self.turnaround_ms.to_string(),
            quoted(request.referer.as_deref()),
            quoted(request.user_agent.as_deref()),
            dash(request.version_id.as_deref()),
            self.host_id.clone(),
            self.auth_type.map_or("-", |_| "SigV4").to_string(),
            "-".to_string(),
            dash(self.auth_type),
            dash(request.host.as_deref()),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ];
        let mut line = fields.join(" ");
        line.push('\n');
        line
    }
}

/// `06/Feb/2019:00:00:38 +0000`.
fn log_time(ms: u64) -> String {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let iso = crate::time::iso8601(ms);
    let month: usize = iso[5..7].parse().unwrap_or(1);
    format!(
        "{}/{}/{}:{} +0000",
        &iso[8..10],
        MONTHS[month.clamp(1, 12) - 1],
        &iso[0..4],
        &iso[11..19]
    )
}

/// A bucket's owner and its logging configuration (none: off).
#[derive(Default)]
struct Logged {
    owner: String,
    config: Option<LoggingConfig>,
}

/// A bucket's lines not yet written, and when the first one's request came.
struct Buffer {
    lines: String,
    first_ms: u64,
}

#[derive(Default)]
pub struct AccessLogger {
    /// Each bucket's logging configuration, read once and forgotten when it
    /// changes (only this server changes it).
    configs: Mutex<HashMap<String, Arc<Logged>>>,
    /// Counts `forget`s: configurations read before one are not kept.
    generation: AtomicU64,
    /// The lines not yet written, by bucket and its owner.
    buffers: Mutex<HashMap<(String, String), Buffer>>,
    /// Woken when a bucket's lines reach `FLUSH_BYTES`.
    full: tokio::sync::Notify,
}

impl AccessLogger {
    /// Forgets `bucket`'s configuration (it changed, or the bucket is gone).
    pub(crate) fn forget(&self, bucket: &str) {
        let mut configs = self
            .configs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.generation.fetch_add(1, Ordering::Relaxed);
        configs.remove(bucket);
    }

    /// Forgets everything (a standby logs nothing).
    fn clear(&self) {
        let mut configs = self
            .configs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.generation.fetch_add(1, Ordering::Relaxed);
        configs.clear();
        self.take();
    }

    async fn logged(&self, state: &S3State, bucket: &str) -> Arc<Logged> {
        if let Some(found) = self
            .configs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(bucket)
        {
            return Arc::clone(found);
        }
        let generation = self.generation.load(Ordering::Relaxed);
        let name = bucket.to_string();
        let loaded = state
            .engine
            .run_object(move |file| {
                let Some(info) = file.bucket(&name)? else {
                    return Ok(Logged::default());
                };
                let config = file
                    .bucket_config(&name, BUCKET_LOGGING, "")?
                    .and_then(|document| crate::logging_config::parse(&document).ok().flatten());
                Ok(Logged {
                    owner: info.owner,
                    config,
                })
            })
            .await;
        // Not read (the engine busy or gone): nothing logged, nothing kept.
        let Ok(logged) = loaded else {
            return Arc::new(Logged::default());
        };
        let logged = Arc::new(logged);
        let mut cache = self
            .configs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.generation.load(Ordering::Relaxed) == generation {
            cache.insert(bucket.to_string(), Arc::clone(&logged));
        }
        logged
    }

    /// The owner of `bucket` if a request to it is logged: its logging is
    /// on, and it is still `owner`'s (when known; another account's bucket
    /// of the name, made while the request ran, gets no line of it).
    pub(crate) async fn logs(
        &self,
        state: &S3State,
        bucket: &str,
        owner: Option<&str>,
    ) -> Option<String> {
        let logged = self.logged(state, bucket).await;
        (logged.config.is_some() && owner.is_none_or(|owner| owner == logged.owner))
            .then(|| logged.owner.clone())
    }

    /// Keeps `entry`'s line, finished with what its body's end tells.
    pub(crate) fn record(&self, entry: &Entry, sent: u64, total_ms: u64) {
        let line = entry.line(sent, total_ms);
        let mut buffers = self
            .buffers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let buffer = buffers
            .entry((entry.bucket.clone(), entry.owner.clone()))
            .or_insert_with(|| Buffer {
                lines: String::new(),
                first_ms: entry.arrived_ms,
            });
        buffer.lines.push_str(&line);
        if buffer.lines.len() >= FLUSH_BYTES {
            self.full.notify_one();
        }
    }

    fn take(&self) -> Vec<((String, String), Buffer)> {
        std::mem::take(
            &mut *self
                .buffers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
        .into_iter()
        .collect()
    }

    /// Writes each bucket's lines into its target (put back to try again
    /// if the engine could not be reached).
    pub(crate) async fn flush(&self, state: &S3State) {
        for ((bucket, owner), buffer) in self.take() {
            if let Err(buffer) = deliver(state, &bucket, &owner, buffer).await {
                let mut buffers = self
                    .buffers
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let kept = buffers.entry((bucket, owner)).or_insert_with(|| Buffer {
                    lines: String::new(),
                    first_ms: buffer.first_ms,
                });
                kept.lines.insert_str(0, &buffer.lines);
                kept.first_ms = kept.first_ms.min(buffer.first_ms);
            }
        }
    }
}

/// Writes `buffer` as one log object of `bucket` (`owner`'s), if its
/// logging is still on and its target fit; the buffer back if the engine
/// could not be reached.
async fn deliver(state: &S3State, bucket: &str, owner: &str, buffer: Buffer) -> Result<(), Buffer> {
    let now = crate::objects::now_ms();
    let storage = Arc::clone(&state.storage);
    let text = buffer.lines.clone();
    let sealed = tokio::task::spawn_blocking(move || {
        let mut writer = BodyWriter::begin(&storage, now)?;
        writer.write(&storage, text.as_bytes(), now)?;
        writer.finish(&storage, now)
    })
    .await;
    let Ok(Ok(sealed)) = sealed else {
        return Err(buffer);
    };
    let (bucket, owner) = (bucket.to_string(), owner.to_string());
    let region = state.region.clone();
    let first_ms = buffer.first_ms;
    let written = state
        .engine
        .run_object(move |file| {
            // The bucket still the owner's, its logging on, and its target
            // still one it may have: the owner's, without Object Lock.
            if file.bucket(&bucket)?.is_none_or(|info| info.owner != owner) {
                return Ok(());
            }
            let Some(config) = file
                .bucket_config(&bucket, BUCKET_LOGGING, "")?
                .and_then(|document| crate::logging_config::parse(&document).ok().flatten())
            else {
                return Ok(());
            };
            if file
                .bucket(&config.target_bucket)?
                .is_none_or(|target| target.owner != owner || target.object_lock)
            {
                return Ok(());
            }
            let key = log_key(&config, &owner, &region, &bucket, first_ms, now);
            file.set_origin(Origin {
                operation: "PutObject".to_string(),
                request_id: unique(),
                account: Some(owner.clone()),
                ..Origin::default()
            });
            let attrs = ObjectAttrs {
                content_type: Some("text/plain".to_string()),
                ..ObjectAttrs::default()
            };
            file.transaction(|tx| {
                tx.put_object(
                    &config.target_bucket,
                    &key,
                    &sealed,
                    &attrs,
                    StorageClass::Standard,
                    now,
                    GRACE_MS,
                )
                .map(|_| ())
            })
        })
        .await;
    match written {
        Ok(()) => Ok(()),
        _ => Err(buffer),
    }
}

/// A log object's key: the prefix, then (partitioned) the source's account,
/// region, bucket and date, then the time and a unique string.
fn log_key(
    config: &LoggingConfig,
    owner: &str,
    region: &str,
    bucket: &str,
    first_ms: u64,
    now_ms: u64,
) -> String {
    let stamp = |ms: u64| {
        let iso = crate::time::iso8601(ms);
        format!(
            "{}-{}-{}-{}-{}-{}",
            &iso[0..4],
            &iso[5..7],
            &iso[8..10],
            &iso[11..13],
            &iso[14..16],
            &iso[17..19]
        )
    };
    let name = format!("{}-{}", stamp(now_ms), unique());
    match config.key_format {
        None | Some(KeyFormat::Simple) => format!("{}{name}", config.target_prefix),
        Some(KeyFormat::Partitioned(source)) => {
            let dated = match source {
                DateSource::EventTime => first_ms,
                DateSource::DeliveryTime => now_ms,
            };
            let iso = crate::time::iso8601(dated);
            format!(
                "{}{owner}/{region}/{bucket}/{}/{}/{}/{name}",
                config.target_prefix,
                &iso[0..4],
                &iso[5..7],
                &iso[8..10]
            )
        }
    }
}

/// 16 random hexadecimal digits.
fn unique() -> String {
    use ring::rand::SecureRandom;
    let mut bytes = [0u8; 8];
    let _ = ring::rand::SystemRandom::new().fill(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

/// Writes the logs every `state.access_log_flush`, or when a bucket's
/// reach `FLUSH_BYTES`, until `shutdown`, and once more then. A standby
/// writes nothing.
pub async fn run(state: Arc<S3State>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    loop {
        let stopping = tokio::select! {
            _ = tokio::time::sleep(state.access_log_flush) => false,
            _ = state.access_logger.full.notified() => false,
            changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        };
        if state.storage.is_standby() {
            state.access_logger.clear();
        } else {
            state.access_logger.flush(&state).await;
        }
        if stopping {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_whose_lines_reach_a_mebibyte_is_written_at_once() {
        let logger = AccessLogger::default();
        let entry = |uri_bytes: usize| Entry {
            bucket: "src".into(),
            owner: "111122223333".into(),
            arrived_ms: 0,
            requester: None,
            request_id: "R".into(),
            host_id: "H".into(),
            key: None,
            status: 200,
            error_code: None,
            object_size: None,
            turnaround_ms: 0,
            auth_type: None,
            request: Request {
                operation: "REST.GET.BUCKET".into(),
                request_uri: "x".repeat(uri_bytes),
                referer: None,
                user_agent: None,
                host: None,
                version_id: None,
                remote_ip: None,
            },
        };
        let woken = || {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap()
                .block_on(async {
                    tokio::time::timeout(
                        std::time::Duration::from_millis(50),
                        logger.full.notified(),
                    )
                    .await
                    .is_ok()
                })
        };
        logger.record(&entry(1000), 0, 0);
        assert!(!woken(), "a small bucket waits for the next flush");
        logger.record(&entry(FLUSH_BYTES), 0, 0);
        assert!(woken());
    }

    #[test]
    fn log_times_and_keys_are_written_as_s3_writes_them() {
        // 2019-02-06T00:00:38Z
        let ms = 1_549_411_238_000;
        assert_eq!(log_time(ms), "06/Feb/2019:00:00:38 +0000");
        let config = |key_format| LoggingConfig {
            target_bucket: "logs".into(),
            target_prefix: "p/".into(),
            key_format,
        };
        let simple = log_key(&config(None), "111122223333", "us-east-1", "src", 0, ms);
        assert!(simple.starts_with("p/2019-02-06-00-00-38-"), "{simple}");
        assert_eq!(simple.len(), "p/2019-02-06-00-00-38-".len() + 16);
        // Partitioned by the first request's day, or the delivery's.
        let day_before = ms - 86_400_000;
        let by_event = log_key(
            &config(Some(KeyFormat::Partitioned(DateSource::EventTime))),
            "111122223333",
            "us-east-1",
            "src",
            day_before,
            ms,
        );
        assert!(
            by_event.starts_with("p/111122223333/us-east-1/src/2019/02/05/2019-02-06-00-00-38-"),
            "{by_event}"
        );
        let by_delivery = log_key(
            &config(Some(KeyFormat::Partitioned(DateSource::DeliveryTime))),
            "111122223333",
            "us-east-1",
            "src",
            day_before,
            ms,
        );
        assert!(
            by_delivery.starts_with("p/111122223333/us-east-1/src/2019/02/06/"),
            "{by_delivery}"
        );
    }
}
