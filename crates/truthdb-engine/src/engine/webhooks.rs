//! Webhook delivery (see `truthdb_streams::Webhook`), on a thread of its
//! own (see `session::pool`): POSTs go out concurrently, one at a time per
//! webhook partition, and each outcome is recorded as it comes. A POST carries the record as its JSON body (an S3
//! event message: `{"Records":[...]}`), names it in `x-truthdb-delivery:
//! <topic>/<partition>/<offset>` (the same on a retry, so a receiver can drop
//! a repeat), and is signed: `x-truthdb-signature: t=<unix seconds>,v1=<hex
//! HMAC-SHA256 of "<t>.<delivery>.<body>" under the webhook's secret>`, the
//! time when it is sent. A 2xx answer delivers it; anything else, or no
//! answer within 10 seconds (more for a large body), is a failed try.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use truthdb_s3client::{Bytes, Poster};
use truthdb_streams::{Delivery, StreamFile};

use super::{Engine, EngineError};

/// POSTs in flight at most (one per webhook partition), and at most to one
/// endpoint (host and port): an endpoint that hangs holds no more slots than
/// that, however many topics post to it.
const MAX_IN_FLIGHT: usize = 256;
const MAX_IN_FLIGHT_PER_ENDPOINT: usize = 8;
/// How long a POST may take, connecting included, and more for its body at
/// [`MIN_SEND_RATE`] bytes a second: a slot that an endpoint that does not
/// answer holds is free again after it (and the record waits its backoff, so
/// others take the slot), whatever fills the slots. A receiver that answers
/// later fails the try.
const POST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const MIN_SEND_RATE: u64 = 32 * 1024;
/// How long a pump waits for a POST in flight to finish.
const WAIT_FOR_ONE: std::time::Duration = std::time::Duration::from_millis(200);

/// The POSTs in flight, and the webhook partitions they are of: a
/// partition has one at a time, so its records go out in order.
pub(crate) struct WebhookPosts {
    posts: tokio::task::JoinSet<Result<(), String>>,
    of: HashMap<tokio::task::Id, Delivery>,
    busy: HashSet<(u32, u32)>,
    /// POSTs in flight to each endpoint.
    to: HashMap<String, usize>,
    timeout: std::time::Duration,
}

impl Default for WebhookPosts {
    fn default() -> Self {
        WebhookPosts::with_timeout(POST_TIMEOUT)
    }
}

impl WebhookPosts {
    /// With POSTs given `timeout` each (a test's shorter one).
    pub(crate) fn with_timeout(timeout: std::time::Duration) -> Self {
        WebhookPosts {
            posts: tokio::task::JoinSet::new(),
            of: HashMap::new(),
            busy: HashSet::new(),
            to: HashMap::new(),
            timeout,
        }
    }
}

/// The endpoint a URL posts to: its host (lowercased, without a final dot)
/// and port (the scheme's when none is given), whatever user part or
/// spelling the URL has.
fn endpoint(url: &str) -> String {
    let Ok(uri) = truthdb_s3client::post_target(url) else {
        return String::new();
    };
    let host = uri
        .host()
        .unwrap_or_default()
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("https") {
            443
        } else {
            80
        });
    format!("{host}:{port}")
}

/// The signature header's value for `body` posted as `delivery` at `secs`.
pub(crate) fn signature(secret: &str, secs: u64, delivery: &str, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let mut context = ring::hmac::Context::with_key(&key);
    context.update(format!("{secs}.{delivery}.").as_bytes());
    context.update(body);
    let digest: String = context
        .sign()
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("t={secs},v1={digest}")
}

impl Engine {
    /// One turn of webhook delivery: records the outcome of each POST that
    /// has finished (waiting a little for one), and starts those due on
    /// partitions with none in flight, from a partition further on each
    /// `round`. A slow endpoint holds up only its own partitions. Returns
    /// whether a POST is in flight or one was delivered.
    pub(crate) fn webhook_pump(
        &self,
        runtime: &tokio::runtime::Runtime,
        poster: &Arc<Poster>,
        posts: &mut WebhookPosts,
        round: usize,
    ) -> Result<bool, EngineError> {
        if self.storage.is_standby() {
            return Ok(false);
        }
        let runs = {
            let core = self.storage.core_arc();
            let mut guard = core.lock();
            StreamFile::try_new(&mut guard).is_some()
        };
        if !runs {
            return Ok(false);
        }
        let room = MAX_IN_FLIGHT - posts.busy.len();
        if room > 0 {
            let now_ms = super::now_ms();
            // Chosen before their records are read: not a partition with a
            // POST in flight, nor an endpoint at its share.
            let mut to = posts.to.clone();
            let busy = &posts.busy;
            let mut take = |webhook: &truthdb_streams::Webhook, partition: u32| {
                let count = to.entry(endpoint(&webhook.url)).or_default();
                let taken = !busy.contains(&(webhook.topic_id, partition))
                    && *count < MAX_IN_FLIGHT_PER_ENDPOINT;
                if taken {
                    *count += 1;
                }
                taken
            };
            let starting = self.streams(|file| file.webhook_due(now_ms, room, round, &mut take))?;
            for delivery in starting {
                let (poster, sent) = (Arc::clone(poster), delivery.clone());
                // On the thread's runtime (this is outside it).
                let timeout = posts.timeout
                    + std::time::Duration::from_secs(delivery.value.len() as u64 / MIN_SEND_RATE);
                let task = posts.posts.spawn_on(
                    async move {
                        tokio::time::timeout(timeout, post(&poster, &sent))
                            .await
                            .unwrap_or_else(|_| Err("no answer in time".to_string()))
                    },
                    runtime.handle(),
                );
                posts.busy.insert((delivery.topic_id, delivery.partition));
                *posts.to.entry(endpoint(&delivery.url)).or_default() += 1;
                posts.of.insert(task.id(), delivery);
            }
        }
        let mut finished = Vec::new();
        if !posts.posts.is_empty() {
            runtime.block_on(async {
                if let Ok(Some(done)) =
                    tokio::time::timeout(WAIT_FOR_ONE, posts.posts.join_next_with_id()).await
                {
                    finished.push(done);
                }
                while let Some(done) = posts.posts.try_join_next_with_id() {
                    finished.push(done);
                }
            });
        }
        let mut delivered = false;
        for done in finished {
            // A task that panicked is a failed try of its record.
            let (id, outcome) = match done {
                Ok((id, outcome)) => (id, outcome),
                Err(err) => (err.id(), Err(format!("the delivery failed: {err}"))),
            };
            let Some(delivery) = posts.of.remove(&id) else {
                continue;
            };
            posts.busy.remove(&(delivery.topic_id, delivery.partition));
            let key = endpoint(&delivery.url);
            if let Some(count) = posts.to.get_mut(&key) {
                *count -= 1;
                if *count == 0 {
                    posts.to.remove(&key);
                }
            }
            let recorded = self.streams(|file| {
                file.transaction(|tx| match &outcome {
                    Ok(()) => tx.webhook_delivered(&delivery),
                    Err(error) => tx
                        .webhook_failed(&delivery, error, super::now_ms())
                        .map(|_| ()),
                })
            });
            // One outcome that cannot be recorded does not hold back the
            // others'.
            if let Err(err) = recorded {
                eprintln!("webhook outcome of {}: {err}", delivery.topic);
            }
            delivered |= outcome.is_ok();
        }
        Ok(delivered || !posts.posts.is_empty())
    }
}

/// Posts `delivery`; why it failed, if it did.
async fn post(poster: &Poster, delivery: &Delivery) -> Result<(), String> {
    let secs = super::now_ms() / 1000;
    let id = format!(
        "{}/{}/{}",
        delivery.topic, delivery.partition, delivery.offset
    );
    let headers = [
        ("content-type", "application/json".to_string()),
        ("user-agent", "truthdb-webhooks".to_string()),
        (
            "x-truthdb-signature",
            signature(&delivery.secret, secs, &id, &delivery.value),
        ),
        ("x-truthdb-delivery", id),
    ];
    match poster
        .post(&delivery.url, &headers, Bytes::from(delivery.value.clone()))
        .await
    {
        Ok(status) if (200..300).contains(&status) => Ok(()),
        Ok(status) => Err(format!("HTTP {status}")),
        Err(err) => Err(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_signature_is_the_hmac_of_the_time_and_the_body() {
        // HMAC-SHA256("key", "1.t/0/2.{}"), computed independently.
        let expected = {
            let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"key");
            let tag = ring::hmac::sign(&key, b"1.t/0/2.{}");
            tag.as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(
            signature("key", 1, "t/0/2", b"{}"),
            format!("t=1,v1={expected}")
        );
        assert_ne!(
            signature("key", 1, "t/0/3", b"{}"),
            signature("key", 1, "t/0/2", b"{}")
        );
    }

    #[test]
    fn an_endpoint_is_its_host_and_port_however_written() {
        for url in [
            "http://Hooks.Example/x",
            "http://hooks.example:80/y",
            "http://user@hooks.example./z",
        ] {
            assert_eq!(endpoint(url), "hooks.example:80", "{url}");
        }
        assert_eq!(endpoint("https://hooks.example/x"), "hooks.example:443");
        assert_eq!(
            endpoint("http://hooks.example:8080/x"),
            "hooks.example:8080"
        );
    }
}
