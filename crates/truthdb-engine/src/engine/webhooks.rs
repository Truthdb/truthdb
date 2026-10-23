//! Webhook delivery (see `truthdb_streams::Webhook`), on a thread of its
//! own (see `session::pool`): each step posts the records due, one per
//! webhook partition, and records each outcome. A POST carries the record
//! as its JSON body (an S3 event message: `{"Records":[...]}`), signed:
//! `x-truthdb-signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of
//! "<t>.<body>" under the webhook's secret>`, and names the record in
//! `x-truthdb-delivery: <topic>/<partition>/<offset>` (the same on a
//! retry, so a receiver can drop a repeat). A 2xx answer delivers it;
//! anything else, or no answer, is a failed try.

use truthdb_s3client::{Bytes, Poster};
use truthdb_streams::{Delivery, StreamFile};

use super::{Engine, EngineError};

/// Records one step posts at most.
const STEP_DELIVERIES: usize = 16;

/// The signature header's value for `body` at `secs`.
pub(crate) fn signature(secret: &str, secs: u64, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let mut context = ring::hmac::Context::with_key(&key);
    context.update(format!("{secs}.").as_bytes());
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
    /// One step of webhook delivery; returns whether one was delivered
    /// (more may be due at once).
    pub(crate) fn webhook_step(
        &self,
        runtime: &tokio::runtime::Runtime,
        poster: &Poster,
        stopping: &dyn Fn() -> bool,
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
        let now_ms = super::now_ms();
        let due = self.streams(|file| file.webhook_due(now_ms, STEP_DELIVERIES))?;
        let mut delivered = false;
        for delivery in &due {
            if stopping() {
                break;
            }
            let outcome = runtime.block_on(post(poster, delivery, now_ms / 1000));
            self.streams(|file| {
                file.transaction(|tx| match &outcome {
                    Ok(()) => tx.webhook_delivered(delivery),
                    Err(error) => tx
                        .webhook_failed(delivery, error, super::now_ms())
                        .map(|_| ()),
                })
            })?;
            delivered |= outcome.is_ok();
        }
        Ok(delivered)
    }
}

/// Posts `delivery`; why it failed, if it did.
async fn post(poster: &Poster, delivery: &Delivery, secs: u64) -> Result<(), String> {
    let headers = [
        ("content-type", "application/json".to_string()),
        ("user-agent", "truthdb-webhooks".to_string()),
        (
            "x-truthdb-signature",
            signature(&delivery.secret, secs, &delivery.value),
        ),
        (
            "x-truthdb-delivery",
            format!(
                "{}/{}/{}",
                delivery.topic, delivery.partition, delivery.offset
            ),
        ),
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
        // HMAC-SHA256("key", "1.{}"), computed independently.
        let expected = {
            let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"key");
            let tag = ring::hmac::sign(&key, b"1.{}");
            tag.as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(signature("key", 1, b"{}"), format!("t=1,v1={expected}"));
        assert_ne!(signature("key", 2, b"{}"), signature("key", 1, b"{}"));
    }
}
