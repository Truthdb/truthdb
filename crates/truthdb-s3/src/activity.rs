//! The API activity log: S3's management events (its bucket-level and
//! account-level operations, as CloudTrail names them: `CreateBucket`,
//! `PutBucketLifecycle`, `GetBucketPolicy`, `ListBuckets`, ...), recorded as
//! CloudTrail records and kept 90 days by the object engine (see
//! `truthdb_object::ActivityEvent`), in the account that made the call (an
//! anonymous call: the bucket owner's). Object-level operations are data
//! events, not recorded here.
//!
//! Events are kept in memory and written every `activity_flush`; as in
//! CloudTrail, delivery is best effort: events not yet written when the
//! server stops abruptly are lost.

use std::sync::{Arc, Mutex};

use hyper::Method;
use hyper::body::Incoming;
use serde_json::{Value, json};
use truthdb_object::MAX_ACTIVITY_RECORD;

use crate::api::{Target, has_param, subresource};
use crate::server::{Connection, Principal, S3State, Signed};

/// Events kept at most while they cannot be written: more are dropped.
const MAX_PENDING: usize = 10_000;
/// Events written per transaction.
const WRITE_BATCH: usize = 256;
/// The longest user agent and error message recorded (CloudTrail's limit).
const MAX_FIELD: usize = 1024;

/// An authenticated caller, and how it signed.
type Caller = (Principal, Option<Signed>);

/// Who sent a request, once it is authenticated (in the request's
/// extensions).
#[derive(Clone, Default)]
pub(crate) struct Who(Arc<Mutex<Option<Caller>>>);

impl Who {
    pub(crate) fn set(&self, principal: Option<Principal>, signed: Option<Signed>) {
        if let Some(principal) = principal {
            *self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((principal, signed));
        }
    }

    fn get(&self) -> Option<Caller> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// What a management request's event says that the request itself tells.
pub(crate) struct Call {
    name: &'static str,
    read_only: bool,
    bucket: Option<String>,
    sub: Option<&'static str>,
    host: Option<String>,
    user_agent: Option<String>,
    source_ip: Option<String>,
    tls: bool,
}

impl Call {
    /// The management event `request` makes, if it is one.
    pub(crate) fn of(request: &hyper::Request<Incoming>) -> Option<Call> {
        if request
            .extensions()
            .get::<crate::website::WebsiteHost>()
            .is_some()
            || crate::server::request_service(request).is_some_and(|service| service != "s3")
        {
            return None;
        }
        let method = request.method();
        let (name, bucket) = if crate::api::control_route(request)
            == Some(crate::api::ControlRoute::PublicAccessBlock)
        {
            let name = match *method {
                Method::GET => "GetAccountPublicAccessBlock",
                Method::PUT => "PutAccountPublicAccessBlock",
                Method::DELETE => "DeleteAccountPublicAccessBlock",
                _ => return None,
            };
            (name, None)
        } else if crate::api::control_route(request).is_some() {
            return None;
        } else {
            match crate::api::target(request).ok()? {
                Target::Service if *method == Method::GET => ("ListBuckets", None),
                Target::Bucket(bucket) => (bucket_event(request, method)?, Some(bucket)),
                _ => return None,
            }
        };
        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        };
        let connection = request.extensions().get::<Connection>();
        Some(Call {
            name,
            read_only: name.starts_with("Get") || name.starts_with("List"),
            bucket,
            sub: subresource(request),
            host: header("host"),
            user_agent: header("user-agent"),
            source_ip: connection.map(|connection| connection.peer.ip().to_string()),
            tls: connection.is_some_and(|connection| connection.tls),
        })
    }
}

/// The event name of a request on a bucket, if it is a management event.
fn bucket_event(request: &hyper::Request<Incoming>, method: &Method) -> Option<&'static str> {
    let by_method =
        |get: &'static str, put: &'static str, delete: Option<&'static str>| match *method {
            Method::GET => Some(get),
            Method::PUT => Some(put),
            Method::DELETE => delete,
            _ => None,
        };
    match subresource(request) {
        None => match *method {
            Method::PUT => Some("CreateBucket"),
            Method::DELETE => Some("DeleteBucket"),
            // Listing and HEAD are data events.
            _ => None,
        },
        Some("acl") => by_method("GetBucketAcl", "PutBucketAcl", None),
        Some("cors") => by_method("GetBucketCors", "PutBucketCors", Some("DeleteBucketCors")),
        Some("encryption") => by_method(
            "GetBucketEncryption",
            "PutBucketEncryption",
            Some("DeleteBucketEncryption"),
        ),
        Some("lifecycle") => by_method(
            "GetBucketLifecycle",
            "PutBucketLifecycle",
            Some("DeleteBucketLifecycle"),
        ),
        Some("location") if *method == Method::GET => Some("GetBucketLocation"),
        Some("logging") => by_method("GetBucketLogging", "PutBucketLogging", None),
        Some("metrics") if *method == Method::GET && !has_param(request, "id") => {
            Some("ListBucketMetricsConfigurations")
        }
        Some("metrics") => by_method(
            "GetBucketMetricsConfiguration",
            "PutBucketMetricsConfiguration",
            Some("DeleteBucketMetricsConfiguration"),
        ),
        Some("notification") => by_method("GetBucketNotification", "PutBucketNotification", None),
        Some("object-lock") => by_method(
            "GetBucketObjectLockConfiguration",
            "PutBucketObjectLockConfiguration",
            None,
        ),
        Some("ownershipControls") => by_method(
            "GetBucketOwnershipControls",
            "PutBucketOwnershipControls",
            Some("DeleteBucketOwnershipControls"),
        ),
        Some("policy") => by_method(
            "GetBucketPolicy",
            "PutBucketPolicy",
            Some("DeleteBucketPolicy"),
        ),
        Some("policyStatus") if *method == Method::GET => Some("GetBucketPolicyStatus"),
        Some("publicAccessBlock") => by_method(
            "GetBucketPublicAccessBlock",
            "PutBucketPublicAccessBlock",
            Some("DeleteBucketPublicAccessBlock"),
        ),
        Some("tagging") => by_method(
            "GetBucketTagging",
            "PutBucketTagging",
            Some("DeleteBucketTagging"),
        ),
        Some("versioning") => by_method("GetBucketVersioning", "PutBucketVersioning", None),
        Some("website") => by_method(
            "GetBucketWebsite",
            "PutBucketWebsite",
            Some("DeleteBucketWebsite"),
        ),
        _ => None,
    }
}

/// An event ready to keep: its account, time, id and CloudTrail record.
struct Event {
    account: String,
    time_ms: u64,
    id: [u8; 8],
    record: Vec<u8>,
}

/// `text` cut to at most `MAX_FIELD` bytes (at a character's end).
fn truncated(text: &str) -> &str {
    let mut end = text.len().min(MAX_FIELD);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// The CloudTrail record of `call`, answered with `status` and (an error)
/// `error`, by `who` (none: anonymous) for `account`.
#[allow(clippy::too_many_arguments)]
fn record(
    state: &S3State,
    call: &Call,
    who: Option<&Caller>,
    account: &str,
    owner: Option<&str>,
    time_ms: u64,
    request_id: &str,
    event_id: &str,
    error: Option<(&str, &str)>,
    with_parameters: bool,
) -> Value {
    let identity = match who {
        Some((principal, _)) => match &principal.user {
            None => json!({
                "type": "Root",
                "principalId": principal.account,
                "arn": format!("arn:aws:iam::{}:root", principal.account),
                "accountId": principal.account,
                "accessKeyId": principal.access_key,
            }),
            Some(user) => json!({
                "type": "IAMUser",
                "arn": format!("arn:aws:iam::{}:user/{user}", principal.account),
                "accountId": principal.account,
                "accessKeyId": principal.access_key,
                "userName": user,
            }),
        },
        None => json!({ "type": "AWSAccount", "principalId": "", "accountId": "anonymous" }),
    };
    let iso = crate::time::iso8601(time_ms);
    let mut parameters = json!({});
    if let Some(bucket) = &call.bucket {
        parameters["bucketName"] = json!(bucket);
    }
    if let Some(host) = &call.host {
        parameters["Host"] = json!(host);
    }
    if let Some(sub) = call.sub {
        parameters[sub] = json!("");
    }
    let mut record = json!({
        "eventVersion": "1.11",
        "userIdentity": identity,
        "eventTime": format!("{}Z", &iso[..19]),
        "eventSource": "s3.amazonaws.com",
        "eventName": call.name,
        "awsRegion": state.region,
        "sourceIPAddress": call.source_ip.as_deref().unwrap_or("-"),
        "userAgent": truncated(call.user_agent.as_deref().unwrap_or("")),
        "requestParameters": if with_parameters { parameters } else { Value::Null },
        "responseElements": Value::Null,
        "additionalEventData": {
            "SignatureVersion": if who.is_some() { "SigV4" } else { "" },
            "AuthenticationMethod": match who.and_then(|(_, signed)| signed.as_ref()) {
                Some(signed) if signed.auth_type == "REST-QUERY-STRING" => "QueryString",
                Some(_) => "AuthHeader",
                None => "",
            },
        },
        "requestID": request_id,
        "eventID": event_id,
        "readOnly": call.read_only,
        "eventType": "AwsApiCall",
        "managementEvent": true,
        "recipientAccountId": account,
        "eventCategory": "Management",
    });
    if let Some(bucket) = &call.bucket {
        record["resources"] = json!([{
            "accountId": owner.unwrap_or(account),
            "type": "AWS::S3::Bucket",
            "ARN": format!("arn:aws:s3:::{bucket}"),
        }]);
    }
    if let Some((code, message)) = error {
        record["errorCode"] = json!(code);
        record["errorMessage"] = json!(truncated(message));
    }
    if call.tls {
        record["tlsDetails"] = json!({
            "clientProvidedHostHeader": call.host.as_deref().unwrap_or(""),
        });
    }
    record
}

/// A random event id: 16 bytes as a UUID (version 4), and its first 8 as
/// the key's.
fn event_id() -> (String, [u8; 8]) {
    use ring::rand::SecureRandom;
    let mut bytes = [0u8; 16];
    let _ = ring::rand::SystemRandom::new().fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    (
        format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        ),
        bytes[..8].try_into().expect("8 bytes"),
    )
}

#[derive(Default)]
pub struct ActivityRecorder {
    pending: Mutex<Vec<Event>>,
}

impl ActivityRecorder {
    /// Keeps the event of `call`, answered with `error` (if it failed), in
    /// the account that made it (an anonymous call: `owner`'s, the bucket
    /// owner's; none known: not kept).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record(
        &self,
        state: &S3State,
        call: &Call,
        who: &Who,
        owner: Option<&str>,
        request_id: &str,
        error: Option<(&str, &str)>,
    ) {
        let who = who.get();
        let Some(account) = who
            .as_ref()
            .map(|(principal, _)| principal.account.clone())
            .or_else(|| owner.map(str::to_string))
        else {
            return;
        };
        let time_ms = crate::objects::now_ms();
        let (event_id, id) = event_id();
        // Too large with its parameters (a long host): kept without them.
        let mut bytes = None;
        for with_parameters in [true, false] {
            let built = record(
                state,
                call,
                who.as_ref(),
                &account,
                owner,
                time_ms,
                request_id,
                &event_id,
                error,
                with_parameters,
            )
            .to_string()
            .into_bytes();
            if built.len() <= MAX_ACTIVITY_RECORD {
                bytes = Some(built);
                break;
            }
        }
        let Some(record) = bytes else {
            return;
        };
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.len() < MAX_PENDING {
            pending.push(Event {
                account,
                time_ms,
                id,
                record,
            });
        }
    }

    fn take(&self) -> Vec<Event> {
        std::mem::take(
            &mut *self
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    /// Writes the events kept (put back, up to `MAX_PENDING`, if they could
    /// not be).
    pub(crate) async fn flush(&self, state: &S3State) {
        let mut taken = self.take();
        while !taken.is_empty() {
            let rest = taken.split_off(taken.len().min(WRITE_BATCH));
            let batch = Arc::new(taken);
            let written = Arc::clone(&batch);
            let stored = state
                .engine
                .run_object(move |file| {
                    file.transaction(|tx| {
                        for event in written.iter() {
                            tx.add_activity(
                                &event.account,
                                event.time_ms,
                                &event.id,
                                &event.record,
                            )?;
                        }
                        Ok(())
                    })
                })
                .await;
            if stored.is_err() {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let back: Vec<Event> = Arc::try_unwrap(batch)
                    .unwrap_or_default()
                    .into_iter()
                    .chain(rest)
                    .collect();
                let room = MAX_PENDING.saturating_sub(pending.len());
                pending.splice(0..0, back.into_iter().take(room));
                return;
            }
            taken = rest;
        }
    }
}

/// Writes the activity every `state.activity_flush` until `shutdown`, and
/// once more then. A standby writes (and keeps) nothing.
pub async fn run(state: Arc<S3State>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    loop {
        let stopping = tokio::select! {
            _ = tokio::time::sleep(state.activity_flush) => false,
            changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        };
        if state.storage.is_standby() {
            state.activity.take();
        } else {
            state.activity.flush(&state).await;
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
    fn event_ids_are_version_4_uuids() {
        let (id, key) = event_id();
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "4");
        assert!(matches!(&id[19..20], "8" | "9" | "a" | "b"), "{id}");
        assert_eq!(
            key.iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            id.replace('-', "")[..16]
        );
        assert_eq!(truncated(&"é".repeat(600)).len(), 1024);
        assert_eq!(truncated("short"), "short");
    }
}
