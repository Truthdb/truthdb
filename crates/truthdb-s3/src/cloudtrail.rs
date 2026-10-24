//! A CloudTrail API over the API activity log (see `activity`): LookupEvents,
//! in the AWS JSON 1.1 protocol the SDKs speak to it (`X-Amz-Target:
//! com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.<operation>`,
//! signed for `cloudtrail`). A caller reads its own account's management
//! events of the last 90 days, newest first, as allowed by
//! `cloudtrail:LookupEvents`.
//!
//! As CloudTrail: at most one lookup attribute (AccessKeyId, EventId,
//! EventName, EventSource, ReadOnly, ResourceName, ResourceType or
//! Username), at most 50 events a page, and a NextToken that holds for the
//! same parameters only. Insights events are not kept: asking for them
//! finds none. A page reads a bounded number of events; one that ends
//! early carries a NextToken even if it holds fewer than asked for.

use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::{Map, Value, json};
use truthdb_object::ActivityEvent;

use crate::error::S3Error;
use crate::server::{Body, Connection, S3State, full};

const TARGET_PREFIX: &str = "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.";
const MAX_BODY: usize = 64 * 1024;
const MAX_RESULTS: u64 = 50;
/// Events read per storage-lock hold, and per call at most.
const SLICE: usize = 256;
const MAX_SCANNED: usize = 10_000;
const ATTRIBUTES: [&str; 8] = [
    "AccessKeyId",
    "EventId",
    "EventName",
    "EventSource",
    "ReadOnly",
    "ResourceName",
    "ResourceType",
    "Username",
];

struct CtError {
    status: u16,
    code: &'static str,
    message: String,
}

impl CtError {
    fn new(status: u16, code: &'static str, message: impl Into<String>) -> CtError {
        CtError {
            status,
            code,
            message: message.into(),
        }
    }

    fn token() -> CtError {
        CtError::new(
            400,
            "InvalidNextTokenException",
            "The next token is not valid for these parameters.",
        )
    }
}

impl From<S3Error> for CtError {
    fn from(err: S3Error) -> CtError {
        CtError::new(err.status, err.code, err.message)
    }
}

impl From<truthdb_engine::engine::EngineError> for CtError {
    fn from(err: truthdb_engine::engine::EngineError) -> CtError {
        CtError::new(500, "InternalServiceError", err.to_string())
    }
}

/// Answers a CloudTrail request: its result, or its error as the JSON
/// protocol carries it (`__type`, `message`).
pub async fn handle(state: &S3State, request: Request<Incoming>) -> Response<Body> {
    let (status, body) = match serve(state, request).await {
        Ok(value) => (200, value),
        Err(err) => (
            err.status,
            json!({ "__type": err.code, "message": err.message }),
        ),
    };
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "application/x-amz-json-1.1")
        .body(full(body.to_string()))
        .expect("a valid response")
}

async fn serve(state: &S3State, request: Request<Incoming>) -> Result<Value, CtError> {
    let crate::signed_body::SignedBody {
        parts,
        body,
        principal,
    } = crate::signed_body::read(state, request, "cloudtrail", MAX_BODY).await?;
    let operation = parts
        .headers
        .get("x-amz-target")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix(TARGET_PREFIX))
        .ok_or_else(|| CtError::new(400, "UnknownOperationException", "No operation is named."))?
        .to_string();
    let input: Map<String, Value> = if body.is_empty() {
        Map::new()
    } else {
        match serde_json::from_slice(&body) {
            Ok(Value::Object(map)) => map,
            _ => {
                return Err(CtError::new(
                    400,
                    "SerializationException",
                    "The body is not a JSON object.",
                ));
            }
        }
    };
    let authz = crate::authz::Authz::load(
        state,
        Some(&principal),
        Some(crate::server::Signed {
            auth_type: "REST-HEADER",
            at_ms: parts
                .headers
                .get("x-amz-date")
                .and_then(|value| value.to_str().ok())
                .and_then(crate::server::parse_amz_date)
                .map(|secs| secs * 1000),
        }),
        &parts.headers,
        "",
        parts.extensions.get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    if operation != "LookupEvents" {
        return Err(CtError::new(
            400,
            "UnsupportedOperationException",
            format!("{operation} is not supported."),
        ));
    }
    authz.check_arn("cloudtrail:LookupEvents", "*", &principal.account)?;
    lookup_events(state, &input, &principal.account).await
}

/// A lookup attribute: the event field it matches.
struct Lookup {
    key: String,
    value: String,
}

impl Lookup {
    fn matches(&self, event: &Value) -> bool {
        let found = match self.key.as_str() {
            "AccessKeyId" => text(&event["userIdentity"]["accessKeyId"]),
            "EventId" => text(&event["eventID"]),
            "EventName" => text(&event["eventName"]),
            "EventSource" => text(&event["eventSource"]),
            "ReadOnly" => event["readOnly"].as_bool().map(|read| read.to_string()),
            "Username" => username(event),
            "ResourceName" => {
                return resources(event).iter().any(|(name, _)| *name == self.value);
            }
            _ => {
                return resources(event).iter().any(|(_, kind)| *kind == self.value);
            }
        };
        found.as_deref() == Some(self.value.as_str())
    }
}

fn text(value: &Value) -> Option<String> {
    value.as_str().map(str::to_string)
}

/// The user an event names: an IAM user's name, `root` for an account's
/// root.
fn username(event: &Value) -> Option<String> {
    let identity = &event["userIdentity"];
    match identity["type"].as_str() {
        Some("Root") => Some("root".to_string()),
        _ => text(&identity["userName"]),
    }
}

/// An event's resources: each one's name (a bucket's) and type.
fn resources(event: &Value) -> Vec<(String, String)> {
    event["resources"]
        .as_array()
        .map(|resources| {
            resources
                .iter()
                .filter_map(|resource| {
                    let arn = resource["ARN"].as_str()?;
                    Some((
                        arn.strip_prefix("arn:aws:s3:::").unwrap_or(arn).to_string(),
                        resource["type"].as_str().unwrap_or("").to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A timestamp parameter (epoch seconds, fractions kept as milliseconds).
fn time(input: &Map<String, Value>, name: &str) -> Result<Option<u64>, CtError> {
    match input.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => match number.as_f64() {
            Some(secs) if (0.0..=4e9).contains(&secs) => Ok(Some((secs * 1000.0) as u64)),
            _ => Err(CtError::new(
                400,
                "InvalidTimeRangeException",
                format!("The {name} is out of range."),
            )),
        },
        Some(_) => Err(CtError::new(
            400,
            "SerializationException",
            format!("The {name} must be a timestamp."),
        )),
    }
}

/// The parameters a NextToken holds for: all but the token itself.
fn parameters_digest(input: &Map<String, Value>) -> String {
    let mut kept = input.clone();
    kept.remove("NextToken");
    let digest = ring::digest::digest(
        &ring::digest::SHA256,
        Value::Object(kept).to_string().as_bytes(),
    );
    digest.as_ref()[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn lookup_events(
    state: &S3State,
    input: &Map<String, Value>,
    account: &str,
) -> Result<Value, CtError> {
    let lookup = match input.get("LookupAttributes") {
        None | Some(Value::Null) => None,
        Some(Value::Array(attributes)) if attributes.is_empty() => None,
        Some(Value::Array(attributes)) if attributes.len() == 1 => {
            let attribute = &attributes[0];
            let (Some(key), Some(value)) = (
                attribute["AttributeKey"].as_str(),
                attribute["AttributeValue"].as_str(),
            ) else {
                return Err(CtError::new(
                    400,
                    "InvalidLookupAttributesException",
                    "A lookup attribute has an AttributeKey and an AttributeValue.",
                ));
            };
            if !ATTRIBUTES.contains(&key) {
                return Err(CtError::new(
                    400,
                    "InvalidLookupAttributesException",
                    format!("{key} is not a lookup attribute."),
                ));
            }
            Some(Lookup {
                key: key.to_string(),
                value: value.to_string(),
            })
        }
        Some(_) => {
            return Err(CtError::new(
                400,
                "InvalidLookupAttributesException",
                "At most one lookup attribute is given.",
            ));
        }
    };
    let (start, end) = (time(input, "StartTime")?, time(input, "EndTime")?);
    if let (Some(start), Some(end)) = (start, end)
        && start > end
    {
        return Err(CtError::new(
            400,
            "InvalidTimeRangeException",
            "The StartTime is after the EndTime.",
        ));
    }
    let max = match input.get("MaxResults") {
        None | Some(Value::Null) => MAX_RESULTS,
        Some(Value::Number(number)) => match number.as_u64() {
            Some(max) if (1..=MAX_RESULTS).contains(&max) => max,
            _ => {
                return Err(CtError::new(
                    400,
                    "InvalidMaxResultsException",
                    "MaxResults is 1 to 50.",
                ));
            }
        },
        Some(_) => {
            return Err(CtError::new(
                400,
                "InvalidMaxResultsException",
                "MaxResults is a number.",
            ));
        }
    };
    let insights = match input.get("EventCategory") {
        None | Some(Value::Null) => false,
        Some(Value::String(category)) if category == "insight" => true,
        _ => {
            return Err(CtError::new(
                400,
                "InvalidEventCategoryException",
                "The EventCategory is insight.",
            ));
        }
    };
    // A token: where the last page stopped (an event key of this
    // account's), and the parameters it holds for.
    let digest = parameters_digest(input);
    let mut after: Option<Vec<u8>> = match input.get("NextToken") {
        None | Some(Value::Null) => None,
        Some(Value::String(token)) => {
            use base64::Engine as _;
            let (key, held) = token.split_once('.').ok_or_else(CtError::token)?;
            let key = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(key)
                .map_err(|_| CtError::token())?;
            if held != digest || !key.starts_with(&[account.as_bytes(), &[0]].concat()) {
                return Err(CtError::token());
            }
            Some(key)
        }
        Some(_) => return Err(CtError::token()),
    };
    let mut events = Vec::new();
    let mut scanned = 0;
    let mut more = false;
    // Insights events are not kept.
    if !insights {
        loop {
            let owner = account.to_string();
            let from = after.clone();
            let page: Vec<ActivityEvent> = state
                .engine
                .run_object(move |file| file.activity(Some(&owner), from.as_deref(), SLICE))
                .await?;
            let exhausted = page.len() < SLICE;
            let mut stopped = false;
            for event in page {
                scanned += 1;
                after = Some(event.resume_key());
                // Newest first: past the start, nothing more matches.
                if start.is_some_and(|start| event.time_ms < start) {
                    stopped = true;
                    break;
                }
                if end.is_some_and(|end| event.time_ms > end) {
                    continue;
                }
                let Ok(record) = serde_json::from_slice::<Value>(&event.record) else {
                    continue;
                };
                if lookup.as_ref().is_none_or(|lookup| lookup.matches(&record)) {
                    events.push(found(&event, &record));
                }
                if events.len() as u64 == max || scanned >= MAX_SCANNED {
                    more = true;
                    stopped = true;
                    break;
                }
            }
            if stopped || exhausted {
                break;
            }
        }
    }
    let mut answer = json!({ "Events": events });
    if more && let Some(after) = after {
        use base64::Engine as _;
        answer["NextToken"] = json!(format!(
            "{}.{digest}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(after)
        ));
    }
    Ok(answer)
}

/// An event as LookupEvents returns it.
fn found(event: &ActivityEvent, record: &Value) -> Value {
    json!({
        "EventId": record["eventID"],
        "EventName": record["eventName"],
        "ReadOnly": record["readOnly"].as_bool().map_or("false".to_string(), |read| read.to_string()),
        "AccessKeyId": record["userIdentity"]["accessKeyId"],
        "EventTime": event.time_ms as f64 / 1000.0,
        "EventSource": record["eventSource"],
        "Username": username(record),
        "Resources": resources(record)
            .into_iter()
            .map(|(name, kind)| json!({ "ResourceType": kind, "ResourceName": name }))
            .collect::<Vec<_>>(),
        "CloudTrailEvent": String::from_utf8_lossy(&event.record),
    })
}
