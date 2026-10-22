//! S3 Object Lock: a bucket's `?object-lock` configuration (enabled, and an
//! optional default retention for new objects), each version's `?retention`
//! and `?legal-hold`, and the `x-amz-object-lock-*` headers of writes and
//! reads. The object engine keeps the locks and refuses to delete what they
//! protect (see `truthdb_object::ObjectLock`); this module speaks S3.

use hyper::body::Incoming;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use quick_xml::Reader;
use quick_xml::events::Event;
use truthdb_iam::Context;
use truthdb_object::{
    BucketError, EventHold, OBJECT_LOCK_DEFAULT, ObjectLock, Retention, RetentionChange,
    RetentionMode, VersionSel,
};

use crate::authz::{Authz, Resource};
use crate::error::S3Error;
use crate::server::{Authenticated, Body, S3State, full};

pub const MODE_HEADER: &str = "x-amz-object-lock-mode";
pub const UNTIL_HEADER: &str = "x-amz-object-lock-retain-until-date";
pub const HOLD_HEADER: &str = "x-amz-object-lock-legal-hold";
pub const EVENT_HOLD_HEADER: &str = "x-amz-object-lock-event-hold";
pub const EVENT_DAYS_HEADER: &str = "x-amz-object-lock-event-hold-duration-days";
pub const EVENT_YEARS_HEADER: &str = "x-amz-object-lock-event-hold-duration-years";
pub const BYPASS_HEADER: &str = "x-amz-bypass-governance-retention";
/// CreateBucket's header that enables Object Lock on the new bucket.
pub const ENABLED_HEADER: &str = "x-amz-bucket-object-lock-enabled";

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const DAY_MS: u64 = 86_400_000;
/// The longest default retention or event hold: 100 years, or as many days.
const MAX_DAYS: u32 = 36_500;
const MAX_YEARS: u32 = 100;
/// Largest Object Lock document read.
const MAX_BODY: usize = 64 * 1024;

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

fn malformed() -> S3Error {
    S3Error::new(
        400,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published \
         schema",
    )
}

fn mode_name(mode: RetentionMode) -> &'static str {
    match mode {
        RetentionMode::Governance => "GOVERNANCE",
        RetentionMode::Compliance => "COMPLIANCE",
    }
}

fn parse_mode(text: &str) -> Result<RetentionMode, S3Error> {
    match text {
        "GOVERNANCE" => Ok(RetentionMode::Governance),
        "COMPLIANCE" => Ok(RetentionMode::Compliance),
        _ => Err(invalid(format!("Unknown Object Lock mode {text:?}"))),
    }
}

/// A retain-until date: an ISO 8601 date and time, in the future.
fn parse_until(text: &str, now_ms: u64) -> Result<u64, S3Error> {
    // A bare number would be taken for epoch seconds: S3 wants a date.
    let until = text
        .contains('T')
        .then(|| truthdb_iam::parse_date_ms(text))
        .flatten()
        .and_then(|ms| u64::try_from(ms).ok())
        .ok_or_else(|| {
            invalid(format!(
                "The retain until date {text:?} is not a valid date"
            ))
        })?;
    if until <= now_ms {
        return Err(invalid("The retain until date must be in the future"));
    }
    Ok(until)
}

fn parse_hold(text: &str) -> Result<bool, S3Error> {
    match text {
        "ON" => Ok(true),
        "OFF" => Ok(false),
        _ => Err(invalid("The legal hold status must be ON or OFF")),
    }
}

/// A duration in days or in years, not both (`None` when neither is given),
/// as `(years, count)`.
fn parse_duration(days: Option<&str>, years: Option<&str>) -> Result<Option<(bool, u32)>, S3Error> {
    let count = |text: &str, max: u32| {
        text.parse::<u32>()
            .ok()
            .filter(|count| (1..=max).contains(count))
            .ok_or_else(|| invalid(format!("A duration must be a whole number from 1 to {max}")))
    };
    match (days, years) {
        (None, None) => Ok(None),
        (Some(days), None) => Ok(Some((false, count(days, MAX_DAYS)?))),
        (None, Some(years)) => Ok(Some((true, count(years, MAX_YEARS)?))),
        (Some(_), Some(_)) => Err(invalid("A duration is in either days or years, not both")),
    }
}

/// The retention a request asks for, from its parts (headers, or a
/// Retention body).
#[derive(Debug, PartialEq, Eq)]
enum Asked {
    None,
    Retention(Retention),
    /// The event hold turned off: a release, the date (if any) its minimum.
    Release(RetentionMode, Option<u64>),
}

/// A retention from its parts: a mode with a retain-until date (fixed), or
/// with an event hold ON and its duration (variable; a date is then its
/// minimum). An event hold OFF without a date releases one.
fn asked_retention(
    mode: Option<&str>,
    until: Option<&str>,
    event_hold: Option<&str>,
    duration: Option<(bool, u32)>,
    now_ms: u64,
) -> Result<Asked, S3Error> {
    let event_hold = event_hold
        .map(|text| match text {
            "ON" => Ok(true),
            "OFF" => Ok(false),
            _ => Err(invalid("The event hold status must be ON or OFF")),
        })
        .transpose()?;
    let required_mode = |what: &str| {
        mode.ok_or_else(|| invalid(format!("{what} needs a retention mode")))
            .and_then(parse_mode)
    };
    Ok(match event_hold {
        Some(true) => {
            let (years, count) =
                duration.ok_or_else(|| invalid("An event hold that is ON needs its duration"))?;
            Asked::Retention(Retention {
                mode: required_mode("An event hold")?,
                until_ms: until
                    .map(|until| parse_until(until, now_ms))
                    .transpose()?
                    .unwrap_or(0),
                event: Some(EventHold {
                    on: true,
                    years,
                    count,
                }),
            })
        }
        Some(false) => {
            if duration.is_some() {
                return Err(invalid(
                    "An event hold that is OFF takes no duration: the version's own applies",
                ));
            }
            Asked::Release(
                required_mode("Releasing an event hold")?,
                until.map(|until| parse_until(until, now_ms)).transpose()?,
            )
        }
        None => {
            if duration.is_some() {
                return Err(invalid("An event hold duration needs an event hold"));
            }
            match (mode, until) {
                (None, None) => Asked::None,
                (Some(mode), Some(until)) => Asked::Retention(Retention {
                    mode: parse_mode(mode)?,
                    until_ms: parse_until(until, now_ms)?,
                    event: None,
                }),
                _ => {
                    return Err(invalid(
                        "A retention needs both a mode and a retain-until date, or an event hold",
                    ));
                }
            }
        }
    })
}

/// The lock a write asks for in its headers (or a POST form's fields,
/// which `field` finds by name): a retention (fixed, or variable with an
/// event hold), and a legal hold. `None` when it asks for none.
pub fn requested<'a>(
    field: impl Fn(&str) -> Option<&'a str>,
    now_ms: u64,
) -> Result<Option<ObjectLock>, S3Error> {
    let retention = match asked_retention(
        field(MODE_HEADER),
        field(UNTIL_HEADER),
        field(EVENT_HOLD_HEADER),
        parse_duration(field(EVENT_DAYS_HEADER), field(EVENT_YEARS_HEADER))?,
        now_ms,
    )? {
        Asked::None => None,
        Asked::Retention(retention) => Some(retention),
        // OFF with a date, on a new object: a fixed retention.
        Asked::Release(mode, Some(until_ms)) => Some(Retention {
            mode,
            until_ms,
            event: None,
        }),
        Asked::Release(_, None) => {
            return Err(invalid(
                "A new object has no event hold to release: an event hold that is OFF needs a \
                 retain-until date",
            ));
        }
    };
    let legal_hold = field(HOLD_HEADER).map(parse_hold).transpose()?;
    Ok(
        (retention.is_some() || legal_hold.is_some()).then_some(ObjectLock {
            retention,
            legal_hold,
        }),
    )
}

/// [`requested`] from a request's headers.
pub fn from_headers(headers: &HeaderMap, now_ms: u64) -> Result<Option<ObjectLock>, S3Error> {
    requested(
        |name| headers.get(name).and_then(|value| value.to_str().ok()),
        now_ms,
    )
}

/// Whether a request carries an integrity check of its body: Content-MD5
/// or an `x-amz-checksum-*` (a header, or a trailer it announces).
pub fn has_integrity_check(headers: &HeaderMap) -> bool {
    headers.contains_key("content-md5")
        || headers
            .keys()
            .any(|name| crate::server::CHECKSUM_HEADERS.contains(&name.as_str()))
        || headers.contains_key("x-amz-trailer")
}

/// A write that sets a retention must check its body's integrity (S3
/// requires Content-MD5 or a checksum).
pub fn check_integrity(headers: &HeaderMap, lock: &ObjectLock) -> Result<(), S3Error> {
    if lock.retention.is_some() && !has_integrity_check(headers) {
        return Err(S3Error::invalid_request(
            "Content-MD5 or an x-amz-checksum header is required for a write with an Object \
             Lock retention",
        ));
    }
    Ok(())
}

/// A bucket's default retention: a mode, with a period (a new object's
/// retain-until date is that long after it is made), an event hold's
/// duration (a new object starts held), or both. Durations are
/// `(years, count)`. Kept in the bucket's configuration as
/// `[mode u8: 1 GOVERNANCE, 2 COMPLIANCE][unit u8: 0 none, 1 days, 2 years]
/// [count u32]`, then for an event hold `[unit u8: 1 days, 2 years]
/// [count u32]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultRetention {
    pub mode: RetentionMode,
    pub period: Option<(bool, u32)>,
    pub event: Option<(bool, u32)>,
}

impl DefaultRetention {
    fn encode(&self) -> Vec<u8> {
        let unit = |years: bool| if years { 2 } else { 1 };
        let mut out = vec![match self.mode {
            RetentionMode::Governance => 1,
            RetentionMode::Compliance => 2,
        }];
        let (period_unit, period_count) = self.period.map_or((0, 0), |(y, c)| (unit(y), c));
        out.push(period_unit);
        out.extend_from_slice(&period_count.to_le_bytes());
        if let Some((years, count)) = self.event {
            out.push(unit(years));
            out.extend_from_slice(&count.to_le_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let duration = |unit: u8, count: &[u8]| -> Option<Option<(bool, u32)>> {
            let count = u32::from_le_bytes(count.try_into().ok()?);
            match unit {
                0 => Some(None),
                1 => Some(Some((false, count))),
                2 => Some(Some((true, count))),
                _ => None,
            }
        };
        let (mode, period, event) = match bytes {
            [mode, unit, count @ ..] if count.len() == 4 => (mode, duration(*unit, count)?, None),
            [mode, unit, rest @ ..] if rest.len() == 9 => (
                mode,
                duration(*unit, &rest[..4])?,
                Some(duration(rest[4], &rest[5..])?.filter(|_| rest[4] != 0)?),
            ),
            _ => return None,
        };
        if period.is_none() && event.is_none() {
            return None;
        }
        Some(DefaultRetention {
            mode: match mode {
                1 => RetentionMode::Governance,
                2 => RetentionMode::Compliance,
                _ => return None,
            },
            period,
            event,
        })
    }

    /// The retention a new object gets at `now_ms` (a year is 365 days).
    pub fn retention(&self, now_ms: u64) -> Retention {
        let event = self.event.map(|(years, count)| EventHold {
            on: true,
            years,
            count,
        });
        let until_ms = self.period.map_or(0, |(years, count)| {
            now_ms
                + EventHold {
                    on: false,
                    years,
                    count,
                }
                .duration_ms()
        });
        Retention {
            mode: self.mode,
            until_ms,
            event,
        }
    }
}

/// The lock a new object of a bucket gets: the one its write asks for, or
/// else the bucket's default retention. A bucket without Object Lock takes
/// none (asking for one is refused).
pub fn new_object_lock(
    requested: Option<ObjectLock>,
    bucket_lock: bool,
    default: Option<DefaultRetention>,
    now_ms: u64,
) -> Result<Option<ObjectLock>, S3Error> {
    if !bucket_lock {
        return match requested {
            Some(_) => Err(S3Error::invalid_request(
                "Bucket is missing Object Lock Configuration",
            )),
            None => Ok(None),
        };
    }
    let mut lock = requested.unwrap_or_default();
    if lock.retention.is_none() {
        lock.retention = default.map(|default| default.retention(now_ms));
    }
    Ok((lock != ObjectLock::default()).then_some(lock))
}

/// The bucket's Object Lock state, read in the same engine call as the
/// write it governs: whether it is enabled, and its default retention.
pub fn bucket_state(
    file: &mut truthdb_object::ObjFile<'_>,
    bucket: &str,
) -> Result<(bool, Option<DefaultRetention>), truthdb_core::storage::StorageError> {
    let enabled = file.bucket(bucket)?.is_some_and(|info| info.object_lock);
    let default = match enabled {
        true => file
            .bucket_config(bucket, OBJECT_LOCK_DEFAULT, "")?
            .and_then(|document| DefaultRetention::decode(&document)),
        false => None,
    };
    Ok((enabled, default))
}

/// Sets the Object Lock condition keys of a lock a request asks for.
pub fn set_condition_keys(context: &mut Context, lock: &ObjectLock, now_ms: u64) {
    if let Some(retention) = lock.retention {
        context.set(
            "s3:object-lock-mode",
            vec![mode_name(retention.mode).to_string()],
        );
        let until = retention.effective_until(now_ms);
        context.set(
            "s3:object-lock-retain-until-date",
            vec![crate::time::iso8601(until)],
        );
        // Whole days left, a part of a day counting as one.
        let days = until.saturating_sub(now_ms).div_ceil(DAY_MS);
        context.set(
            "s3:object-lock-remaining-retention-days",
            vec![days.to_string()],
        );
        if let Some(event) = retention.event {
            set_event_hold_keys(context, event);
        }
    }
    if let Some(on) = lock.legal_hold {
        context.set(
            "s3:object-lock-legal-hold",
            vec![if on { "ON" } else { "OFF" }.to_string()],
        );
    }
}

/// `s3:object-lock-event-hold`, and the duration in days (a year is 365)
/// while the hold is on.
fn set_event_hold_keys(context: &mut Context, event: EventHold) {
    context.set(
        "s3:object-lock-event-hold",
        vec![if event.on { "ON" } else { "OFF" }.to_string()],
    );
    if event.on {
        context.set(
            "s3:object-lock-event-hold-duration-days",
            vec![(event.duration_ms() / DAY_MS).to_string()],
        );
    }
}

/// Checks what setting a write's requested lock needs beyond the write
/// itself: `s3:PutObjectRetention` for a retention, `s3:PutObjectLegalHold`
/// for a legal hold, each with the lock's condition keys.
pub fn authorize(authz: &Authz, resource: Resource<'_>, lock: &ObjectLock) -> Result<(), S3Error> {
    let mut context = authz.context().clone();
    set_condition_keys(&mut context, lock, crate::objects::now_ms());
    if lock.retention.is_some() {
        authz.check_with("s3:PutObjectRetention", resource, &context)?;
    }
    if lock.legal_hold.is_some() {
        authz.check_with("s3:PutObjectLegalHold", resource, &context)?;
    }
    Ok(())
}

/// Whether a request bypasses GOVERNANCE retention: it says so in its
/// header, and may (`s3:BypassGovernanceRetention`).
pub fn bypasses(headers: &HeaderMap, authz: &Authz, resource: Resource<'_>) -> bool {
    headers
        .get(BYPASS_HEADER)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"true"))
        && authz
            .check("s3:BypassGovernanceRetention", resource)
            .is_ok()
}

/// Adds an object's lock headers to a GET or HEAD answer: its retention
/// when the caller may read retention, its legal hold when it may read
/// legal holds.
pub fn with_headers(
    mut builder: hyper::http::response::Builder,
    lock: &ObjectLock,
    retention_visible: bool,
    hold_visible: bool,
) -> hyper::http::response::Builder {
    if let Some(retention) = lock.retention
        && retention_visible
    {
        let until = retention.effective_until(crate::objects::now_ms());
        builder = builder
            .header(MODE_HEADER, mode_name(retention.mode))
            .header(UNTIL_HEADER, crate::time::iso8601(until));
        if let Some(event) = retention.event {
            builder = builder.header(EVENT_HOLD_HEADER, if event.on { "ON" } else { "OFF" });
            if event.on {
                let unit = if event.years {
                    EVENT_YEARS_HEADER
                } else {
                    EVENT_DAYS_HEADER
                };
                builder = builder.header(unit, event.count.to_string());
            }
        }
    }
    if let Some(on) = lock.legal_hold
        && hold_visible
    {
        builder = builder.header(HOLD_HEADER, if on { "ON" } else { "OFF" });
    }
    builder
}

/// The elements of a small XML document under root `root`, as (`Parent/
/// Child` path below the root, text; empty for an element holding others).
/// Elements outside `allowed`, text outside a leaf, and an element given
/// twice are malformed.
fn leaves(body: &[u8], root: &str, allowed: &[&str]) -> Result<Vec<(String, String)>, S3Error> {
    let mut reader = Reader::from_reader(body);
    let mut path: Vec<String> = Vec::new();
    // Whether each open element has held another.
    let mut parents: Vec<bool> = Vec::new();
    let mut seen_root = false;
    let mut text = String::new();
    let mut out: Vec<(String, String)> = Vec::new();
    let mut buf = Vec::new();
    loop {
        let event = reader.read_event_into(&mut buf).map_err(|_| malformed())?;
        let empty = matches!(event, Event::Empty(_));
        match event {
            Event::Start(e) | Event::Empty(e) => {
                if !text.trim().is_empty() {
                    return Err(malformed());
                }
                text.clear();
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if path.is_empty() {
                    if name != root || std::mem::replace(&mut seen_root, true) {
                        return Err(malformed());
                    }
                } else {
                    let below: Vec<&str> = path[1..]
                        .iter()
                        .map(String::as_str)
                        .chain([name.as_str()])
                        .collect();
                    if !allowed.contains(&below.join("/").as_str()) {
                        return Err(malformed());
                    }
                }
                if let Some(parent) = parents.last_mut() {
                    *parent = true;
                }
                path.push(name);
                parents.push(false);
                if empty {
                    let leaf = path[1..].join("/");
                    path.pop();
                    parents.pop();
                    if !leaf.is_empty() {
                        out.push((leaf, String::new()));
                    }
                }
            }
            Event::Text(t) => text.push_str(&t.unescape().map_err(|_| malformed())?),
            Event::End(_) => {
                let leaf = path[1..].join("/");
                let value = std::mem::take(&mut text).trim().to_string();
                if parents.pop() == Some(true) && !value.is_empty() {
                    return Err(malformed());
                }
                if !leaf.is_empty() {
                    out.push((leaf, value));
                }
                path.pop();
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    if !seen_root || !path.is_empty() || !text.trim().is_empty() {
        return Err(malformed());
    }
    let mut seen = std::collections::HashSet::new();
    for (leaf, _) in &out {
        if !seen.insert(leaf.clone()) {
            return Err(malformed());
        }
    }
    Ok(out)
}

fn leaf<'a>(leaves: &'a [(String, String)], path: &str) -> Option<&'a str> {
    leaves
        .iter()
        .find(|(name, _)| name == path)
        .map(|(_, value)| value.as_str())
}

/// A PutObjectLockConfiguration body: `ObjectLockEnabled` (`Enabled`) and
/// an optional default retention.
fn parse_configuration(body: &[u8]) -> Result<Option<DefaultRetention>, S3Error> {
    let found = leaves(
        body,
        "ObjectLockConfiguration",
        &[
            "ObjectLockEnabled",
            "Rule",
            "Rule/DefaultRetention",
            "Rule/DefaultRetention/Mode",
            "Rule/DefaultRetention/Days",
            "Rule/DefaultRetention/Years",
            "Rule/DefaultRetention/DefaultEventHold",
            "Rule/DefaultRetention/DefaultEventHold/Days",
            "Rule/DefaultRetention/DefaultEventHold/Years",
        ],
    )?;
    if leaf(&found, "ObjectLockEnabled") != Some("Enabled") {
        return Err(malformed());
    }
    if leaf(&found, "Rule").is_none() {
        return Ok(None);
    }
    if leaf(&found, "Rule/DefaultRetention").is_none() {
        return Err(malformed());
    }
    let mode = leaf(&found, "Rule/DefaultRetention/Mode")
        .ok_or_else(|| invalid("A default retention needs a Mode"))?;
    let period = parse_duration(
        leaf(&found, "Rule/DefaultRetention/Days"),
        leaf(&found, "Rule/DefaultRetention/Years"),
    )?;
    let event = parse_duration(
        leaf(&found, "Rule/DefaultRetention/DefaultEventHold/Days"),
        leaf(&found, "Rule/DefaultRetention/DefaultEventHold/Years"),
    )?;
    if leaf(&found, "Rule/DefaultRetention/DefaultEventHold").is_some() && event.is_none() {
        return Err(invalid("A default event hold needs Days or Years"));
    }
    if period.is_none() && event.is_none() {
        return Err(invalid(
            "A default retention needs a period (Days or Years), a default event hold, or both",
        ));
    }
    Ok(Some(DefaultRetention {
        mode: parse_mode(mode)?,
        period,
        event,
    }))
}

/// `<Days>n</Days>` or `<Years>n</Years>`.
fn duration_xml((years, count): (bool, u32)) -> String {
    let unit = if years { "Years" } else { "Days" };
    format!("<{unit}>{count}</{unit}>")
}

fn render_configuration(default: Option<DefaultRetention>) -> String {
    let rule = match default {
        None => String::new(),
        Some(default) => format!(
            "<Rule><DefaultRetention><Mode>{}</Mode>{}{}</DefaultRetention></Rule>",
            mode_name(default.mode),
            default.period.map(duration_xml).unwrap_or_default(),
            default
                .event
                .map(|event| format!(
                    "<DefaultEventHold>{}</DefaultEventHold>",
                    duration_xml(event)
                ))
                .unwrap_or_default(),
        ),
    };
    format!(
        "<ObjectLockConfiguration xmlns=\"{XMLNS}\"><ObjectLockEnabled>Enabled</ObjectLockEnabled>\
         {rule}</ObjectLockConfiguration>"
    )
}

fn xml(body: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response")
}

fn empty() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .body(full(bytes::Bytes::new()))
        .expect("a valid response")
}

/// Reads a request body that must carry an integrity check (S3 requires
/// one for Object Lock changes), verifying its Content-MD5.
pub(crate) async fn read_checked_body(
    request: Request<Incoming>,
    auth: Authenticated,
    limit: usize,
) -> Result<Vec<u8>, S3Error> {
    if !has_integrity_check(request.headers()) {
        return Err(S3Error::invalid_request(
            "Missing required header for this request: Content-MD5",
        ));
    }
    let content_md5 = request
        .headers()
        .get("content-md5")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = crate::server::read_body(request, auth, limit).await?;
    if let Some(expected) = content_md5 {
        use base64::Engine as _;
        use md5::Digest;
        let ours = base64::engine::general_purpose::STANDARD.encode(md5::Md5::digest(&body));
        if ours != expected.trim() {
            return Err(S3Error::bad_digest(
                "The Content-MD5 you specified did not match what we received.",
            ));
        }
    }
    Ok(body)
}

/// PUT or GET `?object-lock` on `bucket`. A PUT enables Object Lock (the
/// bucket's versioning must be enabled) and sets or removes the default
/// retention.
pub async fn bucket_configuration(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    if method == Method::PUT {
        let body = read_checked_body(request, auth, MAX_BODY).await?;
        let default = parse_configuration(&body)?;
        crate::scope::object(state, move |file| {
            file.transaction(|tx| {
                if let Err(err) = tx.enable_object_lock(&name)? {
                    return Ok(Err(err));
                }
                match default {
                    Some(default) => {
                        tx.put_bucket_config(&name, OBJECT_LOCK_DEFAULT, "", &default.encode())?
                    }
                    None => {
                        tx.delete_bucket_config(&name, OBJECT_LOCK_DEFAULT, "")?;
                    }
                }
                Ok(Ok(()))
            })
        })
        .await?
        .map_err(|err| crate::api::bucket_error(err, &bucket))?;
        return Ok(empty());
    }
    let (exists, (enabled, default)) = crate::scope::object(state, move |file| {
        Ok((file.bucket(&name)?.is_some(), bucket_state(file, &name)?))
    })
    .await?;
    if !exists {
        return Err(crate::api::bucket_error(BucketError::NoSuchBucket, &bucket));
    }
    if !enabled {
        return Err(S3Error::new(
            404,
            "ObjectLockConfigurationNotFoundError",
            "Object Lock configuration does not exist for this bucket",
        ));
    }
    Ok(xml(render_configuration(default)))
}

/// The version a `?retention` or `?legal-hold` request names.
fn which(request: &Request<Incoming>) -> Result<VersionSel, S3Error> {
    crate::objects::version_sel(request.uri().query().unwrap_or(""))
}

/// The lock of the version a GET `?retention` or `?legal-hold` names.
async fn read_lock(
    state: &S3State,
    bucket: &str,
    key: &str,
    which: VersionSel,
) -> Result<ObjectLock, S3Error> {
    let (bucket_name, key_name) = (bucket.to_string(), key.to_string());
    let found = crate::scope::object(state, move |file| {
        let (enabled, _) = bucket_state(file, &bucket_name)?;
        let info = match file.head_object(&bucket_name, &key_name, which)? {
            Ok(info) => info,
            Err(err) => return Ok(Err(err)),
        };
        if info.delete_marker {
            return Ok(Err(match which {
                VersionSel::Latest => truthdb_object::ObjectError::NoSuchKey,
                _ => truthdb_object::ObjectError::DeleteMarker,
            }));
        }
        if !enabled {
            return Ok(Err(truthdb_object::ObjectError::ObjectLockNotEnabled));
        }
        Ok(Ok(file.object_lock(info.version)?))
    })
    .await?;
    found.map_err(|err| crate::objects::object_error(err, bucket, key))
}

fn no_lock_configuration() -> S3Error {
    S3Error::new(
        404,
        "NoSuchObjectLockConfiguration",
        "The specified object does not have a ObjectLock configuration",
    )
}

/// PUT or GET `?retention` on an object version. A PUT is authorized here,
/// after its body is read, against the retention the body asks for (the
/// condition keys), and bypasses GOVERNANCE retention when its header says
/// so and it may.
#[allow(clippy::too_many_arguments)]
pub async fn retention(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    authz: &Authz,
    action: &str,
    bucket: String,
    key: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let which = which(&request)?;
    if method == Method::GET {
        let lock = read_lock(state, &bucket, &key, which).await?;
        let retention = lock.retention.ok_or_else(no_lock_configuration)?;
        let event = retention
            .event
            .map(|event| {
                format!(
                    "<EventHold>{}</EventHold><EventHoldDuration>{}</EventHoldDuration>",
                    if event.on { "ON" } else { "OFF" },
                    duration_xml((event.years, event.count))
                )
            })
            .unwrap_or_default();
        return Ok(xml(format!(
            "<Retention xmlns=\"{XMLNS}\"><Mode>{}</Mode><RetainUntilDate>{}</RetainUntilDate>\
             {event}</Retention>",
            mode_name(retention.mode),
            crate::time::iso8601(retention.effective_until(crate::objects::now_ms()))
        )));
    }
    let bypass = bypasses(request.headers(), authz, Resource::Object(&bucket, &key));
    let body = read_checked_body(request, auth, MAX_BODY).await?;
    // Taken once the body is in (a slow body must not date the change
    // early); the engine takes its own when it applies it.
    let now = crate::objects::now_ms();
    let found = leaves(
        &body,
        "Retention",
        &[
            "Mode",
            "RetainUntilDate",
            "EventHold",
            "EventHoldDuration",
            "EventHoldDuration/Days",
            "EventHoldDuration/Years",
        ],
    )?;
    let duration = parse_duration(
        leaf(&found, "EventHoldDuration/Days"),
        leaf(&found, "EventHoldDuration/Years"),
    )?;
    if leaf(&found, "EventHoldDuration").is_some() && duration.is_none() {
        return Err(invalid("An event hold duration needs Days or Years"));
    }
    // An empty Retention removes it (with bypass, for GOVERNANCE).
    let change = match asked_retention(
        leaf(&found, "Mode"),
        leaf(&found, "RetainUntilDate"),
        leaf(&found, "EventHold"),
        duration,
        now,
    )? {
        Asked::None => RetentionChange::Remove,
        Asked::Retention(retention) => RetentionChange::Set(retention),
        Asked::Release(mode, until) => RetentionChange::Release {
            mode,
            until_ms: until.unwrap_or(0),
        },
    };
    // Authorized here, once, with the retention the body asks for (none,
    // for a removal): headers say nothing about it.
    let mut context = authz.context().clone();
    match change {
        RetentionChange::Set(retention) => set_condition_keys(
            &mut context,
            &ObjectLock {
                retention: Some(retention),
                legal_hold: None,
            },
            now,
        ),
        RetentionChange::Release { mode, until_ms } => {
            context.set("s3:object-lock-mode", vec![mode_name(mode).to_string()]);
            context.set("s3:object-lock-event-hold", vec!["OFF".to_string()]);
            // The date it names, a minimum: the one stored may be later.
            if until_ms > 0 {
                context.set(
                    "s3:object-lock-retain-until-date",
                    vec![crate::time::iso8601(until_ms)],
                );
                context.set(
                    "s3:object-lock-remaining-retention-days",
                    vec![until_ms.saturating_sub(now).div_ceil(DAY_MS).to_string()],
                );
            }
        }
        RetentionChange::Remove => {}
    }
    // Authorized in the change's own lock hold: a fixed date set on a held
    // version releases its hold, which policies see as a release
    // (s3:object-lock-event-hold OFF), from the version as it is then.
    let (authz, action) = (authz.clone(), action.to_string());
    let (bucket_name, key_name) = (bucket.clone(), key.clone());
    crate::scope::object(state, move |file| {
        let fixed = matches!(change, RetentionChange::Set(retention) if retention.event.is_none());
        if fixed && let Ok(info) = file.head_object(&bucket_name, &key_name, which)? {
            let held = file
                .object_lock(info.version)?
                .retention
                .and_then(|retention| retention.event)
                .is_some_and(|event| event.on);
            if held {
                context.set("s3:object-lock-event-hold", vec!["OFF".to_string()]);
            }
        }
        if let Err(err) =
            authz.check_with(&action, Resource::Object(&bucket_name, &key_name), &context)
        {
            return Ok(Err(err));
        }
        let now = crate::objects::now_ms();
        Ok(Ok(file.transaction(|tx| {
            tx.put_retention(&bucket_name, &key_name, which, change, bypass, now)
        })?))
    })
    .await??
    .map_err(|err| crate::objects::object_error(err, &bucket, &key))?;
    Ok(empty())
}

/// PUT or GET `?legal-hold` on an object version. A PUT is authorized here,
/// after its body is read, against the status the body asks for (the
/// condition key).
#[allow(clippy::too_many_arguments)]
pub async fn legal_hold(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    authz: &Authz,
    action: &str,
    bucket: String,
    key: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let which = which(&request)?;
    if method == Method::GET {
        let lock = read_lock(state, &bucket, &key, which).await?;
        let on = lock.legal_hold.ok_or_else(no_lock_configuration)?;
        return Ok(xml(format!(
            "<LegalHold xmlns=\"{XMLNS}\"><Status>{}</Status></LegalHold>",
            if on { "ON" } else { "OFF" }
        )));
    }
    let body = read_checked_body(request, auth, MAX_BODY).await?;
    let found = leaves(&body, "LegalHold", &["Status"])?;
    let on = parse_hold(leaf(&found, "Status").ok_or_else(malformed)?)?;
    let mut context = authz.context().clone();
    set_condition_keys(
        &mut context,
        &ObjectLock {
            retention: None,
            legal_hold: Some(on),
        },
        crate::objects::now_ms(),
    );
    authz.check_with(action, Resource::Object(&bucket, &key), &context)?;
    let (bucket_name, key_name) = (bucket.clone(), key.clone());
    crate::scope::object(state, move |file| {
        file.transaction(|tx| tx.put_legal_hold(&bucket_name, &key_name, which, on))
    })
    .await?
    .map_err(|err| crate::objects::object_error(err, &bucket, &key))?;
    Ok(empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000_000;

    #[test]
    fn write_headers_ask_for_a_retention_and_a_legal_hold() {
        let ask = |pairs: &[(&str, &str)]| {
            requested(
                |name| {
                    pairs
                        .iter()
                        .find(|(key, _)| *key == name)
                        .map(|(_, value)| *value)
                },
                NOW,
            )
        };
        assert_eq!(ask(&[]).unwrap(), None);
        let lock = ask(&[
            (MODE_HEADER, "COMPLIANCE"),
            (UNTIL_HEADER, "2030-01-02T03:04:05Z"),
            (HOLD_HEADER, "ON"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(
            lock.retention,
            Some(Retention {
                mode: RetentionMode::Compliance,
                until_ms: 1_893_553_445_000,
                event: None,
            })
        );
        assert_eq!(lock.legal_hold, Some(true));
        // A variable retention: an event hold with its duration, and a
        // date as its minimum, or none.
        let held = ask(&[
            (MODE_HEADER, "GOVERNANCE"),
            (EVENT_HOLD_HEADER, "ON"),
            (EVENT_YEARS_HEADER, "3"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(
            held.retention,
            Some(Retention {
                mode: RetentionMode::Governance,
                until_ms: 0,
                event: Some(EventHold {
                    on: true,
                    years: true,
                    count: 3
                }),
            })
        );
        let minimum = ask(&[
            (MODE_HEADER, "GOVERNANCE"),
            (EVENT_HOLD_HEADER, "ON"),
            (EVENT_DAYS_HEADER, "30"),
            (UNTIL_HEADER, "2030-01-02T03:04:05Z"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(minimum.retention.unwrap().until_ms, 1_893_553_445_000);
        for bad in [
            &[(MODE_HEADER, "GOVERNANCE")][..],
            &[(UNTIL_HEADER, "2030-01-02T03:04:05Z")],
            &[
                (MODE_HEADER, "governance"),
                (UNTIL_HEADER, "2030-01-02T03:04:05Z"),
            ],
            &[
                (MODE_HEADER, "GOVERNANCE"),
                (UNTIL_HEADER, "2020-01-02T03:04:05Z"),
            ],
            &[(MODE_HEADER, "GOVERNANCE"), (UNTIL_HEADER, "1999999999")],
            &[(HOLD_HEADER, "on")],
            // An event hold without its mode or duration, both units, a
            // duration out of range, a duration without an event hold, and
            // a release on a new object.
            &[(EVENT_HOLD_HEADER, "ON"), (EVENT_DAYS_HEADER, "1")],
            &[(MODE_HEADER, "GOVERNANCE"), (EVENT_HOLD_HEADER, "ON")],
            &[
                (MODE_HEADER, "GOVERNANCE"),
                (EVENT_HOLD_HEADER, "ON"),
                (EVENT_DAYS_HEADER, "1"),
                (EVENT_YEARS_HEADER, "1"),
            ],
            &[
                (MODE_HEADER, "GOVERNANCE"),
                (EVENT_HOLD_HEADER, "ON"),
                (EVENT_DAYS_HEADER, "36501"),
            ],
            &[(MODE_HEADER, "GOVERNANCE"), (EVENT_DAYS_HEADER, "1")],
            &[(MODE_HEADER, "GOVERNANCE"), (EVENT_HOLD_HEADER, "OFF")],
            &[
                (MODE_HEADER, "GOVERNANCE"),
                (EVENT_HOLD_HEADER, "OFF"),
                (EVENT_DAYS_HEADER, "1"),
                (UNTIL_HEADER, "2030-01-02T03:04:05Z"),
            ],
            &[
                (MODE_HEADER, "GOVERNANCE"),
                (EVENT_HOLD_HEADER, "on"),
                (EVENT_DAYS_HEADER, "1"),
            ],
        ] {
            assert_eq!(ask(bad).unwrap_err().code, "InvalidArgument", "{bad:?}");
        }
    }

    #[test]
    fn configurations_parse_strictly_and_render_back() {
        let config = |rule: &str| {
            format!(
                "<ObjectLockConfiguration xmlns=\"{XMLNS}\"><ObjectLockEnabled>Enabled\
                 </ObjectLockEnabled>{rule}</ObjectLockConfiguration>"
            )
        };
        assert_eq!(parse_configuration(config("").as_bytes()).unwrap(), None);
        let years = parse_configuration(
            config(
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Years>2</Years>\
                 </DefaultRetention></Rule>",
            )
            .as_bytes(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            years,
            DefaultRetention {
                mode: RetentionMode::Governance,
                period: Some((true, 2)),
                event: None,
            }
        );
        assert_eq!(DefaultRetention::decode(&years.encode()), Some(years));
        assert_eq!(
            years.retention(NOW).until_ms,
            NOW + 2 * 365 * DAY_MS,
            "a year is 365 days"
        );
        assert_eq!(
            parse_configuration(render_configuration(Some(years)).as_bytes()).unwrap(),
            Some(years)
        );
        // A default event hold, with or without a period.
        for (rule, expected) in [
            (
                "<DefaultEventHold><Days>90</Days></DefaultEventHold>",
                DefaultRetention {
                    mode: RetentionMode::Compliance,
                    period: None,
                    event: Some((false, 90)),
                },
            ),
            (
                "<Days>365</Days><DefaultEventHold><Years>1</Years></DefaultEventHold>",
                DefaultRetention {
                    mode: RetentionMode::Compliance,
                    period: Some((false, 365)),
                    event: Some((true, 1)),
                },
            ),
        ] {
            let parsed = parse_configuration(
                config(&format!(
                    "<Rule><DefaultRetention><Mode>COMPLIANCE</Mode>{rule}</DefaultRetention></Rule>"
                ))
                .as_bytes(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(parsed, expected, "{rule}");
            assert_eq!(DefaultRetention::decode(&parsed.encode()), Some(parsed));
            assert_eq!(
                parse_configuration(render_configuration(Some(parsed)).as_bytes()).unwrap(),
                Some(parsed)
            );
        }
        let held = DefaultRetention {
            mode: RetentionMode::Compliance,
            period: Some((false, 365)),
            event: Some((false, 90)),
        }
        .retention(NOW);
        assert_eq!(held.until_ms, NOW + 365 * DAY_MS, "the period is a minimum");
        assert_eq!(held.effective_until(NOW), NOW + 365 * DAY_MS);
        assert!(held.event.unwrap().on, "a new object starts held");
        assert_eq!(DefaultRetention::decode(&[1, 0, 0, 0, 0, 0]), None);
        for (rule, code) in [
            (
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days><Years>1</Years></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>0</Days></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>36501</Days></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Years>101</Years></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><DefaultRetention><Days>1</Days></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><DefaultRetention><Mode>WORM</Mode><Days>1</Days></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            ("<Rule></Rule>", "MalformedXML"),
            ("<Rule><Other/></Rule>", "MalformedXML"),
            (
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days><Days>2</Days></DefaultRetention></Rule>",
                "MalformedXML",
            ),
            (
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><DefaultEventHold></DefaultEventHold></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><DefaultEventHold><Days>0</Days></DefaultEventHold></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><DefaultRetention><DefaultEventHold><Days>1</Days></DefaultEventHold></DefaultRetention></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention>text</Rule>",
                "MalformedXML",
            ),
        ] {
            assert_eq!(
                parse_configuration(config(rule).as_bytes())
                    .unwrap_err()
                    .code,
                code,
                "{rule}"
            );
        }
        for bad in [
            "<ObjectLockConfiguration/>",
            "<ObjectLockConfiguration><ObjectLockEnabled>Disabled</ObjectLockEnabled></ObjectLockConfiguration>",
            "<Other><ObjectLockEnabled>Enabled</ObjectLockEnabled></Other>",
            "<ObjectLockConfiguration>text<ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>",
        ] {
            assert_eq!(
                parse_configuration(bad.as_bytes()).unwrap_err().code,
                "MalformedXML",
                "{bad}"
            );
        }
    }

    #[test]
    fn new_objects_take_their_own_lock_or_the_buckets_default() {
        let default = DefaultRetention {
            mode: RetentionMode::Compliance,
            period: Some((false, 3)),
            event: None,
        };
        let held = ObjectLock {
            retention: None,
            legal_hold: Some(true),
        };
        assert_eq!(
            new_object_lock(None, true, Some(default), NOW).unwrap(),
            Some(ObjectLock {
                retention: Some(default.retention(NOW)),
                legal_hold: None
            })
        );
        assert_eq!(
            new_object_lock(Some(held), true, Some(default), NOW).unwrap(),
            Some(ObjectLock {
                retention: Some(default.retention(NOW)),
                legal_hold: Some(true)
            }),
            "a legal hold alone keeps the default retention"
        );
        assert_eq!(new_object_lock(None, true, None, NOW).unwrap(), None);
        assert_eq!(new_object_lock(None, false, None, NOW).unwrap(), None);
        assert_eq!(
            new_object_lock(Some(held), false, None, NOW)
                .unwrap_err()
                .code,
            "InvalidRequest"
        );
    }
}
