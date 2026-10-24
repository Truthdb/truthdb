//! CloudTrail trails: an account's trails (CreateTrail, GetTrail,
//! DescribeTrails, ListTrails, UpdateTrail, DeleteTrail, StartLogging,
//! StopLogging, GetTrailStatus; see `cloudtrail` for the protocol) and the
//! delivery of a logging trail's events as log files into its bucket.
//!
//! A trail is kept as a document of its account (configuration kind
//! `ACCOUNT_TRAIL`, by `<account>/<name>`), at most five an account. It logs
//! the account's management events once started (StartLogging), as the
//! API activity log records them: when the activity is written, its events
//! go to each of the account's logging trails, and every `trail_flush` (or
//! once a trail's reach 1 MiB) each trail's are written as one gzipped
//! `{"Records":[...]}` object at
//! `<prefix>/AWSLogs/<account>/CloudTrail/<region>/YYYY/MM/DD/<account>_CloudTrail_<region>_<YYYYMMDDTHHmmZ>_<16 characters>.json.gz`.
//! The bucket must be the trail's account's, when the trail is made or
//! changed and again when a file is written (else the events are dropped and
//! the trail's status tells the error). Delivery is best effort, as
//! CloudTrail's. SNS notices, CloudWatch Logs, KMS keys, log file
//! validation, tags and organization trails are not supported (refused).

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};
use truthdb_object::{ACCOUNT_TRAIL, BodyWriter, ObjFile, ObjectAttrs, Origin, StorageClass};

use crate::cloudtrail::CtError;
use crate::selectors::Selectors;
use crate::server::S3State;

/// Trails an account has at most.
const MAX_TRAILS: usize = 5;
const MAX_PREFIX: usize = 200;
/// A trail's events written at once when they reach this size, and kept
/// at most while they cannot be written.
const FLUSH_BYTES: usize = 1024 * 1024;
const MAX_KEPT: usize = 4 * FLUSH_BYTES;
const GRACE_MS: u64 = 60_000;

/// A trail as kept: its settings and its logging status.
#[derive(Debug, Clone, Default)]
pub(crate) struct Trail {
    pub name: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub include_global: bool,
    pub multi_region: bool,
    pub recursive: bool,
    pub logging: bool,
    pub started_ms: Option<u64>,
    pub stopped_ms: Option<u64>,
    pub delivered_ms: Option<u64>,
    pub delivery_error: Option<String>,
    /// When it was made (a trail of the same name made again is another).
    pub created_ms: u64,
    /// Its event selectors (none: every management event).
    pub selectors: Option<Selectors>,
}

impl Trail {
    fn to_document(&self) -> Vec<u8> {
        json!({
            "name": self.name,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "include_global": self.include_global,
            "multi_region": self.multi_region,
            "recursive": self.recursive,
            "logging": self.logging,
            "started_ms": self.started_ms,
            "stopped_ms": self.stopped_ms,
            "delivered_ms": self.delivered_ms,
            "delivery_error": self.delivery_error,
            "created_ms": self.created_ms,
            "event_selectors": self.selectors.as_ref().map(|s| s.render().0),
            "advanced_event_selectors": self.selectors.as_ref().map(|s| s.render().1),
        })
        .to_string()
        .into_bytes()
    }

    fn from_document(document: &[u8]) -> Option<Trail> {
        let value: Value = serde_json::from_slice(document).ok()?;
        let text = |name: &str| value[name].as_str().map(str::to_string);
        Some(Trail {
            name: text("name")?,
            bucket: text("bucket")?,
            prefix: text("prefix"),
            include_global: value["include_global"].as_bool().unwrap_or(true),
            multi_region: value["multi_region"].as_bool().unwrap_or(false),
            recursive: value["recursive"].as_bool().unwrap_or(true),
            logging: value["logging"].as_bool().unwrap_or(false),
            started_ms: value["started_ms"].as_u64(),
            stopped_ms: value["stopped_ms"].as_u64(),
            delivered_ms: value["delivered_ms"].as_u64(),
            delivery_error: text("delivery_error"),
            created_ms: value["created_ms"].as_u64().unwrap_or(0),
            selectors: Selectors::from_rendered(
                &value["event_selectors"],
                &value["advanced_event_selectors"],
            ),
        })
    }

    /// The trail as the API returns it.
    fn describe(&self, region: &str, account: &str) -> Value {
        let mut trail = json!({
            "Name": self.name,
            "S3BucketName": self.bucket,
            "IncludeGlobalServiceEvents": self.include_global,
            "IsMultiRegionTrail": self.multi_region,
            "HomeRegion": region,
            "TrailARN": trail_arn(region, account, &self.name),
            "LogFileValidationEnabled": false,
            "HasCustomEventSelectors": self.selectors.is_some(),
            "HasInsightSelectors": false,
            "IsOrganizationTrail": false,
            "RecursiveLogging": self.recursive,
        });
        if let Some(prefix) = &self.prefix {
            trail["S3KeyPrefix"] = json!(prefix);
        }
        trail
    }
}

fn trail_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:cloudtrail:{region}:{account}:trail/{name}")
}

fn trail_id(account: &str, name: &str) -> String {
    format!("{account}/{name}")
}

fn invalid_name(name: &str) -> CtError {
    CtError::new(
        400,
        "InvalidTrailNameException",
        format!("{name} is not a valid trail name."),
    )
}

/// Whether `name` is a trail name as CloudTrail takes them: 3 to 128 ASCII
/// letters, digits, periods, underscores and dashes, starting and ending
/// with a letter or digit, none of `._-` next to another, and not an IP
/// address.
fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    let separator = |byte: &u8| matches!(byte, b'.' | b'_' | b'-');
    (3..=128).contains(&bytes.len())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || separator(byte))
        && bytes[0].is_ascii_alphanumeric()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        && !bytes
            .windows(2)
            .any(|pair| separator(&pair[0]) && separator(&pair[1]))
        && name.parse::<std::net::Ipv4Addr>().is_err()
}

/// The trail name a `Name` parameter (a name, or a trail ARN of this
/// region and account) gives.
fn trail_name(value: &str, region: &str, account: &str) -> Result<String, CtError> {
    let name = match value.strip_prefix("arn:") {
        None => value,
        Some(_) => {
            let parts: Vec<&str> = value.splitn(6, ':').collect();
            let [_, partition, service, arn_region, arn_account, resource] = parts[..] else {
                return Err(invalid_arn(value));
            };
            let Some(name) = resource.strip_prefix("trail/") else {
                return Err(invalid_arn(value));
            };
            if partition != "aws" || service != "cloudtrail" {
                return Err(invalid_arn(value));
            }
            if arn_region != region || arn_account != account {
                return Err(not_found(value));
            }
            name
        }
    };
    if !valid_name(name) {
        return Err(invalid_name(name));
    }
    Ok(name.to_string())
}

fn invalid_arn(value: &str) -> CtError {
    CtError::new(
        400,
        "CloudTrailARNInvalidException",
        format!("{value} is not a valid trail ARN."),
    )
}

fn not_found(name: &str) -> CtError {
    CtError::new(
        400,
        "TrailNotFoundException",
        format!("Unknown trail: {name} for the user."),
    )
}

/// A string parameter.
fn string<'a>(input: &'a Map<String, Value>, name: &str) -> Result<Option<&'a str>, CtError> {
    match input.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(CtError::new(
            400,
            "InvalidParameterException",
            format!("{name} must be a string."),
        )),
    }
}

fn boolean(input: &Map<String, Value>, name: &str) -> Result<Option<bool>, CtError> {
    match input.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(CtError::new(
            400,
            "InvalidParameterException",
            format!("{name} must be a boolean."),
        )),
    }
}

/// Refuses the settings this server does not support.
fn check_supported(input: &Map<String, Value>) -> Result<(), CtError> {
    let unsupported = |what: &str| {
        Err(CtError::new(
            400,
            "UnsupportedOperationException",
            format!("{what} is not supported."),
        ))
    };
    for name in [
        "SnsTopicName",
        "CloudWatchLogsLogGroupArn",
        "CloudWatchLogsRoleArn",
        "KmsKeyId",
    ] {
        if string(input, name)?.is_some() {
            return unsupported(name);
        }
    }
    for name in ["IsOrganizationTrail", "EnableLogFileValidation"] {
        if boolean(input, name)? == Some(true) {
            return unsupported(name);
        }
    }
    if input
        .get("TagsList")
        .and_then(Value::as_array)
        .is_some_and(|tags| !tags.is_empty())
    {
        return unsupported("TagsList");
    }
    Ok(())
}

/// Checks `prefix` (at most 200 characters).
fn check_prefix(prefix: Option<&str>) -> Result<(), CtError> {
    if prefix.is_some_and(|prefix| prefix.chars().count() > MAX_PREFIX) {
        return Err(CtError::new(
            400,
            "InvalidS3PrefixException",
            "The S3KeyPrefix is at most 200 characters.",
        ));
    }
    Ok(())
}

/// Why `bucket` cannot take `account`'s logs, if it cannot.
fn bucket_problem(
    file: &mut ObjFile<'_>,
    bucket: &str,
    account: &str,
) -> Result<Option<CtError>, truthdb_core::storage::StorageError> {
    Ok(match file.bucket(bucket)? {
        None => Some(CtError::new(
            400,
            "S3BucketDoesNotExistException",
            format!("S3 bucket {bucket} does not exist."),
        )),
        Some(info) if info.owner != account => Some(CtError::new(
            400,
            "InsufficientS3BucketPolicyException",
            format!("Incorrect S3 bucket policy is detected for bucket: {bucket}"),
        )),
        Some(_) => None,
    })
}

fn load(
    file: &mut ObjFile<'_>,
    account: &str,
    name: &str,
) -> Result<Option<Trail>, truthdb_core::storage::StorageError> {
    Ok(file
        .bucket_config("", ACCOUNT_TRAIL, &trail_id(account, name))?
        .and_then(|document| Trail::from_document(&document)))
}

/// `account`'s trails, by name.
fn trails_of(
    file: &mut ObjFile<'_>,
    account: &str,
) -> Result<Vec<Trail>, truthdb_core::storage::StorageError> {
    let prefix = format!("{account}/");
    let mut trails = Vec::new();
    for id in file.bucket_config_ids_with("", ACCOUNT_TRAIL, &prefix)? {
        if let Some(name) = id.strip_prefix(&prefix)
            && let Some(trail) = load(file, account, name)?
        {
            trails.push(trail);
        }
    }
    Ok(trails)
}

/// Where a CreateTrail or UpdateTrail would have the trail's logs written:
/// its bucket and prefix, as given or (for an update) as kept; none if the
/// call names neither, or the trail does not exist.
pub(crate) async fn target(
    state: &S3State,
    operation: &str,
    input: &Map<String, Value>,
    account: &str,
) -> Result<Option<(String, Option<String>)>, CtError> {
    let bucket = string(input, "S3BucketName")?.map(str::to_string);
    let prefix = string(input, "S3KeyPrefix")?.map(str::to_string);
    if bucket.is_none() && prefix.is_none() {
        return Ok(None);
    }
    let kept = match operation {
        "UpdateTrail" => {
            let name = trail_name(
                string(input, "Name")?.unwrap_or_default(),
                &state.region,
                account,
            )?;
            let owner = account.to_string();
            state
                .engine
                .run_object(move |file| load(file, &owner, &name))
                .await?
        }
        _ => None,
    };
    let Some(bucket) = bucket.or_else(|| kept.as_ref().map(|trail| trail.bucket.clone())) else {
        return Ok(None);
    };
    let prefix = match prefix {
        Some(prefix) => (!prefix.is_empty()).then_some(prefix),
        None => kept.and_then(|trail| trail.prefix),
    };
    Ok(Some((bucket, prefix)))
}

/// The resource a trail operation acts on, for its authorization: the
/// trail's ARN (or every trail's, for the ones that list them).
pub(crate) fn resource(
    operation: &str,
    input: &Map<String, Value>,
    region: &str,
    account: &str,
) -> Result<String, CtError> {
    match operation {
        "DescribeTrails" | "ListTrails" => Ok("*".to_string()),
        _ => {
            let parameter = match operation {
                "PutEventSelectors" | "GetEventSelectors" => "TrailName",
                _ => "Name",
            };
            let name = string(input, parameter)?.ok_or_else(|| {
                CtError::new(
                    400,
                    "InvalidParameterException",
                    format!("{parameter} is required."),
                )
            })?;
            Ok(trail_arn(
                region,
                account,
                &trail_name(name, region, account)?,
            ))
        }
    }
}

/// Serves trail operation `operation` for `account`.
pub(crate) async fn serve(
    state: &S3State,
    operation: &str,
    input: &Map<String, Value>,
    account: &str,
) -> Result<Value, CtError> {
    let region = state.region.clone();
    let owner = account.to_string();
    match operation {
        "CreateTrail" | "UpdateTrail" => {
            check_supported(input)?;
            let creating = operation == "CreateTrail";
            let name = trail_name(string(input, "Name")?.unwrap_or_default(), &region, account)?;
            let bucket = string(input, "S3BucketName")?.map(str::to_string);
            if creating && bucket.is_none() {
                return Err(CtError::new(
                    400,
                    "InvalidParameterException",
                    "S3BucketName is required.",
                ));
            }
            let prefix = string(input, "S3KeyPrefix")?.map(str::to_string);
            check_prefix(prefix.as_deref())?;
            let (global, multi, recursive) = (
                boolean(input, "IncludeGlobalServiceEvents")?,
                boolean(input, "IsMultiRegionTrail")?,
                boolean(input, "RecursiveLogging")?,
            );
            let made = state
                .engine
                .run_object(move |file| {
                    let existing = load(file, &owner, &name)?;
                    let mut trail = match (existing, creating) {
                        (Some(_), true) => {
                            return Ok(Err(CtError::new(
                                400,
                                "TrailAlreadyExistsException",
                                format!("Trail {name} already exists for customer: {owner}"),
                            )));
                        }
                        (None, false) => return Ok(Err(not_found(&name))),
                        (Some(trail), false) => trail,
                        (None, true) => {
                            if trails_of(file, &owner)?.len() >= MAX_TRAILS {
                                return Ok(Err(CtError::new(
                                    400,
                                    "MaximumNumberOfTrailsExceededException",
                                    format!("User: {owner} already has {MAX_TRAILS} trails."),
                                )));
                            }
                            Trail {
                                name: name.clone(),
                                include_global: true,
                                recursive: true,
                                created_ms: crate::objects::now_ms(),
                                ..Trail::default()
                            }
                        }
                    };
                    if let Some(bucket) = &bucket {
                        if let Some(problem) = bucket_problem(file, bucket, &owner)? {
                            return Ok(Err(problem));
                        }
                        trail.bucket = bucket.clone();
                    }
                    if let Some(prefix) = &prefix {
                        trail.prefix = (!prefix.is_empty()).then(|| prefix.clone());
                    }
                    trail.include_global = global.unwrap_or(trail.include_global);
                    trail.multi_region = multi.unwrap_or(trail.multi_region);
                    trail.recursive = recursive.unwrap_or(trail.recursive);
                    let document = trail.to_document();
                    file.transaction(|tx| {
                        tx.put_bucket_config("", ACCOUNT_TRAIL, &trail_id(&owner, &name), &document)
                    })?;
                    Ok(Ok(trail))
                })
                .await??;
            state.trail_selectors.forget(account);
            let mut answer = made.describe(&region, account);
            if let Some(object) = answer.as_object_mut() {
                object.remove("HomeRegion");
                object.remove("HasCustomEventSelectors");
                object.remove("HasInsightSelectors");
            }
            Ok(answer)
        }
        "GetTrail" | "GetTrailStatus" | "DeleteTrail" | "StartLogging" | "StopLogging" => {
            let name = trail_name(string(input, "Name")?.unwrap_or_default(), &region, account)?;
            let op = operation.to_string();
            let now = crate::objects::now_ms();
            let trail = state
                .engine
                .run_object(move |file| {
                    let Some(mut trail) = load(file, &owner, &name)? else {
                        return Ok(None);
                    };
                    let id = trail_id(&owner, &name);
                    match op.as_str() {
                        "DeleteTrail" => {
                            file.transaction(|tx| tx.delete_bucket_config("", ACCOUNT_TRAIL, &id))?;
                        }
                        "StartLogging" | "StopLogging" => {
                            let start = op == "StartLogging";
                            if trail.logging != start {
                                trail.logging = start;
                                if start {
                                    trail.started_ms = Some(now);
                                } else {
                                    trail.stopped_ms = Some(now);
                                }
                                let document = trail.to_document();
                                file.transaction(|tx| {
                                    tx.put_bucket_config("", ACCOUNT_TRAIL, &id, &document)
                                })?;
                            }
                        }
                        _ => {}
                    }
                    Ok(Some(trail))
                })
                .await?;
            if matches!(operation, "DeleteTrail" | "StartLogging" | "StopLogging") {
                state.trail_selectors.forget(account);
            }
            let trail = trail
                .ok_or_else(|| not_found(string(input, "Name").ok().flatten().unwrap_or("")))?;
            Ok(match operation {
                "GetTrail" => json!({ "Trail": trail.describe(&region, account) }),
                "GetTrailStatus" => {
                    let secs = |ms: Option<u64>| ms.map(|ms| ms as f64 / 1000.0);
                    let mut status = json!({ "IsLogging": trail.logging });
                    for (field, value) in [
                        ("StartLoggingTime", secs(trail.started_ms)),
                        ("StopLoggingTime", secs(trail.stopped_ms)),
                        ("LatestDeliveryTime", secs(trail.delivered_ms)),
                    ] {
                        if let Some(value) = value {
                            status[field] = json!(value);
                        }
                    }
                    if let Some(error) = &trail.delivery_error {
                        status["LatestDeliveryError"] = json!(error);
                    }
                    status
                }
                _ => json!({}),
            })
        }
        "PutEventSelectors" | "GetEventSelectors" => {
            let name = trail_name(
                string(input, "TrailName")?.unwrap_or_default(),
                &region,
                account,
            )?;
            let set = match operation {
                "PutEventSelectors" => Some(Selectors::parse(
                    input.get("EventSelectors"),
                    input.get("AdvancedEventSelectors"),
                )?),
                _ => None,
            };
            let trail = state
                .engine
                .run_object(move |file| {
                    let Some(mut trail) = load(file, &owner, &name)? else {
                        return Ok(None);
                    };
                    if let Some(set) = set {
                        trail.selectors = Some(set);
                        let document = trail.to_document();
                        let id = trail_id(&owner, &name);
                        file.transaction(|tx| {
                            tx.put_bucket_config("", ACCOUNT_TRAIL, &id, &document)
                        })?;
                    }
                    Ok(Some(trail))
                })
                .await?
                .ok_or_else(|| {
                    not_found(string(input, "TrailName").ok().flatten().unwrap_or(""))
                })?;
            if operation == "PutEventSelectors" {
                state.trail_selectors.forget(account);
            }
            let mut answer = json!({ "TrailARN": trail_arn(&region, account, &trail.name) });
            match &trail.selectors {
                Some(selectors) => {
                    let (basic, advanced) = selectors.render();
                    if !basic.is_null() {
                        answer["EventSelectors"] = basic;
                    }
                    if !advanced.is_null() {
                        answer["AdvancedEventSelectors"] = advanced;
                    }
                }
                // The default: every management event.
                None => {
                    answer["EventSelectors"] = json!([{
                        "ReadWriteType": "All",
                        "IncludeManagementEvents": true,
                        "DataResources": [],
                    }]);
                }
            }
            Ok(answer)
        }
        "DescribeTrails" | "ListTrails" => {
            // Names of trails of other accounts or regions match none.
            let names: Option<Vec<String>> = match input.get("trailNameList") {
                Some(Value::Array(list)) if !list.is_empty() => {
                    let mut names = Vec::new();
                    for name in list {
                        match trail_name(name.as_str().unwrap_or_default(), &region, account) {
                            Ok(name) => names.push(name),
                            Err(err) if err.code == "TrailNotFoundException" => {}
                            Err(err) => return Err(err),
                        }
                    }
                    Some(names)
                }
                _ => None,
            };
            let trails = state
                .engine
                .run_object(move |file| trails_of(file, &owner))
                .await?;
            let listed: Vec<Value> = trails
                .iter()
                .filter(|trail| {
                    names
                        .as_ref()
                        .is_none_or(|names| names.contains(&trail.name))
                })
                .map(|trail| {
                    if operation == "ListTrails" {
                        json!({
                            "TrailARN": trail_arn(&region, account, &trail.name),
                            "Name": trail.name,
                            "HomeRegion": region,
                        })
                    } else {
                        trail.describe(&region, account)
                    }
                })
                .collect();
            Ok(if operation == "ListTrails" {
                json!({ "Trails": listed })
            } else {
                json!({ "trailList": listed })
            })
        }
        _ => Err(CtError::new(
            400,
            "UnsupportedOperationException",
            format!("{operation} is not supported."),
        )),
    }
}

/// A logging trail: its name, making and selectors.
pub(crate) type Logging = (String, u64, Option<Selectors>);

/// The logging trails of each of `accounts`.
pub(crate) fn logging_trails(
    file: &mut ObjFile<'_>,
    accounts: &[String],
) -> Result<HashMap<String, Vec<Logging>>, truthdb_core::storage::StorageError> {
    let mut found = HashMap::new();
    for account in accounts {
        let names: Vec<Logging> = trails_of(file, account)?
            .into_iter()
            .filter(|trail| trail.logging)
            .map(|trail| (trail.name, trail.created_ms, trail.selectors))
            .collect();
        if !names.is_empty() {
            found.insert(account.clone(), names);
        }
    }
    Ok(found)
}

/// Each account's logging trails, read once and forgotten when a trail of
/// it changes (only this server changes them): data events consult it for
/// every object request.
#[derive(Default)]
pub struct TrailCache {
    accounts: Mutex<HashMap<String, Arc<Vec<Logging>>>>,
    /// Counts `forget`s: trails read before one are not kept.
    generation: std::sync::atomic::AtomicU64,
}

impl TrailCache {
    pub(crate) fn forget(&self, account: &str) {
        let mut accounts = self
            .accounts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        accounts.remove(account);
    }

    /// Forgets every account's trails.
    pub(crate) fn clear(&self) {
        let mut accounts = self
            .accounts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        accounts.clear();
    }

    /// `account`'s logging trails (none if they cannot be read).
    pub(crate) async fn logging(&self, state: &S3State, account: &str) -> Arc<Vec<Logging>> {
        if let Some(found) = self
            .accounts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(account)
        {
            return Arc::clone(found);
        }
        let generation = self.generation.load(std::sync::atomic::Ordering::Relaxed);
        let owner = account.to_string();
        let Ok(mut found) = state
            .engine
            .run_object(move |file| logging_trails(file, std::slice::from_ref(&owner)))
            .await
        else {
            return Arc::new(Vec::new());
        };
        let trails = Arc::new(found.remove(account).unwrap_or_default());
        // A standby's trails change by replication, not here: not kept.
        if state.storage.is_standby() {
            return trails;
        }
        let mut accounts = self
            .accounts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.generation.load(std::sync::atomic::Ordering::Relaxed) == generation {
            accounts.insert(account.to_string(), Arc::clone(&trails));
        }
        trails
    }
}

/// A trail's records not yet written (each a JSON record, comma-joined).
struct Buffer {
    records: String,
}

/// The trails' events waiting to be written, by account and trail.
#[derive(Default)]
pub struct TrailLogs {
    buffers: Mutex<HashMap<(String, String, u64), Buffer>>,
    /// Woken when a trail's records reach `FLUSH_BYTES`.
    full: tokio::sync::Notify,
}

impl TrailLogs {
    /// Adds `record` to trail `name` of `account` (dropped past
    /// `MAX_KEPT`).
    pub(crate) fn append(&self, account: &str, trail: &Logging, record: &[u8]) {
        let mut buffers = self
            .buffers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let buffer = buffers
            .entry((account.to_string(), trail.0.clone(), trail.1))
            .or_insert_with(|| Buffer {
                records: String::new(),
            });
        let before = buffer.records.len();
        if before + record.len() + 1 > MAX_KEPT {
            return;
        }
        if before > 0 {
            buffer.records.push(',');
        }
        buffer.records.push_str(&String::from_utf8_lossy(record));
        if before < FLUSH_BYTES && buffer.records.len() >= FLUSH_BYTES {
            self.full.notify_one();
        }
    }

    fn take(&self) -> Vec<((String, String, u64), Buffer)> {
        std::mem::take(
            &mut *self
                .buffers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
        .into_iter()
        .collect()
    }

    /// Writes each trail's records as one log file (put back, up to
    /// `MAX_KEPT`, if the engine could not be reached).
    pub(crate) async fn flush(&self, state: &S3State) {
        for ((account, name, created), buffer) in self.take() {
            if !deliver(state, &account, &name, created, &buffer.records).await {
                let mut buffers = self
                    .buffers
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let kept = buffers
                    .entry((account, name, created))
                    .or_insert_with(|| Buffer {
                        records: String::new(),
                    });
                if kept.records.len() + buffer.records.len() < MAX_KEPT {
                    if !kept.records.is_empty() {
                        kept.records.insert(0, ',');
                    }
                    kept.records.insert_str(0, &buffer.records);
                }
            }
        }
    }
}

/// A log file's key.
fn log_key(prefix: Option<&str>, account: &str, region: &str, now_ms: u64) -> String {
    let iso = crate::time::iso8601(now_ms);
    let (year, month, day) = (&iso[0..4], &iso[5..7], &iso[8..10]);
    let stamp = format!("{year}{month}{day}T{}{}Z", &iso[11..13], &iso[14..16]);
    let unique: String = {
        use ring::rand::SecureRandom;
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut bytes = [0u8; 16];
        let _ = ring::rand::SystemRandom::new().fill(&mut bytes);
        bytes
            .iter()
            .map(|byte| ALPHABET[*byte as usize % ALPHABET.len()] as char)
            .collect()
    };
    let prefix = prefix.map_or(String::new(), |prefix| format!("{prefix}/"));
    format!(
        "{prefix}AWSLogs/{account}/CloudTrail/{region}/{year}/{month}/{day}/{account}_CloudTrail_{region}_{stamp}_{unique}.json.gz"
    )
}

/// `records` as a gzipped `{"Records":[...]}`.
fn gzipped(records: &str) -> std::io::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(b"{\"Records\":[")?;
    encoder.write_all(records.as_bytes())?;
    encoder.write_all(b"]}")?;
    encoder.finish()
}

/// Writes `records` as a log file of trail `name` of `account`, if it
/// still exists (the events are dropped otherwise, and when its bucket
/// cannot take them, which its status then tells); whether the engine was
/// reached.
async fn deliver(state: &S3State, account: &str, name: &str, created: u64, records: &str) -> bool {
    let now = crate::objects::now_ms();
    let Ok(body) = gzipped(records) else {
        return true;
    };
    let storage = Arc::clone(&state.storage);
    let sealed = tokio::task::spawn_blocking(move || {
        let mut writer = BodyWriter::begin(&storage, now)?;
        writer.write(&storage, &body, now)?;
        writer.finish(&storage, now)
    })
    .await;
    let Ok(Ok(sealed)) = sealed else {
        return false;
    };
    let (account, name, region) = (account.to_string(), name.to_string(), state.region.clone());
    state
        .engine
        .run_object(move |file| {
            // Its events were taken while it logged; a trail since deleted
            // gets none.
            let Some(mut trail) =
                load(file, &account, &name)?.filter(|trail| trail.created_ms == created)
            else {
                return Ok(());
            };
            let problem = bucket_problem(file, &trail.bucket, &account)?;
            // A bucket's default retention holds for its log files too.
            let (bucket_lock, default) = crate::object_lock::bucket_state(file, &trail.bucket)?;
            let lock = crate::object_lock::new_object_lock(None, bucket_lock, default, now)
                .ok()
                .flatten();
            let key = log_key(trail.prefix.as_deref(), &account, &region, now);
            file.set_origin(Origin {
                operation: "PutObject".to_string(),
                request_id: "cloudtrail".to_string(),
                account: Some(account.clone()),
                ..Origin::default()
            });
            let attrs = ObjectAttrs {
                content_type: Some("application/x-gzip".to_string()),
                ..ObjectAttrs::default()
            };
            let id = trail_id(&account, &name);
            file.transaction(|tx| {
                match &problem {
                    None => match tx.put_object(
                        &trail.bucket,
                        &key,
                        &sealed,
                        &attrs,
                        StorageClass::Standard,
                        now,
                        GRACE_MS,
                    )? {
                        Ok(info) => {
                            if let Some(lock) = &lock {
                                tx.set_lock(info.version, lock)?;
                            }
                            trail.delivered_ms = Some(now);
                            trail.delivery_error = None;
                        }
                        Err(_) => trail.delivery_error = Some("InternalError".to_string()),
                    },
                    Some(problem) => {
                        trail.delivery_error = Some(
                            if problem.code == "S3BucketDoesNotExistException" {
                                "NoSuchBucket"
                            } else {
                                "AccessDenied"
                            }
                            .to_string(),
                        );
                    }
                }
                tx.put_bucket_config("", ACCOUNT_TRAIL, &id, &trail.to_document())
            })
        })
        .await
        .is_ok()
}

/// Writes the trails' logs every `state.trail_flush`, or when a trail's
/// reach `FLUSH_BYTES`, until `shutdown` (sent once the activity, which
/// feeds the trails, has written its last), and once more then. A standby
/// writes (and keeps) nothing.
pub async fn run(state: Arc<S3State>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
    loop {
        let stopping = tokio::select! {
            _ = tokio::time::sleep(state.trail_flush) => false,
            _ = state.trail_logs.full.notified() => false,
            changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        };
        if state.storage.is_standby() {
            state.trail_logs.take();
            // What it read of the trails (replicated, not changed here) is
            // read again once it is promoted.
            state.trail_selectors.clear();
        } else {
            state.trail_logs.flush(&state).await;
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
    fn trail_names_are_checked_as_cloudtrail_checks_them() {
        for good in ["abc", "My-Trail.1", "a_b", &"x".repeat(128)] {
            assert!(valid_name(good), "{good}");
        }
        for bad in [
            "ab",
            &"x".repeat(129),
            "-abc",
            "abc_",
            "my-_namespace",
            "my--namespace",
            "a..b",
            "192.168.5.4",
            "a b",
            "ünï",
        ] {
            assert!(!valid_name(bad), "{bad}");
        }
        let region = "us-east-1";
        let account = "111122223333";
        assert_eq!(trail_name("main", region, account).unwrap(), "main");
        assert_eq!(
            trail_name(
                "arn:aws:cloudtrail:us-east-1:111122223333:trail/main",
                region,
                account
            )
            .unwrap(),
            "main"
        );
        let code = |value: &str| trail_name(value, region, account).unwrap_err().code;
        assert_eq!(
            code("arn:aws:cloudtrail:us-east-1:444455556666:trail/main"),
            "TrailNotFoundException"
        );
        assert_eq!(code("arn:aws:s3:::main"), "CloudTrailARNInvalidException");
        assert_eq!(
            code("arn:aws:cloudtrail:us-east-1:111122223333:main"),
            "CloudTrailARNInvalidException"
        );
        assert_eq!(code("x"), "InvalidTrailNameException");
    }

    #[test]
    fn log_files_are_named_as_cloudtrail_names_them() {
        // 2014-06-20T12:55:00Z
        let ms = 1_403_268_900_000;
        let key = log_key(Some("logs"), "123456789012", "us-west-1", ms);
        let name = key
            .strip_prefix("logs/AWSLogs/123456789012/CloudTrail/us-west-1/2014/06/20/")
            .expect(&key);
        let unique = name
            .strip_prefix("123456789012_CloudTrail_us-west-1_20140620T1255Z_")
            .and_then(|rest| rest.strip_suffix(".json.gz"))
            .expect(&key);
        assert_eq!(unique.len(), 16);
        assert!(unique.chars().all(|c| c.is_ascii_alphanumeric()));
        assert!(log_key(None, "123456789012", "us-west-1", ms).starts_with("AWSLogs/"));
        let mut decoded = String::new();
        std::io::Read::read_to_string(
            &mut flate2::read::GzDecoder::new(&gzipped("{\"a\":1},{\"b\":2}").unwrap()[..]),
            &mut decoded,
        )
        .unwrap();
        assert_eq!(decoded, "{\"Records\":[{\"a\":1},{\"b\":2}]}");
    }
}
