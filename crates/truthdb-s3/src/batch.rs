//! S3 Batch Operations (the S3 Control API under `/v20180820/jobs`):
//! CreateJob, DescribeJob, ListJobs, UpdateJobStatus and UpdateJobPriority,
//! for the caller's own account (`x-amz-account-id`). A job runs one
//! operation on every object a CSV manifest lists: a copy, replacing an
//! object's tags, or deleting them. The manifest is pinned by its ETag; the
//! job is run in the background (see `batch_run`), and its completion
//! report written to the bucket it names.
//!
//! The request is kept as given (its elements, re-rendered), so
//! DescribeJob answers with the job's manifest, operation and report as
//! the request stated them.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use percent_encoding::percent_decode_str;
use truthdb_object::{Job, JobStatus, ObjectAttrs, StorageClass, time::iso8601};

use crate::authz::Authz;
use crate::error::{S3Error, escape};
use crate::object_lock::malformed;
use crate::server::{Authenticated, Body, Connection, S3State};
use crate::xml::{self, Element};

pub(crate) const XMLNS: &str = "http://awss3control.amazonaws.com/doc/2018-08-20/";
/// The largest CreateJob request body taken.
const MAX_REQUEST_BYTES: usize = 256 * 1024;
/// Jobs one ListJobs page lists at most (and by default).
const MAX_LIST: usize = 1000;
/// Tags a job may carry at most.
const MAX_JOB_TAGS: usize = 50;

/// What a job does to each object of its manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Operation {
    Copy(Box<CopySpec>),
    PutTagging(Vec<(String, String)>),
    DeleteTagging,
}

impl Operation {
    /// The operation's name, as ListJobs gives it.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Operation::Copy(_) => "S3PutObjectCopy",
            Operation::PutTagging(_) => "S3PutObjectTagging",
            Operation::DeleteTagging => "S3DeleteObjectTagging",
        }
    }
}

/// A copy: into which bucket, under which key prefix, in which class, with
/// which metadata (`None`: the source's) and tags (`None`: the source's).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopySpec {
    pub bucket: String,
    pub prefix: String,
    pub class: StorageClass,
    pub metadata: Option<ObjectAttrs>,
    pub tags: Option<Vec<(String, String)>>,
}

/// A manifest column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Field {
    Ignore,
    Bucket,
    Key,
    VersionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestSpec {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub etag: String,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReportSpec {
    pub bucket: String,
    pub prefix: String,
    /// Only failed tasks reported (ReportScope FailedTasksOnly).
    pub failed_only: bool,
}

/// A job's request, as the runner needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Spec {
    pub operation: Operation,
    pub manifest: ManifestSpec,
    /// `None`: no report (Enabled false).
    pub report: Option<ReportSpec>,
    pub confirmation: bool,
    pub priority: u32,
    pub token: String,
}

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidRequest", message)
}

/// A bucket's name from its ARN (`arn:<partition>:s3:::<bucket>`), or an
/// object's bucket and key (`.../<key>`).
fn arn_path(arn: &str) -> Option<&str> {
    let mut parts = arn.splitn(6, ':');
    let (arn_word, _partition, service, region, account, rest) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    );
    (arn_word == "arn" && service == "s3" && region.is_empty() && account.is_empty())
        .then_some(rest)
}

fn bucket_arn(arn: &str, what: &str) -> Result<String, S3Error> {
    match arn_path(arn) {
        Some(bucket) if !bucket.is_empty() && !bucket.contains('/') => Ok(bucket.to_string()),
        _ => Err(invalid(format!("{what} is not a bucket ARN"))),
    }
}

fn flag(text: &str) -> Result<bool, S3Error> {
    match text {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(malformed()),
    }
}

fn s3_tags(parent: &Element) -> Result<Vec<(String, String)>, S3Error> {
    parent.only(&["S3Tag"])?;
    parent
        .all("S3Tag")
        .map(|tag| {
            tag.only(&["Key", "Value"])?;
            let key = tag.text_of("Key")?.ok_or_else(malformed)?;
            let value = tag.text_of("Value")?.unwrap_or("");
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

/// Checks and reads a CreateJob request.
pub(crate) fn spec(request: &Element) -> Result<Spec, S3Error> {
    request.only(&[
        "ConfirmationRequired",
        "Operation",
        "Report",
        "ClientRequestToken",
        "Manifest",
        "Description",
        "Priority",
        "RoleArn",
        "Tags",
        "ManifestGenerator",
    ])?;
    if request.one("ManifestGenerator")?.is_some() {
        return Err(S3Error::not_implemented(
            "A job made from a ManifestGenerator",
        ));
    }
    let token = request
        .text_of("ClientRequestToken")?
        .ok_or_else(|| invalid("ClientRequestToken is required"))?;
    if token.is_empty() || token.chars().count() > 64 {
        return Err(invalid("ClientRequestToken is 1 to 64 characters"));
    }
    let priority = request
        .text_of("Priority")?
        .ok_or_else(|| invalid("Priority is required"))?
        .parse::<u32>()
        .ok()
        .filter(|priority| *priority <= i32::MAX as u32)
        .ok_or_else(|| invalid("Priority is 0 to 2147483647"))?;
    let role = request
        .text_of("RoleArn")?
        .ok_or_else(|| invalid("RoleArn is required"))?;
    if !valid_role_arn(role) {
        return Err(invalid("RoleArn is not an IAM role ARN"));
    }
    if let Some(description) = request.text_of("Description")?
        && (description.is_empty() || description.chars().count() > 256)
    {
        return Err(invalid("Description is 1 to 256 characters"));
    }
    if let Some(tags) = request.one("Tags")? {
        let tags = s3_tags(tags)?;
        if tags.len() > MAX_JOB_TAGS
            || tags.iter().any(|(key, value)| {
                key.is_empty() || key.chars().count() > 128 || value.chars().count() > 256
            })
        {
            return Err(invalid("The job's tags are invalid"));
        }
    }
    let confirmation = match request.text_of("ConfirmationRequired")? {
        Some(text) => flag(text)?,
        None => false,
    };
    let operation = operation(
        request
            .one("Operation")?
            .ok_or_else(|| invalid("Operation is required"))?,
    )?;
    let manifest = manifest(
        request
            .one("Manifest")?
            .ok_or_else(|| invalid("Manifest is required"))?,
    )?;
    let report = report(
        request
            .one("Report")?
            .ok_or_else(|| invalid("Report is required"))?,
    )?;
    Ok(Spec {
        operation,
        manifest,
        report,
        confirmation,
        priority,
        token: token.to_string(),
    })
}

/// `arn:<partition>:iam::<12 digits>:role/<name>`.
fn valid_role_arn(arn: &str) -> bool {
    let mut parts = arn.splitn(6, ':');
    let parts: Vec<&str> = parts.by_ref().collect();
    parts.len() == 6
        && arn.len() <= 2048
        && parts[0] == "arn"
        && !parts[1].is_empty()
        && parts[2] == "iam"
        && parts[3].is_empty()
        && parts[4].len() == 12
        && parts[4].bytes().all(|byte| byte.is_ascii_digit())
        && parts[5].starts_with("role/")
}

const OPERATIONS: [&str; 11] = [
    "LambdaInvoke",
    "S3ComputeObjectChecksum",
    "S3DeleteObjectTagging",
    "S3InitiateRestoreObject",
    "S3PutObjectAcl",
    "S3PutObjectCopy",
    "S3PutObjectLegalHold",
    "S3PutObjectRetention",
    "S3PutObjectTagging",
    "S3ReplicateObject",
    "S3UpdateObjectEncryption",
];

fn operation(element: &Element) -> Result<Operation, S3Error> {
    element.only(&OPERATIONS)?;
    let [chosen] = element.children.as_slice() else {
        return Err(invalid("Operation names exactly one operation"));
    };
    match chosen.name.as_str() {
        "S3DeleteObjectTagging" => {
            chosen.only(&[])?;
            Ok(Operation::DeleteTagging)
        }
        "S3PutObjectTagging" => {
            chosen.only(&["TagSet"])?;
            let tags = match chosen.one("TagSet")? {
                Some(set) => s3_tags(set)?,
                None => Vec::new(),
            };
            crate::tagging::validate(&tags)?;
            Ok(Operation::PutTagging(tags))
        }
        "S3PutObjectCopy" => copy(chosen).map(|copy| Operation::Copy(Box::new(copy))),
        other => Err(S3Error::not_implemented(format!(
            "The batch operation {other}"
        ))),
    }
}

fn copy(element: &Element) -> Result<CopySpec, S3Error> {
    const TAKEN: [&str; 6] = [
        "TargetResource",
        "TargetKeyPrefix",
        "StorageClass",
        "MetadataDirective",
        "NewObjectMetadata",
        "NewObjectTagging",
    ];
    if let Some(other) = element
        .children
        .iter()
        .find(|child| !TAKEN.contains(&child.name.as_str()))
    {
        return Err(S3Error::not_implemented(format!(
            "{} in a batch copy",
            other.name
        )));
    }
    let bucket = bucket_arn(
        element
            .text_of("TargetResource")?
            .ok_or_else(|| invalid("TargetResource is required"))?,
        "TargetResource",
    )?;
    let prefix = element
        .text_of("TargetKeyPrefix")?
        .unwrap_or("")
        .to_string();
    let class = match element.text_of("StorageClass")? {
        Some(name) => StorageClass::parse(name)
            .ok_or_else(|| invalid("The storage class you specified is not valid"))?,
        None => StorageClass::Standard,
    };
    let replace = match element.text_of("MetadataDirective")? {
        None | Some("COPY") => false,
        Some("REPLACE") => true,
        Some(_) => return Err(invalid("MetadataDirective is COPY or REPLACE")),
    };
    let metadata = match (replace, element.one("NewObjectMetadata")?) {
        (true, Some(given)) => Some(metadata(given)?),
        (true, None) => Some(ObjectAttrs::default()),
        (false, _) => None,
    };
    let tags = match element.one("NewObjectTagging")? {
        Some(set) => {
            let tags = s3_tags(set)?;
            crate::tagging::validate(&tags)?;
            Some(tags)
        }
        None => None,
    };
    Ok(CopySpec {
        bucket,
        prefix,
        class,
        metadata,
        tags,
    })
}

fn metadata(element: &Element) -> Result<ObjectAttrs, S3Error> {
    element.only(&[
        "CacheControl",
        "ContentDisposition",
        "ContentEncoding",
        "ContentLanguage",
        "ContentLength",
        "ContentMD5",
        "ContentType",
        "HttpExpiresDate",
        "RequesterCharged",
        "SSEAlgorithm",
        "UserMetadata",
    ])?;
    match element.text_of("SSEAlgorithm")? {
        None | Some("AES256") => {}
        Some(other) => {
            return Err(S3Error::not_implemented(format!(
                "SSEAlgorithm {other} in a batch copy"
            )));
        }
    }
    let text = |name: &str| -> Result<Option<String>, S3Error> {
        Ok(element.text_of(name)?.map(str::to_string))
    };
    let mut user = Vec::new();
    if let Some(entries) = element.one("UserMetadata")? {
        entries.only(&["entry"])?;
        for entry in entries.all("entry") {
            entry.only(&["key", "value"])?;
            let key = entry.text_of("key")?.ok_or_else(malformed)?;
            user.push((
                key.to_ascii_lowercase(),
                entry.text_of("value")?.unwrap_or("").to_string(),
            ));
        }
    }
    Ok(ObjectAttrs {
        content_type: text("ContentType")?,
        cache_control: text("CacheControl")?,
        content_disposition: text("ContentDisposition")?,
        content_encoding: text("ContentEncoding")?,
        content_language: text("ContentLanguage")?,
        expires: element
            .text_of("HttpExpiresDate")?
            .map(|date| date.to_string()),
        user,
        ..ObjectAttrs::default()
    })
}

fn manifest(element: &Element) -> Result<ManifestSpec, S3Error> {
    element.only(&["Spec", "Location"])?;
    let spec = element
        .one("Spec")?
        .ok_or_else(|| invalid("Manifest Spec is required"))?;
    spec.only(&["Format", "Fields"])?;
    match spec.text_of("Format")? {
        Some("S3BatchOperations_CSV_20180820") => {}
        Some("S3InventoryReport_CSV_20161130") => {
            return Err(S3Error::not_implemented(
                "An inventory report as a job's manifest",
            ));
        }
        _ => return Err(invalid("The manifest format is not valid")),
    }
    let fields = match spec.one("Fields")? {
        Some(fields) => {
            fields.only(&["member"])?;
            fields
                .all("member")
                .map(|member| match member.text.as_str() {
                    "Ignore" => Ok(Field::Ignore),
                    "Bucket" => Ok(Field::Bucket),
                    "Key" => Ok(Field::Key),
                    "VersionId" => Ok(Field::VersionId),
                    _ => Err(invalid(
                        "A manifest field is Ignore, Bucket, Key or VersionId",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        None => vec![Field::Bucket, Field::Key],
    };
    let count = |field| fields.iter().filter(|f| **f == field).count();
    if count(Field::Bucket) != 1 || count(Field::Key) != 1 || count(Field::VersionId) > 1 {
        return Err(invalid(
            "The manifest fields name Bucket and Key once each, and VersionId at most once",
        ));
    }
    let location = element
        .one("Location")?
        .ok_or_else(|| invalid("Manifest Location is required"))?;
    location.only(&["ObjectArn", "ObjectVersionId", "ETag"])?;
    let arn = location
        .text_of("ObjectArn")?
        .ok_or_else(|| invalid("The manifest's ObjectArn is required"))?;
    let (bucket, key) = arn_path(arn)
        .and_then(|path| path.split_once('/'))
        .filter(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
        .ok_or_else(|| invalid("The manifest's ObjectArn is not an object ARN"))?;
    let etag = location
        .text_of("ETag")?
        .filter(|etag| !etag.is_empty())
        .ok_or_else(|| invalid("The manifest's ETag is required"))?;
    Ok(ManifestSpec {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: location.text_of("ObjectVersionId")?.map(str::to_string),
        etag: etag.trim_matches('"').to_string(),
        fields,
    })
}

fn report(element: &Element) -> Result<Option<ReportSpec>, S3Error> {
    element.only(&[
        "Bucket",
        "Format",
        "Enabled",
        "Prefix",
        "ReportScope",
        "ExpectedBucketOwner",
    ])?;
    let enabled = flag(
        element
            .text_of("Enabled")?
            .ok_or_else(|| invalid("Report Enabled is required"))?,
    )?;
    if !enabled {
        return Ok(None);
    }
    let bucket = bucket_arn(
        element
            .text_of("Bucket")?
            .ok_or_else(|| invalid("An enabled report needs its Bucket"))?,
        "The report Bucket",
    )?;
    if element.text_of("Format")? != Some("Report_CSV_20180820") {
        return Err(invalid("An enabled report's Format is Report_CSV_20180820"));
    }
    let failed_only = match element.text_of("ReportScope")? {
        None | Some("AllTasks") => false,
        Some("FailedTasksOnly") => true,
        Some(_) => return Err(invalid("ReportScope is AllTasks or FailedTasksOnly")),
    };
    let prefix = element.text_of("Prefix")?.unwrap_or("");
    if prefix.chars().count() > 512 {
        return Err(invalid("The report Prefix is at most 512 characters"));
    }
    Ok(Some(ReportSpec {
        bucket,
        prefix: prefix.to_string(),
        failed_only,
    }))
}

/// An element as XML (its text escaped).
pub(crate) fn render(element: &Element, out: &mut String) {
    out.push('<');
    out.push_str(&element.name);
    out.push('>');
    if element.children.is_empty() {
        out.push_str(&escape(&element.text));
    }
    for child in &element.children {
        render(child, out);
    }
    out.push_str("</");
    out.push_str(&element.name);
    out.push('>');
}

/// A job's stored request, as elements.
pub(crate) fn stored_request(job: &Job) -> Result<Element, S3Error> {
    xml::parse(&job.request, "CreateJobRequest")
        .map_err(|_| S3Error::internal(format!("job {} has a corrupt request", job.id())))
}

fn xml_answer(body: String) -> Response<Body> {
    Response::builder()
        .status(200)
        .header("content-type", "application/xml")
        .body(
            Full::new(Bytes::from(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
            )))
            .map_err(|never| match never {})
            .boxed(),
        )
        .expect("a valid response")
}

fn not_found() -> S3Error {
    S3Error::new(400, "NotFoundException", "The specified job was not found")
}

/// The query's values of `name`, percent-decoded.
fn query_values(query: &str, name: &str) -> Vec<String> {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('=').or(Some((pair, ""))))
        .filter(|(key, _)| *key == name)
        .map(|(_, value)| {
            percent_decode_str(&value.replace('+', " "))
                .decode_utf8_lossy()
                .into_owned()
        })
        .collect()
}

/// Handles a request under `/v20180820/jobs`.
pub async fn handle(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let Some(principal) = auth.principal.clone() else {
        return Err(S3Error::access_denied("Access Denied"));
    };
    let header_account = request
        .headers()
        .get("x-amz-account-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if header_account != principal.account {
        return Err(S3Error::access_denied("Access Denied"));
    }
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let rest = path.strip_prefix("/v20180820/jobs").unwrap_or("");
    let segments: Vec<&str> = rest.split('/').skip(1).collect();
    let (action, job_id, sub) = match (&method, segments.as_slice()) {
        (&Method::POST, []) => ("s3:CreateJob", None, ""),
        (&Method::GET, []) => ("s3:ListJobs", None, ""),
        (&Method::GET, [id]) => ("s3:DescribeJob", Some(*id), ""),
        (&Method::POST, [id, "status"]) => ("s3:UpdateJobStatus", Some(*id), "status"),
        (&Method::POST, [id, "priority"]) => ("s3:UpdateJobPriority", Some(*id), "priority"),
        _ => {
            return Err(S3Error::not_implemented(format!("{method} {path}")));
        }
    };
    let authz = Authz::load(
        state,
        Some(&principal),
        auth.signed,
        request.headers(),
        &query,
        request.extensions().get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    authz.check(action, crate::authz::Resource::Service)?;
    let account = principal.account.clone();
    let job_id = job_id.map(str::to_string);
    let scope = crate::scope::Scope {
        key: Some((principal.access_key.clone(), principal.account.clone())),
        buckets: Vec::new(),
        origin: None,
    };
    crate::scope::within(scope, async move {
        match (job_id, sub) {
            (None, _) if method == Method::POST => create(state, request, auth, account).await,
            (None, _) => list(state, &account, &query).await,
            (Some(id), "") => describe(state, &account, &id).await,
            (Some(id), "status") => update_status(state, &account, &id, &query).await,
            (Some(id), _) => update_priority(state, &account, &id, &query).await,
        }
    })
    .await
}

async fn create(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    account: String,
) -> Result<Response<Body>, S3Error> {
    let body = crate::server::read_body(request, auth, MAX_REQUEST_BYTES).await?;
    let parsed = xml::parse(&body, "CreateJobRequest")?;
    let spec = spec(&parsed)?;
    let mut rendered = String::new();
    render(&parsed, &mut rendered);
    let fingerprint = ring::digest::digest(&ring::digest::SHA256, rendered.as_bytes())
        .as_ref()
        .to_vec();
    let mut job = Job::new(
        &account,
        &spec.token,
        fingerprint.clone(),
        rendered.into_bytes(),
    );
    job.priority = spec.priority;
    let tag = {
        use ring::rand::SecureRandom;
        let mut bytes = [0u8; 8];
        ring::rand::SystemRandom::new()
            .fill(&mut bytes)
            .map_err(|_| S3Error::internal("no randomness for a job id"))?;
        u64::from_le_bytes(bytes)
    };
    let now = crate::objects::now_ms();
    let (job, made) = crate::scope::object(state, move |file| {
        file.transaction(|tx| tx.create_job(job, tag, now))
    })
    .await?;
    if !made && job.fingerprint != fingerprint {
        return Err(S3Error::new(
            400,
            "IdempotencyException",
            "The ClientRequestToken was used for another job's request",
        ));
    }
    Ok(xml_answer(format!(
        "<CreateJobResult xmlns=\"{XMLNS}\"><JobId>{}</JobId></CreateJobResult>",
        job.id()
    )))
}

/// The account's job with id `id`.
async fn find(state: &S3State, account: &str, id: &str) -> Result<Job, S3Error> {
    let (number, tag) = Job::parse_id(id).ok_or_else(not_found)?;
    let account = account.to_string();
    crate::scope::object(state, move |file| file.job(number))
        .await?
        .filter(|job| job.tag == tag && job.account == account)
        .ok_or_else(not_found)
}

fn progress(job: &Job, now_ms: u64) -> String {
    let active_secs = job.active_since_ms.map_or(0, |since| {
        job.terminated_ms.unwrap_or(now_ms).saturating_sub(since) / 1000
    });
    format!(
        "<ProgressSummary><NumberOfTasksFailed>{}</NumberOfTasksFailed>\
         <NumberOfTasksSucceeded>{}</NumberOfTasksSucceeded>\
         <Timers><ElapsedTimeInActiveSeconds>{active_secs}</ElapsedTimeInActiveSeconds></Timers>\
         <TotalNumberOfTasks>{}</TotalNumberOfTasks></ProgressSummary>",
        job.failed, job.succeeded, job.total
    )
}

async fn describe(state: &S3State, account: &str, id: &str) -> Result<Response<Body>, S3Error> {
    let job = find(state, account, id).await?;
    let request = stored_request(&job)?;
    let spec = spec(&request)?;
    let now = crate::objects::now_ms();
    let mut out = format!("<DescribeJobResult xmlns=\"{XMLNS}\"><Job>");
    out.push_str(&format!(
        "<ConfirmationRequired>{}</ConfirmationRequired><CreationTime>{}</CreationTime>",
        spec.confirmation,
        iso8601(job.created_ms)
    ));
    if let Some(description) = request.text_of("Description")? {
        out.push_str(&format!(
            "<Description>{}</Description>",
            escape(description)
        ));
    }
    if !job.failures.is_empty() {
        out.push_str("<FailureReasons>");
        for (code, reason) in &job.failures {
            out.push_str(&format!(
                "<JobFailure><FailureCode>{}</FailureCode><FailureReason>{}</FailureReason></JobFailure>",
                escape(code),
                escape(reason)
            ));
        }
        out.push_str("</FailureReasons>");
    }
    out.push_str(&format!(
        "<JobArn>arn:aws:s3:{}:{}:job/{}</JobArn><JobId>{}</JobId>",
        escape(&state.region),
        job.account,
        job.id(),
        job.id()
    ));
    for name in ["Manifest", "Operation"] {
        if let Some(element) = request.one(name)? {
            render(element, &mut out);
        }
    }
    out.push_str(&format!("<Priority>{}</Priority>", job.priority));
    out.push_str(&progress(&job, now));
    if let Some(element) = request.one("Report")? {
        render(element, &mut out);
    }
    if let Some(role) = request.text_of("RoleArn")? {
        out.push_str(&format!("<RoleArn>{}</RoleArn>", escape(role)));
    }
    out.push_str(&format!("<Status>{}</Status>", job.status.as_str()));
    if !job.status_reason.is_empty() {
        out.push_str(&format!(
            "<StatusUpdateReason>{}</StatusUpdateReason>",
            escape(&job.status_reason)
        ));
    }
    if let Some(at) = job.suspended_ms {
        out.push_str(&format!(
            "<SuspendedCause>AwaitingConfirmation</SuspendedCause><SuspendedDate>{}</SuspendedDate>",
            iso8601(at)
        ));
    }
    if let Some(at) = job.terminated_ms {
        out.push_str(&format!(
            "<TerminationDate>{}</TerminationDate>",
            iso8601(at)
        ));
    }
    out.push_str("</Job></DescribeJobResult>");
    Ok(xml_answer(out))
}

async fn list(state: &S3State, account: &str, query: &str) -> Result<Response<Body>, S3Error> {
    let mut statuses = Vec::new();
    for value in query_values(query, "jobStatuses") {
        for name in value.split(',').filter(|name| !name.is_empty()) {
            statuses.push(JobStatus::parse(name).ok_or_else(|| {
                S3Error::new(
                    400,
                    "InvalidRequestException",
                    format!("Unknown job status {name}"),
                )
            })?);
        }
    }
    let max = match query_values(query, "maxResults").first() {
        Some(text) => text
            .parse::<usize>()
            .ok()
            .filter(|max| *max <= MAX_LIST)
            .ok_or_else(|| {
                S3Error::new(400, "InvalidRequestException", "maxResults is 0 to 1000")
            })?,
        None => MAX_LIST,
    };
    let after = match query_values(query, "nextToken").first() {
        Some(token) => Some(u64::from_str_radix(token, 16).map_err(|_| {
            S3Error::new(
                400,
                "InvalidNextTokenException",
                "The nextToken is not valid",
            )
        })?),
        None => None,
    };
    let jobs = crate::scope::object(state, |file| file.jobs()).await?;
    let mut matching = jobs.into_iter().filter(|job| {
        job.account == account
            && (statuses.is_empty() || statuses.contains(&job.status))
            && after.is_none_or(|after| job.number > after)
    });
    let page: Vec<Job> = matching.by_ref().take(max).collect();
    let more = matching.next().is_some();
    let now = crate::objects::now_ms();
    let mut out = format!("<ListJobsResult xmlns=\"{XMLNS}\"><Jobs>");
    for job in &page {
        let request = stored_request(job)?;
        let operation = spec(&request)?.operation;
        out.push_str(&format!(
            "<JobListDescriptor><CreationTime>{}</CreationTime>",
            iso8601(job.created_ms)
        ));
        if let Some(description) = request.text_of("Description")? {
            out.push_str(&format!(
                "<Description>{}</Description>",
                escape(description)
            ));
        }
        out.push_str(&format!(
            "<JobId>{}</JobId><Operation>{}</Operation><Priority>{}</Priority>{}<Status>{}</Status>",
            job.id(),
            operation.name(),
            job.priority,
            progress(job, now),
            job.status.as_str()
        ));
        if let Some(at) = job.terminated_ms {
            out.push_str(&format!(
                "<TerminationDate>{}</TerminationDate>",
                iso8601(at)
            ));
        }
        out.push_str("</JobListDescriptor>");
    }
    out.push_str("</Jobs>");
    if more && let Some(last) = page.last() {
        out.push_str(&format!("<NextToken>{:016x}</NextToken>", last.number));
    }
    out.push_str("</ListJobsResult>");
    Ok(xml_answer(out))
}

async fn update_status(
    state: &S3State,
    account: &str,
    id: &str,
    query: &str,
) -> Result<Response<Body>, S3Error> {
    let requested = query_values(query, "requestedJobStatus");
    let requested = match requested.first().map(String::as_str) {
        Some("Ready") => JobStatus::Ready,
        Some("Cancelled") => JobStatus::Cancelled,
        _ => {
            return Err(S3Error::new(
                400,
                "BadRequestException",
                "requestedJobStatus is Ready or Cancelled",
            ));
        }
    };
    let reason = query_values(query, "statusUpdateReason")
        .into_iter()
        .next()
        .unwrap_or_default();
    if reason.chars().count() > 256 {
        return Err(S3Error::new(
            400,
            "BadRequestException",
            "statusUpdateReason is at most 256 characters",
        ));
    }
    let number = find(state, account, id).await?.number;
    let job = crate::scope::object(state, move |file| {
        file.transaction(|tx| {
            let Some(mut job) = tx.job(number)? else {
                return Ok(Err(not_found()));
            };
            let next = match (requested, job.status) {
                (JobStatus::Ready, JobStatus::Suspended) => JobStatus::Ready,
                (
                    JobStatus::Cancelled,
                    JobStatus::New
                    | JobStatus::Preparing
                    | JobStatus::Suspended
                    | JobStatus::Ready
                    | JobStatus::Active,
                ) => JobStatus::Cancelling,
                (_, current) => {
                    return Ok(Err(S3Error::new(
                        400,
                        "JobStatusException",
                        format!(
                            "A {} job cannot be made {}",
                            current.as_str(),
                            requested.as_str()
                        ),
                    )));
                }
            };
            job.status = next;
            if next == JobStatus::Ready {
                job.suspended_ms = None;
            }
            if !reason.is_empty() {
                job.status_reason = reason;
            }
            tx.put_job(&job)?;
            Ok(Ok(job))
        })
    })
    .await??;
    let mut out = format!(
        "<UpdateJobStatusResult xmlns=\"{XMLNS}\"><JobId>{}</JobId><Status>{}</Status>",
        job.id(),
        job.status.as_str()
    );
    if !job.status_reason.is_empty() {
        out.push_str(&format!(
            "<StatusUpdateReason>{}</StatusUpdateReason>",
            escape(&job.status_reason)
        ));
    }
    out.push_str("</UpdateJobStatusResult>");
    Ok(xml_answer(out))
}

async fn update_priority(
    state: &S3State,
    account: &str,
    id: &str,
    query: &str,
) -> Result<Response<Body>, S3Error> {
    let priority = query_values(query, "priority")
        .first()
        .and_then(|text| text.parse::<u32>().ok())
        .filter(|priority| *priority <= i32::MAX as u32)
        .ok_or_else(|| S3Error::new(400, "BadRequestException", "priority is 0 to 2147483647"))?;
    let number = find(state, account, id).await?.number;
    let job = crate::scope::object(state, move |file| {
        file.transaction(|tx| {
            let Some(mut job) = tx.job(number)? else {
                return Ok(None);
            };
            job.priority = priority;
            tx.put_job(&job)?;
            Ok(Some(job))
        })
    })
    .await?
    .ok_or_else(not_found)?;
    Ok(xml_answer(format!(
        "<UpdateJobPriorityResult xmlns=\"{XMLNS}\"><JobId>{}</JobId><Priority>{}</Priority></UpdateJobPriorityResult>",
        job.id(),
        job.priority
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(operation: &str, manifest: &str, report: &str) -> String {
        format!(
            "<CreateJobRequest xmlns=\"{XMLNS}\"><ClientRequestToken>t1</ClientRequestToken>\
             <Operation>{operation}</Operation><Manifest>{manifest}</Manifest>\
             <Report>{report}</Report><Priority>10</Priority>\
             <RoleArn>arn:aws:iam::111122223333:role/batch</RoleArn></CreateJobRequest>"
        )
    }

    const MANIFEST: &str = "<Spec><Format>S3BatchOperations_CSV_20180820</Format>\
        <Fields><member>Bucket</member><member>Key</member><member>VersionId</member></Fields></Spec>\
        <Location><ObjectArn>arn:aws:s3:::src/lists/m.csv</ObjectArn><ETag>\"abc\"</ETag></Location>";
    const REPORT: &str = "<Enabled>true</Enabled><Bucket>arn:aws:s3:::reports</Bucket>\
        <Format>Report_CSV_20180820</Format><Prefix>out</Prefix><ReportScope>FailedTasksOnly</ReportScope>";

    fn parsed(body: &str) -> Result<Spec, S3Error> {
        spec(&xml::parse(body.as_bytes(), "CreateJobRequest")?)
    }

    #[test]
    fn create_job_requests_are_read_as_s3_control_states_them() {
        let spec = parsed(&request(
            "<S3PutObjectTagging><TagSet><S3Tag><Key>k</Key><Value>v</Value></S3Tag></TagSet></S3PutObjectTagging>",
            MANIFEST,
            REPORT,
        ))
        .unwrap();
        assert_eq!(
            spec.operation,
            Operation::PutTagging(vec![("k".into(), "v".into())])
        );
        assert_eq!(spec.manifest.bucket, "src");
        assert_eq!(spec.manifest.key, "lists/m.csv");
        assert_eq!(spec.manifest.etag, "abc", "quotes dropped");
        assert_eq!(
            spec.manifest.fields,
            [Field::Bucket, Field::Key, Field::VersionId]
        );
        assert_eq!(
            spec.report,
            Some(ReportSpec {
                bucket: "reports".into(),
                prefix: "out".into(),
                failed_only: true
            })
        );
        assert_eq!((spec.priority, spec.confirmation), (10, false));

        let copy = parsed(&request(
            "<S3PutObjectCopy><TargetResource>arn:aws:s3:::dst</TargetResource>\
             <TargetKeyPrefix>copies/</TargetKeyPrefix><StorageClass>STANDARD_IA</StorageClass>\
             <MetadataDirective>REPLACE</MetadataDirective><NewObjectMetadata>\
             <ContentType>text/plain</ContentType><UserMetadata><entry><key>A</key><value>1</value></entry>\
             </UserMetadata></NewObjectMetadata></S3PutObjectCopy>",
            MANIFEST,
            "<Enabled>false</Enabled>",
        ))
        .unwrap();
        let Operation::Copy(copy_spec) = copy.operation else {
            panic!("a copy");
        };
        assert_eq!(copy_spec.bucket, "dst");
        assert_eq!(copy_spec.class, StorageClass::StandardIa);
        let metadata = copy_spec.metadata.unwrap();
        assert_eq!(metadata.content_type.as_deref(), Some("text/plain"));
        assert_eq!(metadata.user, [("a".to_string(), "1".to_string())]);
        assert_eq!(copy.report, None);
    }

    #[test]
    fn create_job_requests_are_refused_as_s3_control_refuses_them() {
        let tagging = "<S3DeleteObjectTagging/>";
        let code = |body: String| parsed(&body).unwrap_err().code;
        // No such operation as a delete; two operations; an unknown element.
        assert_eq!(
            code(request("<S3DeleteObject/>", MANIFEST, REPORT)),
            "MalformedXML"
        );
        assert_eq!(
            code(request(&format!("{tagging}{tagging}"), MANIFEST, REPORT)),
            "InvalidRequest"
        );
        assert_eq!(
            code(request("<LambdaInvoke/>", MANIFEST, REPORT)),
            "NotImplemented"
        );
        let no_fields = MANIFEST.replace("<member>Bucket</member>", "");
        assert_eq!(code(request(tagging, &no_fields, REPORT)), "InvalidRequest");
        let no_etag = MANIFEST.replace("<ETag>\"abc\"</ETag>", "");
        assert_eq!(code(request(tagging, &no_etag, REPORT)), "InvalidRequest");
        let no_bucket = REPORT.replace("<Bucket>arn:aws:s3:::reports</Bucket>", "");
        assert_eq!(
            code(request(tagging, MANIFEST, &no_bucket)),
            "InvalidRequest"
        );
        let body = request(tagging, MANIFEST, REPORT);
        assert_eq!(
            code(body.replace("<Priority>10</Priority>", "<Priority>-1</Priority>")),
            "InvalidRequest"
        );
        assert_eq!(
            code(body.replace(
                "arn:aws:iam::111122223333:role/batch",
                "arn:aws:iam::1:role/x"
            )),
            "InvalidRequest"
        );
        let too_many: String = (0..11)
            .map(|n| format!("<S3Tag><Key>k{n}</Key><Value>v</Value></S3Tag>"))
            .collect();
        assert_eq!(
            code(request(
                &format!("<S3PutObjectTagging><TagSet>{too_many}</TagSet></S3PutObjectTagging>"),
                MANIFEST,
                REPORT
            )),
            "BadRequest"
        );
    }

    #[test]
    fn a_rendered_request_reads_back_the_same() {
        let body = request(
            "<S3PutObjectTagging><TagSet><S3Tag><Key>a&amp;b</Key><Value>&lt;v&gt;</Value></S3Tag></TagSet></S3PutObjectTagging>",
            MANIFEST,
            REPORT,
        );
        let element = xml::parse(body.as_bytes(), "CreateJobRequest").unwrap();
        let mut rendered = String::new();
        render(&element, &mut rendered);
        let again = xml::parse(rendered.as_bytes(), "CreateJobRequest").unwrap();
        assert_eq!(again, element);
        assert_eq!(
            spec(&again).unwrap().operation,
            Operation::PutTagging(vec![("a&b".into(), "<v>".into())])
        );
    }
}
