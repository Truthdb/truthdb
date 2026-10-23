//! The Batch Operations runner: a task beside the S3 listener that moves
//! each job through its statuses (New, Preparing, Suspended or Ready,
//! Active, then Completing, Cancelling or Failing, and their ends) and runs
//! its tasks. Preparing pins the manifest's version (it must still have the
//! ETag the job named) and counts its lines; an Active job runs a few dozen
//! tasks a step, the job with the highest priority first. Each task's
//! change, its result and the job's place in the manifest are one
//! transaction, so a job resumed after a crash runs each task once. A job
//! whose tasks fail more than half the time, once 1,000 have run, fails. At
//! its end a job writes its completion report (when it asked for one and a
//! task ran), then drops its results; it is forgotten 90 days after.
//!
//! Tasks act for the user that created the job (its account's root, or one
//! of its users): each is authorized as that user's own request would be,
//! on the account's own buckets only. A job whose step fails (a storage
//! error) waits before it is tried again, so it holds up no other job.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use truthdb_core::storage::StorageError;
use truthdb_object::{
    BodyWriter, Job, JobResult, JobStatus, ListAfter, ObjFile, ObjectAttrs, ObjectInfo, ObjectLock,
    Origin, SealedBody, StorageClass, VersionSel, time::iso8601,
};

use crate::authz::{Authz, Resource};
use crate::batch::{
    Field, GeneratorSpec, LambdaSpec, ManifestSpec, Operation, OutputSpec, Spec, spec,
    stored_request,
};
use crate::error::S3Error;
use crate::objects::{DROP_GRACE_MS, now_ms, object_error};
use crate::server::{Principal, S3State};
use truthdb_s3client::Poster;

/// How long the runner waits when no job has work.
const IDLE: Duration = Duration::from_secs(1);
/// Manifest bytes Preparing counts a step.
const PREPARE_BYTES: u64 = 1 << 20;
/// Manifest bytes, and tasks, an Active job takes a step.
const TASK_BYTES: u64 = 64 * 1024;
const TASKS_PER_STEP: usize = 64;
/// The largest object a batch copy copies (S3's CopyObject limit).
const MAX_COPY_BYTES: u64 = 5 << 30;
/// Results read, or dropped, per lock hold.
const RESULT_PAGE: usize = 512;
/// How long an ended job is kept.
const KEEP_ENDED_MS: u64 = 90 * 24 * 3600 * 1000;
/// Tasks run before the failure rate counts, and the rate past which a job
/// fails.
const THRESHOLD_TASKS: u64 = 1000;
/// Longest key, bucket and message a result keeps (a result row must fit
/// one tree cell).
const MAX_RESULT_KEY: usize = 1024;
const MAX_RESULT_TEXT: usize = 256;
/// What [`advance`] fails with when the job moved meanwhile (cancelled,
/// say): the transaction rolls back, and the step ends.
const MOVED: &str = "the batch job moved";
/// How keys are written in reports and generated manifests: URL-encoded,
/// '/' kept.
const REPORT_KEY: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');
/// Objects a generated manifest's listing takes a step.
const LIST_PAGE: usize = 1000;
const REPORT_SCHEMA: &str =
    "Bucket, Key, VersionId, TaskStatus, ErrorCode, HTTPStatusCode, ResultMessage";

/// The longest a failing job waits before it is tried again.
const MAX_WAIT: Duration = Duration::from_secs(300);

/// Jobs whose last step failed: when each is tried again, and its wait
/// (doubling from a second).
#[derive(Default)]
struct Waits(HashMap<u64, (Instant, Duration)>);

impl Waits {
    fn ready(&self, number: u64, now: Instant) -> bool {
        self.0.get(&number).is_none_or(|(at, _)| *at <= now)
    }

    fn failed(&mut self, number: u64, now: Instant) {
        let wait = self
            .0
            .get(&number)
            .map_or(IDLE, |(_, wait)| (*wait * 2).min(MAX_WAIT));
        self.0.insert(number, (now + wait, wait));
    }
}

/// Runs jobs until `shutdown` turns true. A standby runs none (its log is
/// the primary's).
pub async fn run(state: Arc<S3State>, mut shutdown: watch::Receiver<bool>) {
    let mut waits = Waits::default();
    // LambdaInvoke's POSTs (none, should the client not build: those jobs
    // fail).
    let poster = Poster::new(None)
        .inspect_err(|err| eprintln!("batch operations: no HTTP client: {err}"))
        .ok()
        .map(Arc::new);
    let invoking = Invoking::default();
    loop {
        if *shutdown.borrow() {
            return;
        }
        let more = !state.storage.is_standby()
            && step(&state, &mut waits, poster.as_ref(), &invoking).await;
        if !more {
            tokio::select! {
                _ = tokio::time::sleep(IDLE) => {}
                _ = shutdown.changed() => {}
            }
        }
    }
}

/// The LambdaInvoke jobs whose invocations are under way, each in a task of
/// its own (so a slow endpoint holds up only its own job): the runner does
/// nothing else with them meanwhile.
#[derive(Default, Clone)]
struct Invoking(Arc<std::sync::Mutex<std::collections::HashSet<u64>>>);

/// Jobs invoking at once, at most.
const MAX_INVOKING: usize = 32;

impl Invoking {
    fn has(&self, number: u64) -> bool {
        self.0.lock().expect("invoking poisoned").contains(&number)
    }

    /// Takes a place for job `number`, unless every place is taken.
    fn take(&self, number: u64) -> bool {
        let mut set = self.0.lock().expect("invoking poisoned");
        set.len() < MAX_INVOKING && set.insert(number)
    }

    fn release(&self, number: u64) {
        self.0.lock().expect("invoking poisoned").remove(&number);
    }
}

/// Runs `op` on the object engine (for the runner: no request scope).
async fn object<T, F>(state: &S3State, op: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(&mut ObjFile<'_>) -> Result<T, StorageError> + Send + 'static,
{
    state
        .engine
        .run_object(op)
        .await
        .map_err(|err| err.to_string())
}

/// What a step does to a job.
enum Work {
    DropResults,
    Forget,
    Begin,
    Prepare,
    Finish,
    Activate,
    Run,
}

/// The next work of a job not waiting: ended jobs' cleanup first, then New,
/// Preparing, ending, Ready jobs, then the Active job of the highest
/// priority (the oldest of those).
fn next_work<'j>(
    jobs: &'j [Job],
    waits: &Waits,
    invoking: &Invoking,
    now_ms: u64,
) -> Option<(&'j Job, Work)> {
    let now = Instant::now();
    let ready: Vec<&Job> = jobs
        .iter()
        .filter(|job| waits.ready(job.number, now) && !invoking.has(job.number))
        .collect();
    for job in ready.iter().filter(|job| job.status.is_terminal()) {
        if job.results_left {
            return Some((job, Work::DropResults));
        }
        if job
            .terminated_ms
            .is_some_and(|at| at + KEEP_ENDED_MS < now_ms)
        {
            return Some((job, Work::Forget));
        }
    }
    let first = |status: JobStatus| ready.iter().copied().find(|job| job.status == status);
    if let Some(job) = first(JobStatus::New) {
        return Some((job, Work::Begin));
    }
    if let Some(job) = first(JobStatus::Preparing) {
        return Some((job, Work::Prepare));
    }
    if let Some(job) = ready.iter().copied().find(|job| {
        matches!(
            job.status,
            JobStatus::Completing | JobStatus::Cancelling | JobStatus::Failing
        )
    }) {
        return Some((job, Work::Finish));
    }
    if let Some(job) = first(JobStatus::Ready) {
        return Some((job, Work::Activate));
    }
    ready
        .iter()
        .copied()
        .filter(|job| job.status == JobStatus::Active)
        .max_by_key(|job| (job.priority, std::cmp::Reverse(job.number)))
        .map(|job| (job, Work::Run))
}

/// One step of whichever job has work; whether there may be more now.
async fn step(
    state: &Arc<S3State>,
    waits: &mut Waits,
    poster: Option<&Arc<Poster>>,
    invoking: &Invoking,
) -> bool {
    let jobs = match object(state, |file| file.jobs()).await {
        Ok(jobs) => jobs,
        Err(err) => {
            eprintln!("batch operations: {err}");
            return false;
        }
    };
    waits
        .0
        .retain(|number, _| jobs.iter().any(|job| job.number == *number));
    let Some((job, work)) = next_work(&jobs, waits, invoking, now_ms()) else {
        return false;
    };
    let number = job.number;
    let outcome = match work {
        Work::DropResults => {
            object(state, move |file| {
                file.transaction(|tx| {
                    // Its results, then a generated manifest's list.
                    if tx.drop_job_results(number, RESULT_PAGE)?
                        && tx.drop_job_tasks(number, RESULT_PAGE)?
                        && let Some(mut job) = tx.job(number)?
                    {
                        job.results_left = false;
                        tx.put_job(&job)?;
                    }
                    Ok(())
                })
            })
            .await
        }
        Work::Forget => {
            let job = job.clone();
            object(state, move |file| file.transaction(|tx| tx.drop_job(&job))).await
        }
        Work::Begin => {
            move_job(state, number, JobStatus::New, |job, _| {
                job.status = JobStatus::Preparing;
            })
            .await
        }
        Work::Prepare => prepare(state, number).await,
        Work::Finish => finish(state, job.clone()).await,
        Work::Activate => {
            move_job(state, number, JobStatus::Ready, |job, now| {
                job.status = JobStatus::Active;
                job.active_since_ms.get_or_insert(now);
            })
            .await
        }
        Work::Run => run_tasks(state, number, poster, invoking).await,
    };
    match outcome {
        Ok(()) => {
            waits.0.remove(&number);
        }
        Err(err) => {
            eprintln!("batch operations: job {}: {err}", job.id());
            waits.failed(number, Instant::now());
        }
    }
    true
}

/// The job's request, read; `None` when it is missing or unreadable.
fn job_spec(file: &mut ObjFile<'_>, job: &Job) -> Result<Option<Spec>, StorageError> {
    let Some(request) = file.job_request(job.number)? else {
        return Ok(None);
    };
    Ok(stored_request(job, &request)
        .and_then(|request| spec(&request))
        .ok())
}

/// Whether the user that created `job` still exists (the same user: a user
/// of its name made since is another). A job whose creator is gone fails.
fn creator_holds(file: &mut ObjFile<'_>, job: &Job) -> Result<bool, StorageError> {
    let Some(name) = &job.user else {
        return Ok(true);
    };
    Ok(file
        .users(&job.account)?
        .iter()
        .any(|user| &user.name == name && user.user_id == job.user_id))
}

const CREATOR_GONE: &str = "The user that created the job no longer exists";

/// The canonical version id of a version selector (`None`: the latest).
fn version_key(which: VersionSel) -> Option<String> {
    match which {
        VersionSel::Latest => None,
        VersionSel::Null => Some("null".to_string()),
        VersionSel::Seq(version) => Some(version.to_string()),
    }
}

/// The user that created `job`, as a principal (for its authorization).
fn creator(job: &Job) -> Principal {
    Principal {
        access_key: String::new(),
        account: job.account.clone(),
        user: job.user.clone(),
    }
}

/// Changes job `number` by `change` if it is still in `from`.
async fn move_job(
    state: &S3State,
    number: u64,
    from: JobStatus,
    change: impl FnOnce(&mut Job, u64) + Send + 'static,
) -> Result<(), String> {
    let now = now_ms();
    object(state, move |file| {
        file.transaction(|tx| {
            if let Some(mut job) = tx.job(number)?
                && job.status == from
            {
                change(&mut job, now);
                tx.put_job(&job)?;
            }
            Ok(())
        })
    })
    .await
}

/// Ends job `job` as failed (a Failing job reports first).
fn fail(job: &mut Job, code: &str, reason: &str) {
    job.failures.push((code.to_string(), reason.to_string()));
    job.status = JobStatus::Failing;
}

/// What a step learnt of the manifest: its bytes from an offset and its
/// size, or why it cannot be read.
type ManifestRead = Result<(Vec<u8>, u64), (&'static str, String)>;

/// Up to `max` bytes of the manifest version `version` from `offset`.
fn read_manifest(
    file: &mut ObjFile<'_>,
    manifest: &ManifestSpec,
    version: u64,
    offset: u64,
    max: u64,
) -> Result<ManifestRead, StorageError> {
    let info = match file.head_object(&manifest.bucket, &manifest.key, VersionSel::Seq(version))? {
        Ok(info) => info,
        Err(_) => {
            return Ok(Err((
                "ManifestNotFound",
                "The manifest's version no longer exists".to_string(),
            )));
        }
    };
    let len = max.min(info.size.saturating_sub(offset));
    let mut out = vec![0u8; len as usize];
    let mut done = 0u64;
    for (part, start, part_len) in file.body_layout(info.body)? {
        let (from, to) = (offset + done, offset + len);
        if done == len {
            break;
        }
        if from >= start + part_len || from < start {
            continue;
        }
        let take = (start + part_len).min(to) - from;
        file.read_body(
            part,
            from - start,
            &mut out[done as usize..(done + take) as usize],
        )?;
        done += take;
    }
    Ok(Ok((out, info.size)))
}

/// A generated manifest listed in full, to be written out: the job, where
/// to, the bucket it lists, and whether the job waits for confirmation
/// after.
type ToWrite = Option<(Job, OutputSpec, String, bool)>;

/// Preparing: checks the manifest (or the bucket a manifest is generated
/// from) and the report's bucket, then a step of the manifest's work: a
/// CSV manifest's version pinned and its lines counted, or a page of a
/// generated one listed. A generated manifest asked to be written out is
/// written, outside the lock, once it is listed.
async fn prepare(state: &S3State, number: u64) -> Result<(), String> {
    let now = now_ms();
    let to_write = object(state, move |file| prepare_step(file, number, now)).await?;
    if let Some((job, output, source, confirmation)) = to_write {
        write_generated(state, job, output, source, confirmation).await?;
    }
    Ok(())
}

fn prepare_step(file: &mut ObjFile<'_>, number: u64, now: u64) -> Result<ToWrite, StorageError> {
    let Some(job) = file.job(number)? else {
        return Ok(None);
    };
    let Some(spec) = job_spec(file, &job)? else {
        return finish_failed(
            file,
            number,
            "InvalidJob",
            "The job's request cannot be read",
        )
        .map(|()| None);
    };
    if !creator_holds(file, &job)? {
        return finish_failed(file, number, "AccessDenied", CREATOR_GONE).map(|()| None);
    }
    if let Some(report) = &spec.report
        && !owned_by(file, &report.bucket, &job.account, None)?
    {
        return finish_failed(
            file,
            number,
            "ReportBucketNotOwned",
            "The report's bucket does not exist or is not the job's account's",
        )
        .map(|()| None);
    }
    match (&spec.manifest, &spec.generator) {
        (Some(manifest), _) => prepare_csv(file, &job, &spec, manifest, now).map(|()| None),
        (None, Some(generator)) => prepare_listing(file, &job, &spec, generator, now),
        (None, None) => {
            finish_failed(file, number, "InvalidJob", "The job names no manifest").map(|()| None)
        }
    }
}

/// Whether `bucket` exists and is `account`'s (and, when given, the
/// owner it is expected to have).
fn owned_by(
    file: &mut ObjFile<'_>,
    bucket: &str,
    account: &str,
    expected: Option<&str>,
) -> Result<bool, StorageError> {
    Ok(file.bucket(bucket)?.is_some_and(|info| {
        info.owner == account && expected.is_none_or(|expected| expected == info.owner)
    }))
}

/// A Preparing job's next status: Suspended when it waits for
/// confirmation, else Ready.
fn prepared(job: &mut Job, confirmation: bool, now: u64) {
    if confirmation {
        job.status = JobStatus::Suspended;
        job.suspended_ms = Some(now);
    } else {
        job.status = JobStatus::Ready;
    }
}

fn prepare_csv(
    file: &mut ObjFile<'_>,
    job: &Job,
    spec: &Spec,
    manifest: &ManifestSpec,
    now: u64,
) -> Result<(), StorageError> {
    let number = job.number;
    if !owned_by(file, &manifest.bucket, &job.account, None)? {
        return finish_failed(
            file,
            number,
            "ManifestBucketNotOwned",
            "The manifest's bucket does not exist or is not the job's account's",
        );
    }
    let version = match job.manifest_version {
        Some(version) => version,
        None => {
            let which = match manifest.version_id.as_deref() {
                None => VersionSel::Latest,
                Some(id) => match version_sel(id) {
                    Some(which) => which,
                    None => {
                        return finish_failed(
                            file,
                            number,
                            "ManifestNotFound",
                            "The manifest's version id is not valid",
                        );
                    }
                },
            };
            let action = match manifest.version_id {
                Some(_) => "s3:GetObjectVersion",
                None => "s3:GetObject",
            };
            let authz = Authz::load_now(
                file,
                &creator(job),
                vec![manifest.bucket.clone()],
                vec![(manifest.bucket.clone(), manifest.key.clone(), which)],
                now,
            )?;
            let authz = match version_key(which) {
                Some(version) => authz.with_keys(&[("s3:VersionId", version)]),
                None => authz,
            };
            if authz
                .check(action, Resource::Object(&manifest.bucket, &manifest.key))
                .is_err()
            {
                return finish_failed(
                    file,
                    number,
                    "AccessDenied",
                    "The job's creator may not read the manifest",
                );
            }
            let info = match file.head_object(&manifest.bucket, &manifest.key, which)? {
                Ok(info) => info,
                Err(_) => {
                    return finish_failed(
                        file,
                        number,
                        "ManifestNotFound",
                        "The manifest object does not exist",
                    );
                }
            };
            if info.etag().trim_matches('"') != manifest.etag {
                return finish_failed(
                    file,
                    number,
                    "ManifestETagMismatch",
                    "The manifest's ETag is not the one the job names",
                );
            }
            if info.attrs.customer_key.is_some()
                || file.archived_unreadable(info.version, info.storage_class, now)?
            {
                return finish_failed(
                    file,
                    number,
                    "ManifestNotReadable",
                    "The manifest is encrypted with a customer-provided key or archived",
                );
            }
            info.version
        }
    };
    let (bytes, size) = match read_manifest(file, manifest, version, job.cursor, PREPARE_BYTES)? {
        Ok(read) => read,
        Err((code, reason)) => return finish_failed(file, number, code, &reason),
    };
    let at_end = job.cursor + bytes.len() as u64 >= size;
    // Whole lines only, unless the manifest ends here.
    let taken = if at_end {
        bytes.len()
    } else {
        match bytes.iter().rposition(|&byte| byte == b'\n') {
            Some(last) => last + 1,
            None => {
                return finish_failed(
                    file,
                    number,
                    "ManifestLineTooLong",
                    "A manifest line is longer than 1 MiB",
                );
            }
        }
    };
    let lines = bytes[..taken]
        .split(|&byte| byte == b'\n')
        .filter(|line| !blank(line))
        .count() as u64;
    let (cursor, confirmation) = (job.cursor, spec.confirmation);
    file.transaction(|tx| {
        let Some(mut job) = tx.job(number)? else {
            return Ok(());
        };
        if job.status != JobStatus::Preparing || job.cursor != cursor {
            return Ok(());
        }
        job.manifest_version = Some(version);
        job.total += lines;
        job.cursor += taken as u64;
        if at_end {
            job.cursor = 0;
            prepared(&mut job, confirmation, now);
        }
        tx.put_job(&job)
    })
}

/// A page of a generated manifest's listing: the source bucket's current
/// versions passing the filter become its tasks. Generating one is
/// authorized as AWS asks for it (s3:PutInventoryConfiguration on the
/// bucket).
fn prepare_listing(
    file: &mut ObjFile<'_>,
    job: &Job,
    spec: &Spec,
    generator: &GeneratorSpec,
    now: u64,
) -> Result<ToWrite, StorageError> {
    let number = job.number;
    let bucket = &generator.bucket;
    if !owned_by(
        file,
        bucket,
        &job.account,
        generator.expected_owner.as_deref(),
    )? {
        return finish_failed(
            file,
            number,
            "ManifestBucketNotOwned",
            "The bucket to list does not exist or is not the job's account's (or the owner expected)",
        )
        .map(|()| None);
    }
    if let Some(output) = &generator.output
        && !owned_by(
            file,
            &output.bucket,
            &job.account,
            output.expected_owner.as_deref(),
        )?
    {
        return finish_failed(
            file,
            number,
            "ManifestBucketNotOwned",
            "The generated manifest's bucket does not exist or is not the job's account's",
        )
        .map(|()| None);
    }
    let authz = Authz::load_now(file, &creator(job), vec![bucket.clone()], Vec::new(), now)?;
    if authz
        .check("s3:PutInventoryConfiguration", Resource::Bucket(bucket))
        .is_err()
    {
        return finish_failed(
            file,
            number,
            "AccessDenied",
            "The job's creator may not list the bucket for a manifest",
        )
        .map(|()| None);
    }
    if job.listing_done {
        return Ok(match (&generator.output, &job.generated) {
            (Some(output), None) => Some((
                job.clone(),
                output.clone(),
                bucket.clone(),
                spec.confirmation,
            )),
            _ => {
                let confirmation = spec.confirmation;
                file.transaction(|tx| {
                    if let Some(mut job) = tx.job(number)?
                        && job.status == JobStatus::Preparing
                    {
                        prepared(&mut job, confirmation, now);
                        tx.put_job(&job)?;
                    }
                    Ok(())
                })?;
                None
            }
        });
    }
    let after = job.listed_after.clone();
    // Only the keys under a prefix filter's prefix are listed.
    let listing = match file.list_objects(
        bucket,
        generator.filter.prefix.as_deref().unwrap_or(""),
        None,
        after.as_deref().map(ListAfter::Key),
        LIST_PAGE,
    )? {
        Ok(listing) => listing,
        Err(_) => {
            return finish_failed(
                file,
                number,
                "ManifestNotFound",
                "The bucket cannot be listed",
            )
            .map(|()| None);
        }
    };
    let versioning = crate::objects::versioning_of(file, bucket)?;
    let tasks: Vec<(String, String)> = listing
        .objects
        .iter()
        .filter(|object| object.latest && !object.info.delete_marker)
        .filter(|object| {
            generator.filter.passes(
                &object.key,
                object.info.size,
                object.info.mtime_ms,
                object.info.storage_class,
            )
        })
        .map(|object| {
            let version = crate::objects::version_header(
                versioning,
                object.info.version,
                object.info.null_version,
            );
            (object.key.clone(), version.unwrap_or_default())
        })
        .collect();
    let last = listing.last.as_ref().map(|(key, _)| key.clone());
    let done = !listing.truncated;
    file.transaction(|tx| {
        let Some(mut job) = tx.job(number)? else {
            return Ok(());
        };
        if job.status != JobStatus::Preparing || job.listed_after != after || job.listing_done {
            return Ok(());
        }
        for (key, version_id) in &tasks {
            tx.add_job_task(&mut job, key, version_id)?;
        }
        if last.is_some() {
            job.listed_after = last;
        }
        job.listing_done = done;
        tx.put_job(&job)
    })?;
    Ok(None)
}

/// Writes a listed generated manifest (`bucket,key,versionId` lines, keys
/// URL-encoded) to `<prefix>/job-<id>/manifest.csv` of its output bucket,
/// authorized as the creator's PutObject, then the job moves on.
async fn write_generated(
    state: &S3State,
    job: Job,
    output: OutputSpec,
    source: String,
    confirmation: bool,
) -> Result<(), String> {
    let number = job.number;
    let storage = Arc::clone(&state.storage);
    let sealed = tokio::task::spawn_blocking({
        let job = job.clone();
        move || write_task_list(&storage, &job, &source)
    })
    .await
    .map_err(|err| err.to_string())?
    .map_err(|err| err.to_string())?;
    let key = match output.prefix.trim_end_matches('/') {
        "" => format!("job-{}/manifest.csv", job.id()),
        prefix => format!("{prefix}/job-{}/manifest.csv", job.id()),
    };
    let now = now_ms();
    object(state, move |file| {
        let Some(job) = file.job(number)? else {
            return Ok(());
        };
        if job.status != JobStatus::Preparing || job.generated.is_some() {
            return Ok(());
        }
        if !owned_by(
            file,
            &output.bucket,
            &job.account,
            output.expected_owner.as_deref(),
        )? {
            return finish_failed(
                file,
                number,
                "ManifestBucketNotOwned",
                "The generated manifest's bucket does not exist or is not the job's account's",
            );
        }
        let (bucket_lock, default) = crate::object_lock::bucket_state(file, &output.bucket)?;
        let lock = crate::object_lock::new_object_lock(None, bucket_lock, default, now)
            .ok()
            .flatten();
        let authz = Authz::load_now(
            file,
            &creator(&job),
            vec![output.bucket.clone()],
            Vec::new(),
            now,
        )?;
        if authz
            .check_storing(
                "s3:PutObject",
                Resource::Object(&output.bucket, &key),
                None,
                None,
            )
            .is_err()
        {
            return finish_failed(
                file,
                number,
                "AccessDenied",
                "The job's creator may not write the generated manifest",
            );
        }
        file.set_origin(Origin {
            operation: "PutObject".to_string(),
            request_id: job.id(),
            account: Some(job.account.clone()),
            ..Origin::default()
        });
        let versioning = crate::objects::versioning_of(file, &output.bucket)?;
        let attrs = ObjectAttrs {
            content_type: Some("text/csv".to_string()),
            ..ObjectAttrs::default()
        };
        let written = file.transaction(|tx| {
            let Some(mut job) = tx.job(number)? else {
                return Ok(false);
            };
            match tx.put_object(
                &output.bucket,
                &key,
                &sealed,
                &attrs,
                StorageClass::Standard,
                now,
                DROP_GRACE_MS,
            )? {
                Ok(info) => {
                    if let Some(lock) = &lock {
                        tx.set_lock(info.version, lock)?;
                    }
                    let version =
                        crate::objects::version_header(versioning, info.version, info.null_version);
                    job.generated = Some((
                        output.bucket.clone(),
                        key.clone(),
                        version.unwrap_or_default(),
                        info.etag(),
                    ));
                    prepared(&mut job, confirmation, now);
                    tx.put_job(&job)?;
                    Ok(true)
                }
                Err(_) => Ok(false),
            }
        })?;
        if !written {
            return finish_failed(
                file,
                number,
                "ManifestNotWritten",
                "The generated manifest could not be written",
            );
        }
        Ok(())
    })
    .await
}

/// A generated manifest's lines as a body.
fn write_task_list(
    storage: &truthdb_core::storage::Storage,
    job: &Job,
    bucket: &str,
) -> Result<SealedBody, StorageError> {
    let now = now_ms();
    let mut writer = BodyWriter::begin(storage, now)?;
    let mut from = 0;
    loop {
        let page = ObjFile::new(&mut storage.lock()).job_tasks(job.number, from, RESULT_PAGE)?;
        let Some(&(last, _, _)) = page.last() else {
            break;
        };
        from = last + 1;
        let mut text = String::new();
        for (_, key, version_id) in &page {
            let key = percent_encoding::utf8_percent_encode(key, REPORT_KEY).to_string();
            text.push_str(&format!("{bucket},{key},{version_id}\n"));
        }
        writer.write(storage, text.as_bytes(), now)?;
    }
    writer.finish(storage, now)
}

/// Fails job `number` in its own transaction (it reports as any ending job).
fn finish_failed(
    file: &mut ObjFile<'_>,
    number: u64,
    code: &str,
    reason: &str,
) -> Result<(), StorageError> {
    file.transaction(|tx| {
        if let Some(mut job) = tx.job(number)?
            && !job.status.is_terminal()
        {
            fail(&mut job, code, reason);
            tx.put_job(&job)?;
        }
        Ok(())
    })
}

/// A manifest's version id as a selector (`null`, or the number S3 gave).
fn version_sel(id: &str) -> Option<VersionSel> {
    match id {
        "null" => Some(VersionSel::Null),
        digits => digits.parse().ok().map(VersionSel::Seq),
    }
}

/// One manifest line's object, or why the line is not one.
fn parse_line(line: &str, fields: &[Field]) -> Result<(String, String, Option<String>), String> {
    let values: Vec<&str> = line.split(',').collect();
    let (mut bucket, mut key, mut version) = (None, None, None);
    for (index, field) in fields.iter().enumerate() {
        let value = values.get(index).copied().unwrap_or("");
        match field {
            Field::Bucket => bucket = Some(value),
            Field::Key => key = Some(value),
            Field::VersionId => version = Some(value),
            Field::Ignore => {}
        }
    }
    let bucket = bucket
        .filter(|bucket| !bucket.is_empty())
        .ok_or("The line names no bucket")?;
    let key = key
        .filter(|key| !key.is_empty())
        .ok_or("The line names no key")?;
    let key = percent_encoding::percent_decode_str(key)
        .decode_utf8()
        .map_err(|_| "The line's key is not URL-encoded UTF-8")?
        .into_owned();
    Ok((
        bucket.to_string(),
        key,
        version
            .filter(|version| !version.is_empty())
            .map(str::to_string),
    ))
}

/// What a task will write, read in the lock hold it is written in.
enum Planned {
    Tags(VersionSel, Vec<(String, String)>),
    Restore(VersionSel, truthdb_object::RestoreRequest),
    Retention(VersionSel, truthdb_object::RetentionChange, bool),
    LegalHold(VersionSel, bool),
    Copy {
        source: Box<ObjectInfo>,
        bucket: String,
        key: String,
        attrs: Box<ObjectAttrs>,
        class: StorageClass,
        lock: Option<ObjectLock>,
    },
}

/// Reads what a task needs; the error it ends with otherwise.
#[allow(clippy::too_many_arguments)]
fn plan(
    file: &mut ObjFile<'_>,
    delays: crate::restore::RestoreDelays,
    creator: &Principal,
    operation: &Operation,
    bucket: &str,
    key: &str,
    version: Option<&str>,
    now: u64,
) -> Result<Result<Planned, S3Error>, StorageError> {
    let which = match version {
        None => VersionSel::Latest,
        Some(id) => match version_sel(id) {
            Some(which) => which,
            None => {
                return Ok(Err(S3Error::new(
                    400,
                    "InvalidArgument",
                    "Invalid version id specified",
                )));
            }
        },
    };
    let owned =
        |file: &mut ObjFile<'_>, bucket: &str| -> Result<Result<(), S3Error>, StorageError> {
            Ok(match file.bucket(bucket)? {
                None => Err(S3Error::new(
                    404,
                    "NoSuchBucket",
                    format!("The bucket {bucket} does not exist"),
                )),
                Some(info) if info.owner != creator.account => {
                    Err(S3Error::access_denied("Access Denied"))
                }
                Some(_) => Ok(()),
            })
        };
    if let Err(err) = owned(file, bucket)? {
        return Ok(Err(err));
    }
    // Authorized as the creator's own request, before anything of the
    // object is looked at (a denied task tells nothing of it).
    let mut buckets = vec![bucket.to_string()];
    if let Operation::Copy(copy) = operation {
        buckets.push(copy.bucket.clone());
    }
    let authz = Authz::load_now(
        file,
        creator,
        buckets,
        vec![(bucket.to_string(), key.to_string(), which)],
        now,
    )?;
    let object = Resource::Object(bucket, key);
    let versioned = version.is_some();
    // The keys the equivalent request would carry: the version it names.
    let authz = match version_key(which) {
        Some(version) => authz.with_keys(&[("s3:VersionId", version)]),
        None => authz,
    };
    let copy = match operation {
        Operation::PutTagging(tags) => {
            let action = if versioned {
                "s3:PutObjectVersionTagging"
            } else {
                "s3:PutObjectTagging"
            };
            if let Err(err) = authz.check_tags(action, object, tags) {
                return Ok(Err(err));
            }
            return Ok(Ok(Planned::Tags(which, tags.clone())));
        }
        Operation::DeleteTagging => {
            let action = if versioned {
                "s3:DeleteObjectVersionTagging"
            } else {
                "s3:DeleteObjectTagging"
            };
            if let Err(err) = authz.check(action, object) {
                return Ok(Err(err));
            }
            return Ok(Ok(Planned::Tags(which, Vec::new())));
        }
        Operation::Restore { days, bulk } => {
            if let Err(err) = authz.check("s3:RestoreObject", object) {
                return Ok(Err(err));
            }
            let seconds = if *bulk { delays.bulk } else { delays.standard };
            return Ok(Ok(Planned::Restore(
                which,
                truthdb_object::RestoreRequest {
                    days: *days,
                    delay_ms: seconds.saturating_mul(1000),
                    expedited: false,
                },
            )));
        }
        Operation::Retention { document, bypass } => {
            // As a PutObjectRetention: its keys, from the change it asks
            // for (and a release of a held version's hold), and the bypass
            // only when the creator may bypass.
            let change = match crate::object_lock::retention_change(document.as_bytes(), now) {
                Ok(change) => change,
                Err(err) => return Ok(Err(err)),
            };
            let mut context = authz.context().clone();
            crate::object_lock::retention_keys(&mut context, change, now);
            crate::object_lock::release_keys(file, bucket, key, which, change, &mut context)?;
            if let Err(err) = authz.check_with("s3:PutObjectRetention", object, &context) {
                return Ok(Err(err));
            }
            let bypass = *bypass && authz.check("s3:BypassGovernanceRetention", object).is_ok();
            return Ok(Ok(Planned::Retention(which, change, bypass)));
        }
        Operation::LegalHold(on) => {
            let mut context = authz.context().clone();
            crate::object_lock::set_condition_keys(
                &mut context,
                &ObjectLock {
                    retention: None,
                    legal_hold: Some(*on),
                },
                now,
            );
            if let Err(err) = authz.check_with("s3:PutObjectLegalHold", object, &context) {
                return Ok(Err(err));
            }
            return Ok(Ok(Planned::LegalHold(which, *on)));
        }
        // Invoked outside the lock, not planned here.
        Operation::Lambda(_) => {
            return Ok(Err(S3Error::internal("a LambdaInvoke task is not planned")));
        }
        Operation::Copy(copy) => copy,
    };
    // A CopyObject's keys: its source, the class and the metadata
    // directive, on both its checks (the source's read also keeps the
    // source's version id; the target's write has none).
    let mut source_id = format!("{bucket}/{key}");
    if let Some(version) = version_key(which) {
        source_id.push_str(&format!("?versionId={version}"));
    }
    let directive = match copy.metadata {
        Some(_) => "REPLACE",
        None => "COPY",
    };
    let mut copy_keys = vec![
        ("s3:x-amz-copy-source", source_id),
        ("s3:x-amz-storage-class", copy.class.as_str().to_string()),
        ("s3:x-amz-metadata-directive", directive.to_string()),
    ];
    if copy.sse {
        copy_keys.push(("s3:x-amz-server-side-encryption", "AES256".to_string()));
    }
    let read = if versioned {
        "s3:GetObjectVersion"
    } else {
        "s3:GetObject"
    };
    if let Err(err) = authz.with_keys(&copy_keys).check(read, object) {
        return Ok(Err(err));
    }
    let source = match file.head_object(bucket, key, which)? {
        Ok(source) => source,
        Err(err) => return Ok(Err(object_error(err, bucket, key))),
    };
    if source.attrs.customer_key.is_some() {
        return Ok(Err(S3Error::invalid_request(
            "An object encrypted with a customer-provided key is not copied by a batch job",
        )));
    }
    if file.archived_unreadable(source.version, source.storage_class, now)? {
        return Ok(Err(S3Error::new(
            403,
            "InvalidObjectState",
            "The operation is not valid for the object's storage class",
        )));
    }
    if source.size > MAX_COPY_BYTES {
        return Ok(Err(S3Error::invalid_request(
            "The object is larger than a batch copy copies (5 GiB)",
        )));
    }
    if let Err(err) = owned(file, &copy.bucket)? {
        return Ok(Err(err));
    }
    let target = format!("{}{key}", copy.prefix);
    if copy.metadata.is_none()
        && copy.tags.is_none()
        && copy.class == source.storage_class
        && (copy.bucket.as_str(), target.as_str()) == (bucket, key)
        // As CopyObject: an older version copied onto its key restores it.
        && file
            .head_object(bucket, key, VersionSel::Latest)?
            .is_ok_and(|current| current.version == source.version)
    {
        return Ok(Err(S3Error::invalid_request(
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
        )));
    }
    let mut attrs = copy
        .metadata
        .clone()
        .unwrap_or_else(|| source.attrs.clone());
    attrs.tags = copy
        .tags
        .clone()
        .unwrap_or_else(|| source.attrs.tags.clone());
    attrs.customer_key = None;
    let (bucket_lock, default) = crate::object_lock::bucket_state(file, &copy.bucket)?;
    let lock = crate::object_lock::new_object_lock(None, bucket_lock, default, now)
        .ok()
        .flatten();
    let target_authz = Authz::load_now(file, creator, vec![copy.bucket.clone()], Vec::new(), now)?
        .with_keys(&copy_keys);
    if let Err(err) = target_authz.check_storing(
        "s3:PutObject",
        Resource::Object(&copy.bucket, &target),
        copy.tags.as_deref(),
        // As a CopyObject asking for no lock: the bucket's default retention
        // applies to the write, not to its authorization.
        None,
    ) {
        return Ok(Err(err));
    }
    Ok(Ok(Planned::Copy {
        source: Box::new(source),
        bucket: copy.bucket.clone(),
        key: target,
        attrs: Box::new(attrs),
        class: copy.class,
        lock,
    }))
}

/// Whether a manifest line holds no task.
fn blank(line: &[u8]) -> bool {
    line.iter().all(u8::is_ascii_whitespace)
}

fn cut(text: &str, max: usize) -> String {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

/// Runs up to [`TASKS_PER_STEP`] tasks of Active job `number`: the lines
/// of its CSV manifest, or the objects its generated one lists. A
/// LambdaInvoke job's tasks invoke their function outside the storage lock
/// (see [`run_invocations`]).
async fn run_tasks(
    state: &Arc<S3State>,
    number: u64,
    poster: Option<&Arc<Poster>>,
    invoking: &Invoking,
) -> Result<(), String> {
    let now = now_ms();
    let delays = state.restore_delays;
    let lambda = object(state, move |file| {
        let Some(job) = file.job(number)? else {
            return Ok(None);
        };
        if job.status != JobStatus::Active {
            return Ok(None);
        }
        let Some(spec) = job_spec(file, &job)? else {
            finish_failed(
                file,
                number,
                "InvalidJob",
                "The job's request cannot be read",
            )?;
            return Ok(None);
        };
        if !creator_holds(file, &job)? {
            finish_failed(file, number, "AccessDenied", CREATOR_GONE)?;
            return Ok(None);
        }
        if let Operation::Lambda(lambda) = &spec.operation {
            return Ok(Some((job, (**lambda).clone(), spec.report.clone())));
        }
        file.set_origin(Origin {
            operation: match spec.operation {
                Operation::Copy(_) => "CopyObject",
                Operation::PutTagging(_) => "PutObjectTagging",
                Operation::DeleteTagging => "DeleteObjectTagging",
                Operation::Restore { .. } => "RestoreObject",
                Operation::Retention { .. } => "PutObjectRetention",
                Operation::LegalHold(_) => "PutObjectLegalHold",
                Operation::Lambda(_) => "",
            }
            .to_string(),
            request_id: job.id(),
            account: Some(job.account.clone()),
            ..Origin::default()
        });
        let task = Task {
            number,
            now,
            delays,
            who: creator(&job),
            spec: &spec,
        };
        match next_items(file, &job, &spec, TASKS_PER_STEP)? {
            Next::Complete(cursor) => complete(file, number, cursor)?,
            Next::Failed(code, reason) => finish_failed(file, number, code, &reason)?,
            Next::Items(items) => {
                for item in items {
                    let moved_on = match item.object {
                        None => moved(
                            file.transaction(|tx| advance(tx, number, item.start, item.end, None)),
                        )?,
                        Some(object) => run_one(file, &task, item.start, item.end, object)?,
                    };
                    if moved_on {
                        break;
                    }
                }
            }
        }
        Ok(None)
    })
    .await?;
    // Invoked in a task of its own (when a place is free; else the job
    // waits for the next step).
    if let Some((job, lambda, report)) = lambda
        && invoking.take(number)
    {
        let (state, poster, invoking) = (Arc::clone(state), poster.cloned(), invoking.clone());
        tokio::spawn(async move {
            if let Err(err) = run_invocations(&state, poster.as_deref(), job, lambda, report).await
            {
                eprintln!("batch operations: job {number}: {err}");
            }
            invoking.release(number);
        });
    }
    Ok(())
}

/// LambdaInvoke tasks a step invokes at most, and the times one is tried
/// (a TemporaryFailure, or no answer, is tried again after 1 s, then 2 s).
const INVOCATIONS_PER_STEP: usize = 16;
/// How long a step of invocations starts new ones: none starts after it
/// (one under way runs to its end, so none is cut short and invoked
/// again); the step's results are then recorded, and a cancel or a change
/// of priority seen.
const INVOCATION_STEP_BUDGET: Duration = Duration::from_secs(60);
const INVOCATION_TRIES: u32 = 3;
/// The largest answer a function may give.
const MAX_INVOCATION_ANSWER: usize = 1 << 20;

/// A step of a LambdaInvoke job: its next tasks taken (and checked: the
/// creator may invoke the function, the objects are the account's) in one
/// lock hold, each invoked outside it, then their results recorded in
/// order, each with the job's place. A crash between an invocation and its
/// record invokes it again: tasks are invoked at least once, as AWS
/// invokes them.
async fn run_invocations(
    state: &S3State,
    poster: Option<&Poster>,
    job: Job,
    lambda: LambdaSpec,
    report: Option<crate::batch::ReportSpec>,
) -> Result<(), String> {
    let number = job.number;
    let Some(poster) = poster else {
        return object(state, move |file| {
            finish_failed(
                file,
                number,
                "InternalError",
                "No HTTP client to invoke with",
            )
        })
        .await;
    };
    // The function: a topic of the job's account with a webhook.
    let account = job.account.clone();
    let function = lambda.function.clone();
    let webhook = state
        .engine
        .run_engine(move |engine| {
            engine.streams(|file| {
                let owned = file
                    .topic(&function)?
                    .is_some_and(|(topic, _)| topic.account.as_deref() == Some(account.as_str()));
                Ok(file
                    .webhooks()?
                    .into_iter()
                    .find(|webhook| owned && webhook.topic == function)
                    .map(|webhook| (webhook.url, webhook.secret)))
            })
        })
        .await
        .map_err(|err| err.to_string())?;
    let found = webhook.filter(|_| {
        lambda
            .account
            .as_ref()
            .is_none_or(|named| *named == job.account)
    });
    let Some((url, secret)) = found else {
        return object(state, move |file| {
            finish_failed(
                file,
                number,
                "LambdaFunctionNotFound",
                "The function names no topic of the job's account with a webhook",
            )
        })
        .await;
    };
    let now = now_ms();
    // Authorized on the function invoked, named canonically (the way the
    // ARN was written does not matter): this server's region, the job's
    // account.
    let mut arn = format!(
        "arn:aws:lambda:{}:{}:function:{}",
        state.region, job.account, lambda.function
    );
    if let Some(qualifier) = &lambda.qualifier {
        arn.push(':');
        arn.push_str(qualifier);
    }
    let taken = object(state, move |file| {
        let Some(job) = file.job(number)? else {
            return Ok(None);
        };
        if job.status != JobStatus::Active {
            return Ok(None);
        }
        let Some(spec) = job_spec(file, &job)? else {
            return Ok(None);
        };
        let authz = Authz::load_now(file, &creator(&job), Vec::new(), Vec::new(), now)?;
        if authz
            .check_arn("lambda:InvokeFunction", &arn, &job.account)
            .is_err()
        {
            finish_failed(
                file,
                number,
                "AccessDenied",
                "The job's creator may not invoke the function",
            )?;
            return Ok(None);
        }
        match next_items(file, &job, &spec, INVOCATIONS_PER_STEP)? {
            Next::Complete(cursor) => {
                complete(file, number, cursor)?;
                Ok(None)
            }
            Next::Failed(code, reason) => {
                finish_failed(file, number, code, &reason)?;
                Ok(None)
            }
            Next::Items(items) => {
                // Only the account's own buckets' objects are sent.
                let mut checked = Vec::new();
                for item in items {
                    let object = match item.object {
                        Some(Ok((bucket, key, version))) => {
                            let owned = file
                                .bucket(&bucket)?
                                .is_some_and(|info| info.owner == job.account);
                            Some(if owned {
                                Ok((bucket, key, version))
                            } else {
                                Err((bucket, "Access Denied".to_string()))
                            })
                        }
                        other => other,
                    };
                    checked.push(Item {
                        start: item.start,
                        end: item.end,
                        object,
                    });
                }
                Ok(Some(checked))
            }
        }
    })
    .await?;
    let Some(items) = taken else {
        return Ok(());
    };
    let mut results = Vec::new();
    let began = std::time::Instant::now();
    for item in items {
        // Past the budget no invocation starts (the step has recorded one at
        // least: the first always starts).
        if began.elapsed() >= INVOCATION_STEP_BUDGET && !results.is_empty() {
            break;
        }
        let result = match &item.object {
            None => None,
            Some(Err((text, why))) => Some(JobResult {
                bucket: cut(text, MAX_RESULT_TEXT),
                key: String::new(),
                version_id: String::new(),
                succeeded: false,
                http_status: if why == "Access Denied" { 403 } else { 400 },
                error_code: if why == "Access Denied" {
                    "AccessDenied"
                } else {
                    "InvalidRequest"
                }
                .to_string(),
                message: cut(why, MAX_RESULT_TEXT),
            }),
            Some(Ok((bucket, key, version))) => {
                let task_id = format!("{}-{}", job.id(), item.start);
                let invocation = invoke(
                    poster,
                    &url,
                    &secret,
                    &job,
                    &lambda,
                    &task_id,
                    bucket,
                    key,
                    version.as_deref(),
                );
                // Runs to its end (the client's timeouts and the tries bound
                // it).
                let (succeeded, code, message) = invocation.await;
                Some(JobResult {
                    bucket: cut(bucket, MAX_RESULT_TEXT),
                    key: cut(key, MAX_RESULT_KEY),
                    version_id: cut(version.as_deref().unwrap_or(""), MAX_RESULT_TEXT),
                    succeeded,
                    http_status: 200,
                    error_code: code,
                    message: cut(&message, MAX_RESULT_TEXT),
                })
            }
        };
        results.push((item.start, item.end, result));
    }
    object(state, move |file| {
        for (start, end, result) in results {
            let keep = |result: &JobResult| {
                report
                    .as_ref()
                    .is_some_and(|report| !report.failed_only || !result.succeeded)
            };
            let result = result.map(|result| {
                let kept = keep(&result);
                (result, kept)
            });
            if moved(file.transaction(|tx| advance(tx, number, start, end, result)))? {
                break;
            }
        }
        Ok(())
    })
    .await
}

/// Invokes the function with one task (as AWS's LambdaInvoke event,
/// schema 1.0 or 2.0) by a signed POST to its webhook: whether it
/// succeeded, its error code (empty, PermanentFailure or
/// TemporaryFailure) and its result string or message.
#[allow(clippy::too_many_arguments)]
async fn invoke(
    poster: &Poster,
    url: &str,
    secret: &str,
    job: &Job,
    lambda: &LambdaSpec,
    task_id: &str,
    bucket: &str,
    key: &str,
    version: Option<&str>,
) -> (bool, String, String) {
    let encoded_key = percent_encoding::utf8_percent_encode(key, REPORT_KEY).to_string();
    let mut task = serde_json::json!({
        "taskId": task_id,
        "s3Key": encoded_key,
        "s3VersionId": version,
    });
    let mut job_part = serde_json::json!({ "id": job.id() });
    if lambda.schema_two {
        task["s3Bucket"] = serde_json::json!(bucket);
        let arguments: serde_json::Map<String, serde_json::Value> = lambda
            .user_arguments
            .iter()
            .map(|(key, value)| (key.clone(), serde_json::json!(value)))
            .collect();
        job_part["userArguments"] = serde_json::Value::Object(arguments);
    } else {
        task["s3BucketArn"] = serde_json::json!(format!("arn:aws:s3:::{bucket}"));
    }
    let invocation_id = format!("{}-{}", task_id, now_ms());
    let event = serde_json::json!({
        "invocationSchemaVersion": if lambda.schema_two { "2.0" } else { "1.0" },
        "invocationId": invocation_id,
        "job": job_part,
        "tasks": [task],
    })
    .to_string();
    let delivery = format!("batch/{task_id}");
    let mut last = (false, "TemporaryFailure".to_string(), String::new());
    for attempt in 0..INVOCATION_TRIES {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(1 << (attempt - 1))).await;
        }
        let secs = now_ms() / 1000;
        let headers = [
            ("content-type", "application/json".to_string()),
            ("x-truthdb-delivery", delivery.clone()),
            (
                "x-truthdb-signature",
                truthdb_s3client::webhook_signature(secret, secs, &delivery, event.as_bytes()),
            ),
        ];
        let answer = poster
            .post_answer(
                url,
                &headers,
                bytes::Bytes::from(event.clone()),
                MAX_INVOCATION_ANSWER,
            )
            .await;
        let (status, body) = match answer {
            Ok(answer) => answer,
            Err(err) => {
                last = (
                    false,
                    "TemporaryFailure".to_string(),
                    format!("Lambda invocation failed: {err}"),
                );
                continue;
            }
        };
        if status == 429 || status >= 500 {
            last = (
                false,
                "TemporaryFailure".to_string(),
                format!("Lambda returned function error: HTTP {status}"),
            );
            continue;
        }
        if !(200..300).contains(&status) {
            return (
                false,
                "PermanentFailure".to_string(),
                format!("Lambda returned function error: HTTP {status}"),
            );
        }
        let Ok(answer) = serde_json::from_slice::<serde_json::Value>(&body) else {
            return (
                false,
                "PermanentFailure".to_string(),
                "Invalid JSON returned in Lambda payload".to_string(),
            );
        };
        let result = answer["results"]
            .as_array()
            .and_then(|results| results.iter().find(|result| result["taskId"] == task_id));
        let code = match result {
            Some(result) => result["resultCode"].as_str().unwrap_or("").to_string(),
            None => answer["treatMissingKeysAs"]
                .as_str()
                .unwrap_or("TemporaryFailure")
                .to_string(),
        };
        let text = result
            .and_then(|result| result["resultString"].as_str())
            .unwrap_or("")
            .to_string();
        match code.as_str() {
            "Succeeded" => return (true, String::new(), text),
            "TemporaryFailure" => {
                last = (false, "TemporaryFailure".to_string(), text);
                continue;
            }
            _ => return (false, "PermanentFailure".to_string(), text),
        }
    }
    last
}

/// What every task of a step shares.
struct Task<'s> {
    number: u64,
    now: u64,
    delays: crate::restore::RestoreDelays,
    who: Principal,
    spec: &'s Spec,
}

/// Completes job `number` when it is still Active at `cursor` (its last
/// task done).
fn complete(file: &mut ObjFile<'_>, number: u64, cursor: u64) -> Result<(), StorageError> {
    file.transaction(|tx| {
        if let Some(mut job) = tx.job(number)?
            && job.status == JobStatus::Active
            && job.cursor == cursor
        {
            job.status = JobStatus::Completing;
            tx.put_job(&job)?;
        }
        Ok(())
    })
}

/// A manifest line's or a listed object's place: from `start` to `end` of
/// the job's cursor, and its object (`None`: a blank line, no task; an
/// error: a line that names no object, and why).
struct Item {
    start: u64,
    end: u64,
    object: Option<TaskObject>,
}

type TaskObject = Result<(String, String, Option<String>), (String, String)>;

/// The next tasks of a job: up to `max` items, or that it is done (at
/// that cursor), or why it cannot go on.
enum Next {
    Items(Vec<Item>),
    Complete(u64),
    Failed(&'static str, String),
}

fn next_items(
    file: &mut ObjFile<'_>,
    job: &Job,
    spec: &Spec,
    max: usize,
) -> Result<Next, StorageError> {
    match (&spec.manifest, &spec.generator) {
        (Some(manifest), _) => next_lines(file, job, manifest, max),
        (None, Some(generator)) => next_listed(file, job, &generator.bucket, max),
        (None, None) => Ok(Next::Failed(
            "InvalidJob",
            "The job names no manifest".into(),
        )),
    }
}

/// The next lines of a CSV manifest (the job's cursor a byte offset).
fn next_lines(
    file: &mut ObjFile<'_>,
    job: &Job,
    manifest: &ManifestSpec,
    max: usize,
) -> Result<Next, StorageError> {
    let Some(version) = job.manifest_version else {
        return Ok(Next::Failed(
            "InvalidJob",
            "The job's manifest was never prepared".into(),
        ));
    };
    let (bytes, size) = match read_manifest(file, manifest, version, job.cursor, TASK_BYTES)? {
        Ok(read) => read,
        Err((code, reason)) => return Ok(Next::Failed(code, reason)),
    };
    let mut items = Vec::new();
    let mut at = 0usize;
    let mut tasks = 0;
    while tasks < max {
        let start = job.cursor + at as u64;
        if start >= size {
            // Every line done: the job completes (a manifest of none, at once).
            if items.is_empty() {
                return Ok(Next::Complete(start));
            }
            break;
        }
        let rest = &bytes[at..];
        let (line, next) = match rest.iter().position(|&byte| byte == b'\n') {
            Some(end) => (&rest[..end], at + end + 1),
            None if start + rest.len() as u64 >= size => (rest, at + rest.len()),
            // The rest of the line is past what was read: the next step.
            None if at > 0 => break,
            None => {
                return Ok(Next::Failed(
                    "ManifestLineTooLong",
                    "A manifest line is longer than 64 KiB".into(),
                ));
            }
        };
        at = next;
        let end = job.cursor + at as u64;
        // The same test as Preparing's count: a blank line is no task, only
        // the place moves.
        if blank(line) {
            items.push(Item {
                start,
                end,
                object: None,
            });
            continue;
        }
        tasks += 1;
        let text = String::from_utf8_lossy(line);
        let text = text.trim_end_matches('\r');
        items.push(Item {
            start,
            end,
            object: Some(parse_line(text, &manifest.fields).map_err(|why| (text.to_string(), why))),
        });
    }
    Ok(Next::Items(items))
}

/// The next objects of a generated manifest (the job's cursor a task
/// number).
fn next_listed(
    file: &mut ObjFile<'_>,
    job: &Job,
    bucket: &str,
    max: usize,
) -> Result<Next, StorageError> {
    if job.cursor >= job.total {
        return Ok(Next::Complete(job.cursor));
    }
    Ok(Next::Items(
        file.job_tasks(job.number, job.cursor, max)?
            .into_iter()
            .map(|(seq, key, version_id)| Item {
                start: seq,
                end: seq + 1,
                object: Some(Ok((
                    bucket.to_string(),
                    key,
                    (!version_id.is_empty()).then_some(version_id),
                ))),
            })
            .collect(),
    ))
}

/// Runs one task (from `start` to `end` of the job's cursor) on `object`
/// (a bucket, key and version; or a line that names none, and why): its
/// change, its result and the job's place, one transaction. Whether the
/// job moved meanwhile (the step then ends).
fn run_one(
    file: &mut ObjFile<'_>,
    task: &Task<'_>,
    start: u64,
    end: u64,
    object: Result<(String, String, Option<String>), (String, String)>,
) -> Result<bool, StorageError> {
    let (number, now, spec) = (task.number, task.now, task.spec);
    let (bucket, key, version_id, outcome) = match object {
        Err((text, why)) => (
            cut(&text, MAX_RESULT_TEXT),
            String::new(),
            String::new(),
            Err(S3Error::invalid_request(why)),
        ),
        Ok((bucket, key, version)) => {
            // What cannot be read of this object fails this task, not the
            // job.
            let planned = plan(
                file,
                task.delays,
                &task.who,
                &spec.operation,
                &bucket,
                &key,
                version.as_deref(),
                now,
            )
            .unwrap_or_else(|err| Err(S3Error::internal(err.to_string())));
            (bucket, key, version.unwrap_or_default(), planned)
        }
    };
    let report = spec.report.as_ref();
    let done = file.transaction(|tx| {
        let outcome = match outcome {
            Err(err) => Err(err),
            Ok(Planned::Tags(which, tags)) => tx
                .set_tags(&bucket, &key, which, tags, now)?
                .map(|_| ())
                .map_err(|err| object_error(err, &bucket, &key)),
            Ok(Planned::Restore(which, request)) => tx
                .restore_object(&bucket, &key, which, request, now)?
                .map(|_| ())
                .map_err(|err| object_error(err, &bucket, &key)),
            Ok(Planned::Retention(which, change, bypass)) => tx
                .put_retention(&bucket, &key, which, change, bypass, now)?
                .map(|_| ())
                .map_err(|err| object_error(err, &bucket, &key)),
            Ok(Planned::LegalHold(which, on)) => tx
                .put_legal_hold(&bucket, &key, which, on)?
                .map(|_| ())
                .map_err(|err| object_error(err, &bucket, &key)),
            Ok(Planned::Copy {
                source,
                bucket: to,
                key: target,
                attrs,
                class,
                lock,
            }) => match tx.copy_object(&to, &target, &source, &attrs, class, now, DROP_GRACE_MS)? {
                Ok(copied) => {
                    if let Some(lock) = &lock {
                        tx.set_lock(copied.version, lock)?;
                    }
                    Ok(())
                }
                Err(err) => Err(object_error(err, &to, &target)),
            },
        };
        let result = JobResult {
            bucket: cut(&bucket, MAX_RESULT_TEXT),
            key: cut(&key, MAX_RESULT_KEY),
            version_id: cut(&version_id, MAX_RESULT_TEXT),
            succeeded: outcome.is_ok(),
            http_status: outcome.as_ref().map_or_else(|err| err.status, |()| 200),
            error_code: outcome
                .as_ref()
                .err()
                .map(|err| err.code.to_string())
                .unwrap_or_default(),
            message: cut(
                &outcome
                    .as_ref()
                    .err()
                    .map_or_else(|| "Successful".to_string(), |err| err.message.clone()),
                MAX_RESULT_TEXT,
            ),
        };
        let keep = report.is_some_and(|report| !report.failed_only || !result.succeeded);
        advance(tx, number, start, end, Some((result, keep)))
    });
    moved(done)
}

/// Whether a task's transaction found its job moved (and so rolled back).
fn moved(outcome: Result<(), StorageError>) -> Result<bool, StorageError> {
    match outcome {
        Ok(()) => Ok(false),
        Err(StorageError::InvalidConfig(message)) if message == MOVED => Ok(true),
        Err(err) => Err(err),
    }
}

/// Moves job `number` past the line at `start` (to `end`), counting its
/// task's result (kept for the report when `keep`), if the job is still
/// Active there; fails it past the failure threshold.
fn advance(
    tx: &mut truthdb_object::ObjTxn<'_, '_>,
    number: u64,
    start: u64,
    end: u64,
    result: Option<(JobResult, bool)>,
) -> Result<(), StorageError> {
    let Some(mut job) = tx.job(number)? else {
        return Ok(());
    };
    if job.status != JobStatus::Active || job.cursor != start {
        // Cancelled meanwhile, or moved on: this task does not count. Its
        // change rolls back with the transaction.
        return Err(StorageError::InvalidConfig(MOVED.to_string()));
    }
    job.cursor = end;
    if let Some((result, keep)) = result {
        if result.succeeded {
            job.succeeded += 1;
        } else {
            job.failed += 1;
        }
        if keep {
            tx.add_job_result(&mut job, &result)?;
        }
        let run = job.succeeded + job.failed;
        if run >= THRESHOLD_TASKS && job.failed * 2 > run {
            fail(
                &mut job,
                "TaskFailureRateExceeded",
                "More than half of the job's tasks failed, after at least 1,000 had run",
            );
        }
    }
    tx.put_job(&job)
}

/// An ending job: writes its report (when it asked for one and a task ran),
/// then ends it.
async fn finish(state: &S3State, job: Job) -> Result<(), String> {
    let number = job.number;
    let from = job.status;
    let report = object(state, {
        let job = job.clone();
        move |file| job_spec(file, &job)
    })
    .await?
    .and_then(|spec| spec.report);
    let written = match report {
        Some(report) if job.succeeded + job.failed > 0 => {
            let storage = Arc::clone(&state.storage);
            let job = job.clone();
            Some(
                tokio::task::spawn_blocking(move || write_report(&storage, &job, &report))
                    .await
                    .map_err(|err| err.to_string())?
                    .map_err(|err| err.to_string())?,
            )
        }
        _ => None,
    };
    let now = now_ms();
    object(state, move |file| {
        let Some(job) = file.job(number)? else {
            return Ok(());
        };
        if job.status != from {
            return Ok(());
        }
        let mut problem = None;
        if let Some(files) = &written {
            let bucket = &files.bucket;
            if file
                .bucket(bucket)?
                .is_none_or(|info| info.owner != job.account)
            {
                problem = Some((
                    "ReportBucketNotOwned",
                    "The report's bucket does not exist or is not the job's account's",
                ));
            }
        }
        let (bucket_lock, default) = match &written {
            Some(files) => crate::object_lock::bucket_state(file, &files.bucket)?,
            None => (false, None),
        };
        let lock = crate::object_lock::new_object_lock(None, bucket_lock, default, now)
            .ok()
            .flatten();
        if written.is_some() && problem.is_none() && !creator_holds(file, &job)? {
            problem = Some(("AccessDenied", CREATOR_GONE));
        }
        if let (Some(files), None) = (&written, problem) {
            let authz = Authz::load_now(
                file,
                &creator(&job),
                vec![files.bucket.clone()],
                Vec::new(),
                now,
            )?;
            if files.objects.iter().any(|(key, _, _)| {
                authz
                    .check_storing(
                        "s3:PutObject",
                        Resource::Object(&files.bucket, key),
                        None,
                        None,
                    )
                    .is_err()
            }) {
                problem = Some(("AccessDenied", "The job's creator may not write the report"));
            }
        }
        file.set_origin(Origin {
            operation: "PutObject".to_string(),
            request_id: job.id(),
            account: Some(job.account.clone()),
            ..Origin::default()
        });
        file.transaction(|tx| {
            let mut job = job;
            if let (Some(files), None) = (&written, problem) {
                for (key, body, content_type) in &files.objects {
                    let attrs = ObjectAttrs {
                        content_type: Some(content_type.to_string()),
                        ..ObjectAttrs::default()
                    };
                    match tx.put_object(
                        &files.bucket,
                        key,
                        body,
                        &attrs,
                        StorageClass::Standard,
                        now,
                        DROP_GRACE_MS,
                    )? {
                        Ok(info) => {
                            if let Some(lock) = &lock {
                                tx.set_lock(info.version, lock)?;
                            }
                        }
                        Err(_) => {
                            problem = Some((
                                "ReportNotWritten",
                                "The completion report could not be written",
                            ));
                        }
                    }
                }
            }
            if let Some((code, reason)) = problem {
                job.failures.push((code.to_string(), reason.to_string()));
            }
            job.status = match from {
                JobStatus::Completing => JobStatus::Complete,
                JobStatus::Cancelling => JobStatus::Cancelled,
                _ => JobStatus::Failed,
            };
            job.terminated_ms = Some(now);
            tx.put_job(&job)
        })
    })
    .await
}

/// A written report: its bucket and its objects (key, body, type).
struct ReportFiles {
    bucket: String,
    objects: Vec<(String, SealedBody, &'static str)>,
}

/// A CSV field: quoted when it holds a comma, a quote or a line break.
fn csv(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Writes the report's result files (one per outcome that has results) and
/// its `manifest.json`, as bodies to store.
fn write_report(
    storage: &truthdb_core::storage::Storage,
    job: &Job,
    report: &crate::batch::ReportSpec,
) -> Result<ReportFiles, StorageError> {
    let now = now_ms();
    let base = match report.prefix.trim_end_matches('/') {
        "" => format!("job-{}", job.id()),
        prefix => format!("{prefix}/job-{}", job.id()),
    };
    let mut objects = Vec::new();
    let mut listed = Vec::new();
    for (succeeded, status) in [(true, "succeeded"), (false, "failed")] {
        let mut writer: Option<BodyWriter> = None;
        let mut from = 0;
        loop {
            let page =
                ObjFile::new(&mut storage.lock()).job_results(job.number, from, RESULT_PAGE)?;
            let Some(&(last, _)) = page.last() else {
                break;
            };
            from = last + 1;
            let mut text = String::new();
            for (_, result) in page
                .iter()
                .filter(|(_, result)| result.succeeded == succeeded)
            {
                let key =
                    percent_encoding::utf8_percent_encode(&result.key, REPORT_KEY).to_string();
                text.push_str(&format!(
                    "{},{},{},{status},{},{},{}\n",
                    csv(&result.bucket),
                    csv(&key),
                    csv(&result.version_id),
                    result.http_status,
                    csv(&result.error_code),
                    csv(&result.message),
                ));
            }
            if text.is_empty() {
                continue;
            }
            let writer = match &mut writer {
                Some(writer) => writer,
                None => writer.insert(BodyWriter::begin(storage, now)?),
            };
            writer.write(storage, text.as_bytes(), now)?;
        }
        let Some(writer) = writer else {
            continue;
        };
        let sealed = writer.finish(storage, now)?;
        let name: String = ring::digest::digest(
            &ring::digest::SHA256,
            format!("{}/{status}", job.id()).as_bytes(),
        )
        .as_ref()[..20]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let key = format!("{base}/results/{name}.csv");
        listed.push(serde_json::json!({
            "TaskExecutionStatus": status,
            "Bucket": report.bucket,
            "MD5Checksum": sealed.md5.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "Key": key,
        }));
        objects.push((key, sealed, "text/csv"));
    }
    let manifest = serde_json::json!({
        "Format": "Report_CSV_20180820",
        "ReportCreationDate": iso8601(now),
        "Results": listed,
        "ReportSchema": REPORT_SCHEMA,
    })
    .to_string();
    let mut writer = BodyWriter::begin(storage, now)?;
    writer.write(storage, manifest.as_bytes(), now)?;
    objects.push((
        format!("{base}/manifest.json"),
        writer.finish(storage, now)?,
        "application/json",
    ));
    Ok(ReportFiles {
        bucket: report.bucket.clone(),
        objects,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_lines_name_their_objects() {
        let fields = [Field::Bucket, Field::Key, Field::VersionId];
        assert_eq!(
            parse_line("b,dir/a%20b%2Cc,7", &fields),
            Ok(("b".into(), "dir/a b,c".into(), Some("7".into())))
        );
        assert_eq!(
            parse_line("b,k", &fields),
            Ok(("b".into(), "k".into(), None))
        );
        assert!(parse_line("b,", &fields).is_err());
        assert!(parse_line("b,%FF", &fields).is_err(), "not UTF-8");
        let ignoring = [Field::Ignore, Field::Key, Field::Bucket];
        assert_eq!(
            parse_line("x,k,b", &ignoring),
            Ok(("b".into(), "k".into(), None))
        );
        assert_eq!(csv("plain"), "plain");
        assert_eq!(csv("a,\"b\""), "\"a,\"\"b\"\"\"");
        assert_eq!(version_sel("null"), Some(VersionSel::Null));
        assert_eq!(version_sel("12"), Some(VersionSel::Seq(12)));
        assert_eq!(version_sel("x"), None);
    }

    #[test]
    fn a_failing_job_waits_and_holds_up_no_other() {
        let job = |number: u64, status: JobStatus, priority: u32| {
            let mut job = Job::new("111122223333", None, &number.to_string(), Vec::new());
            job.number = number;
            job.status = status;
            job.priority = priority;
            job.results_left = false;
            job
        };
        let jobs = [
            job(1, JobStatus::Preparing, 0),
            job(2, JobStatus::Active, 5),
            job(3, JobStatus::Active, 9),
        ];
        let mut waits = Waits::default();
        let invoking = Invoking::default();
        let picked =
            |waits: &Waits| next_work(&jobs, waits, &invoking, 0).map(|(job, _)| job.number);
        assert_eq!(picked(&waits), Some(1), "preparing first");
        let now = Instant::now();
        waits.failed(1, now);
        assert_eq!(picked(&waits), Some(3), "then the highest priority");
        waits.failed(3, now);
        assert_eq!(picked(&waits), Some(2));
        waits.failed(2, now);
        assert_eq!(picked(&waits), None, "every job waits");
        // A job invoking is passed over until its invocations end.
        let mut fresh = Waits::default();
        let invoking = Invoking::default();
        let pick = |waits: &Waits, invoking: &Invoking| {
            next_work(&jobs, waits, invoking, 0).map(|(job, _)| job.number)
        };
        fresh.failed(1, now);
        assert!(invoking.take(3));
        assert_eq!(pick(&fresh, &invoking), Some(2));
        invoking.release(3);
        assert_eq!(pick(&fresh, &invoking), Some(3));
        // At most MAX_INVOKING at once.
        for n in 100..100 + MAX_INVOKING as u64 {
            assert!(invoking.take(n));
        }
        assert!(!invoking.take(3));

        // The wait doubles, to a limit.
        for _ in 0..20 {
            waits.failed(1, now);
        }
        assert_eq!(waits.0[&1].1, MAX_WAIT);
    }
}
