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
//! Tasks act for the job's account: on its own buckets only.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use truthdb_core::storage::StorageError;
use truthdb_object::{
    BodyWriter, Job, JobResult, JobStatus, ObjFile, ObjectAttrs, ObjectInfo, ObjectLock, Origin,
    SealedBody, StorageClass, VersionSel, time::iso8601,
};

use crate::batch::{Field, Operation, Spec, spec, stored_request};
use crate::error::S3Error;
use crate::objects::{DROP_GRACE_MS, now_ms, object_error};
use crate::server::S3State;

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
const REPORT_SCHEMA: &str =
    "Bucket, Key, VersionId, TaskStatus, ErrorCode, HTTPStatusCode, ResultMessage";

/// Runs jobs until `shutdown` turns true. A standby runs none (its log is
/// the primary's).
pub async fn run(state: Arc<S3State>, mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let more = if state.storage.is_standby() {
            false
        } else {
            match step(&state).await {
                Ok(more) => more,
                Err(err) => {
                    eprintln!("batch operations: {err}");
                    false
                }
            }
        };
        if !more {
            tokio::select! {
                _ = tokio::time::sleep(IDLE) => {}
                _ = shutdown.changed() => {}
            }
        }
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

/// One step of whichever job has work; whether there may be more now.
async fn step(state: &S3State) -> Result<bool, String> {
    let jobs = object(state, |file| file.jobs()).await?;
    let now = now_ms();
    // Ended jobs: their results go, then (after 90 days) the jobs.
    for job in jobs.iter().filter(|job| job.status.is_terminal()) {
        let number = job.number;
        if job.results_left {
            object(state, move |file| {
                file.transaction(|tx| {
                    if tx.drop_job_results(number, RESULT_PAGE)?
                        && let Some(mut job) = tx.job(number)?
                    {
                        job.results_left = false;
                        tx.put_job(&job)?;
                    }
                    Ok(())
                })
            })
            .await?;
            return Ok(true);
        }
        if job.terminated_ms.is_some_and(|at| at + KEEP_ENDED_MS < now) {
            let job = job.clone();
            object(state, move |file| file.transaction(|tx| tx.drop_job(&job))).await?;
            return Ok(true);
        }
    }
    let by_status = |status: JobStatus| jobs.iter().find(|job| job.status == status);
    if let Some(job) = by_status(JobStatus::New) {
        move_job(state, job.number, JobStatus::New, |job, _| {
            job.status = JobStatus::Preparing;
        })
        .await?;
        return Ok(true);
    }
    if let Some(job) = by_status(JobStatus::Preparing) {
        prepare(state, job.number).await?;
        return Ok(true);
    }
    if let Some(job) = jobs.iter().find(|job| {
        matches!(
            job.status,
            JobStatus::Completing | JobStatus::Cancelling | JobStatus::Failing
        )
    }) {
        finish(state, job.clone()).await?;
        return Ok(true);
    }
    if let Some(job) = by_status(JobStatus::Ready) {
        move_job(state, job.number, JobStatus::Ready, |job, now| {
            job.status = JobStatus::Active;
            job.active_since_ms.get_or_insert(now);
        })
        .await?;
        return Ok(true);
    }
    // The Active job of the highest priority; the oldest of those.
    if let Some(job) = jobs
        .iter()
        .filter(|job| job.status == JobStatus::Active)
        .max_by_key(|job| (job.priority, std::cmp::Reverse(job.number)))
    {
        run_tasks(state, job.number).await?;
        return Ok(true);
    }
    Ok(false)
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
    spec: &Spec,
    version: u64,
    offset: u64,
    max: u64,
) -> Result<ManifestRead, StorageError> {
    let manifest = &spec.manifest;
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

/// Preparing: checks the manifest and the report's bucket, pins the
/// manifest's version, and counts its lines (a step at a time).
async fn prepare(state: &S3State, number: u64) -> Result<(), String> {
    let now = now_ms();
    object(state, move |file| {
        let Some(job) = file.job(number)? else {
            return Ok(());
        };
        let Ok(spec) = stored_request(&job).and_then(|request| spec(&request)) else {
            return finish_failed(
                file,
                number,
                "InvalidJob",
                "The job's request cannot be read",
            );
        };
        let owned = |file: &mut ObjFile<'_>, bucket: &str| -> Result<bool, StorageError> {
            Ok(file
                .bucket(bucket)?
                .is_some_and(|info| info.owner == job.account))
        };
        if !owned(file, &spec.manifest.bucket)? {
            return finish_failed(
                file,
                number,
                "ManifestBucketNotOwned",
                "The manifest's bucket does not exist or is not the job's account's",
            );
        }
        if let Some(report) = &spec.report
            && !owned(file, &report.bucket)?
        {
            return finish_failed(
                file,
                number,
                "ReportBucketNotOwned",
                "The report's bucket does not exist or is not the job's account's",
            );
        }
        let version = match job.manifest_version {
            Some(version) => version,
            None => {
                let manifest = &spec.manifest;
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
        let (bytes, size) = match read_manifest(file, &spec, version, job.cursor, PREPARE_BYTES)? {
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
            .filter(|line| !line.iter().all(|byte| byte.is_ascii_whitespace()))
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
                if confirmation {
                    job.status = JobStatus::Suspended;
                    job.suspended_ms = Some(now);
                } else {
                    job.status = JobStatus::Ready;
                }
            }
            tx.put_job(&job)
        })
    })
    .await
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
fn plan(
    file: &mut ObjFile<'_>,
    account: &str,
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
                Some(info) if info.owner != account => Err(S3Error::access_denied("Access Denied")),
                Some(_) => Ok(()),
            })
        };
    if let Err(err) = owned(file, bucket)? {
        return Ok(Err(err));
    }
    let copy = match operation {
        Operation::PutTagging(tags) => return Ok(Ok(Planned::Tags(which, tags.clone()))),
        Operation::DeleteTagging => return Ok(Ok(Planned::Tags(which, Vec::new()))),
        Operation::Copy(copy) => copy,
    };
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
    Ok(Ok(Planned::Copy {
        source: Box::new(source),
        bucket: copy.bucket.clone(),
        key: target,
        attrs: Box::new(attrs),
        class: copy.class,
        lock,
    }))
}

fn cut(text: &str, max: usize) -> String {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

/// Runs up to [`TASKS_PER_STEP`] tasks of Active job `number`.
async fn run_tasks(state: &S3State, number: u64) -> Result<(), String> {
    let now = now_ms();
    object(state, move |file| {
        let Some(job) = file.job(number)? else {
            return Ok(());
        };
        if job.status != JobStatus::Active {
            return Ok(());
        }
        let Ok(spec) = stored_request(&job).and_then(|request| spec(&request)) else {
            return finish_failed(
                file,
                number,
                "InvalidJob",
                "The job's request cannot be read",
            );
        };
        let Some(version) = job.manifest_version else {
            return finish_failed(
                file,
                number,
                "InvalidJob",
                "The job's manifest was never prepared",
            );
        };
        let (bytes, size) = match read_manifest(file, &spec, version, job.cursor, TASK_BYTES)? {
            Ok(read) => read,
            Err((code, reason)) => return finish_failed(file, number, code, &reason),
        };
        file.set_origin(Origin {
            operation: match spec.operation {
                Operation::Copy(_) => "CopyObject",
                Operation::PutTagging(_) => "PutObjectTagging",
                Operation::DeleteTagging => "DeleteObjectTagging",
            }
            .to_string(),
            request_id: job.id(),
            account: Some(job.account.clone()),
            ..Origin::default()
        });
        let mut at = 0usize;
        let mut ran = 0;
        while ran < TASKS_PER_STEP {
            let start = job.cursor + at as u64;
            if start >= size {
                // Every line done: the job completes (a manifest of none, at once).
                return file.transaction(|tx| {
                    if let Some(mut job) = tx.job(number)?
                        && job.status == JobStatus::Active
                        && job.cursor == start
                    {
                        job.status = JobStatus::Completing;
                        tx.put_job(&job)?;
                    }
                    Ok(())
                });
            }
            let rest = &bytes[at..];
            let (line, next) = match rest.iter().position(|&byte| byte == b'\n') {
                Some(end) => (&rest[..end], at + end + 1),
                None if start + rest.len() as u64 >= size => (rest, at + rest.len()),
                // The rest of the line is past what was read: the next step.
                None if at > 0 => break,
                None => {
                    return finish_failed(
                        file,
                        number,
                        "ManifestLineTooLong",
                        "A manifest line is longer than 64 KiB",
                    );
                }
            };
            at = next;
            let end = job.cursor + at as u64;
            let text = String::from_utf8_lossy(line);
            let text = text.trim_end_matches('\r');
            if text.trim().is_empty() {
                // A blank line is no task: only the place moves.
                if moved(file.transaction(|tx| advance(tx, number, start, end, None)))? {
                    return Ok(());
                }
                continue;
            }
            ran += 1;
            let (bucket, key, version_id, outcome) = match parse_line(text, &spec.manifest.fields) {
                Err(why) => (
                    cut(text, MAX_RESULT_TEXT),
                    String::new(),
                    String::new(),
                    Err(S3Error::invalid_request(why)),
                ),
                Ok((bucket, key, version)) => {
                    let planned = plan(
                        file,
                        &job.account,
                        &spec.operation,
                        &bucket,
                        &key,
                        version.as_deref(),
                        now,
                    )?;
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
                    Ok(Planned::Copy {
                        source,
                        bucket: to,
                        key: target,
                        attrs,
                        class,
                        lock,
                    }) => {
                        match tx.copy_object(
                            &to,
                            &target,
                            &source,
                            &attrs,
                            class,
                            now,
                            DROP_GRACE_MS,
                        )? {
                            Ok(copied) => {
                                if let Some(lock) = &lock {
                                    tx.set_lock(copied.version, lock)?;
                                }
                                Ok(())
                            }
                            Err(err) => Err(object_error(err, &to, &target)),
                        }
                    }
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
            if moved(done)? {
                return Ok(());
            }
        }
        Ok(())
    })
    .await
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
    let report = stored_request(&job)
        .and_then(|request| spec(&request))
        .ok()
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
    const ENCODE: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~')
        .remove(b'/');
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
                let key = percent_encoding::utf8_percent_encode(&result.key, ENCODE).to_string();
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
}
