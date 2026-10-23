//! Batch Operations jobs (the S3 Control API's CreateJob): each job of an
//! account, what it does (kept as its request stated it, for the S3 API to
//! interpret), where it stands, and the result of each task it has run, for
//! its completion report. A task's change and the job's progress past it
//! are written in one transaction, so a job resumed after a crash runs each
//! task once.
//!
//! A job's progress is a chunked document keyed by its number, small and
//! rewritten as it runs; its request, written once, is another, in a tree
//! of its own (so what is read of every job each step stays small). Its
//! client request token maps to it, so a repeated CreateJob finds the job
//! it made; its results are rows keyed by (job, task sequence).

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::{Reader, Writer};
use crate::documents::read;
use crate::{
    JOB_REQUESTS_TREE, JOB_RESULTS_TREE, JOB_TASKS_TREE, JOBS_TREE, NEXT_JOB_KEY, ObjFile, ObjTxn,
    next_counter,
};

/// A job's status, as S3 Batch Operations names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    New,
    Preparing,
    Suspended,
    Ready,
    Active,
    Completing,
    Complete,
    Cancelling,
    Cancelled,
    Failing,
    Failed,
}

const STATUSES: [JobStatus; 11] = [
    JobStatus::New,
    JobStatus::Preparing,
    JobStatus::Suspended,
    JobStatus::Ready,
    JobStatus::Active,
    JobStatus::Completing,
    JobStatus::Complete,
    JobStatus::Cancelling,
    JobStatus::Cancelled,
    JobStatus::Failing,
    JobStatus::Failed,
];

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::New => "New",
            JobStatus::Preparing => "Preparing",
            JobStatus::Suspended => "Suspended",
            JobStatus::Ready => "Ready",
            JobStatus::Active => "Active",
            JobStatus::Completing => "Completing",
            JobStatus::Complete => "Complete",
            JobStatus::Cancelling => "Cancelling",
            JobStatus::Cancelled => "Cancelled",
            JobStatus::Failing => "Failing",
            JobStatus::Failed => "Failed",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        STATUSES.into_iter().find(|status| status.as_str() == name)
    }

    /// Whether the job has ended.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobStatus::Complete | JobStatus::Cancelled | JobStatus::Failed
        )
    }

    fn code(self) -> u8 {
        STATUSES.iter().position(|status| *status == self).unwrap() as u8
    }
}

/// A job's progress (its request is kept apart: see
/// [`ObjTxn::create_job`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    /// Its number (in its id, and the order jobs are listed in).
    pub number: u64,
    /// The random part of its id.
    pub tag: u64,
    pub account: String,
    pub token: String,
    /// A digest of the CreateJob request, to tell a repeat from another
    /// request with the same token.
    pub fingerprint: Vec<u8>,
    /// The account's user that created it (`None`: the account's root): its
    /// tasks are authorized as that user's requests would be.
    pub user: Option<String>,
    /// That user's unique id (empty for the root): a user of the same name
    /// made since is another.
    pub user_id: String,
    /// Its operation's name and its description, as ListJobs shows them.
    pub operation: String,
    pub description: String,
    pub priority: u32,
    pub status: JobStatus,
    pub status_reason: String,
    /// (code, reason) of what made it fail.
    pub failures: Vec<(String, String)>,
    pub created_ms: u64,
    pub suspended_ms: Option<u64>,
    pub active_since_ms: Option<u64>,
    pub terminated_ms: Option<u64>,
    /// The manifest version the job reads (pinned when it is prepared).
    pub manifest_version: Option<u64>,
    /// Where in the manifest the next task's line starts.
    pub cursor: u64,
    pub total: u64,
    pub succeeded: u64,
    pub failed: u64,
    /// The next result row's sequence number.
    pub next_result: u64,
    /// Whether its results are still kept (until its report is written).
    pub results_left: bool,
    /// A generated manifest's listing: the last key listed (`None`: none
    /// yet), and whether the listing is done.
    pub listed_after: Option<String>,
    pub listing_done: bool,
    /// The generated manifest written out: its bucket, key, S3 version id
    /// (empty: none) and ETag.
    pub generated: Option<(String, String, String, String)>,
}

impl Job {
    /// A job in status New.
    pub fn new(account: &str, user: Option<&str>, token: &str, fingerprint: Vec<u8>) -> Self {
        Job {
            number: 0,
            tag: 0,
            account: account.to_string(),
            token: token.to_string(),
            fingerprint,
            user: user.map(str::to_string),
            user_id: String::new(),
            operation: String::new(),
            description: String::new(),
            priority: 0,
            status: JobStatus::New,
            status_reason: String::new(),
            failures: Vec::new(),
            created_ms: 0,
            suspended_ms: None,
            active_since_ms: None,
            terminated_ms: None,
            manifest_version: None,
            cursor: 0,
            total: 0,
            succeeded: 0,
            failed: 0,
            next_result: 0,
            results_left: true,
            listed_after: None,
            listing_done: false,
            generated: None,
        }
    }

    /// Its id: its number and its random tag, 33 characters.
    pub fn id(&self) -> String {
        format!("{:016x}-{:016x}", self.number, self.tag)
    }

    /// The number and tag of id `id`, if it is one.
    pub fn parse_id(id: &str) -> Option<(u64, u64)> {
        let (number, tag) = id.split_once('-')?;
        if number.len() != 16 || tag.len() != 16 {
            return None;
        }
        Some((
            u64::from_str_radix(number, 16).ok()?,
            u64::from_str_radix(tag, 16).ok()?,
        ))
    }
}

const FORMAT: u8 = 1;

impl Job {
    fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(FORMAT);
        out.u64(self.tag);
        out.str(&self.account);
        out.str(&self.token);
        out.u32(self.fingerprint.len() as u32);
        out.0.extend_from_slice(&self.fingerprint);
        match &self.user {
            Some(user) => {
                out.u8(1);
                out.str(user);
            }
            None => out.u8(0),
        }
        out.str(&self.user_id);
        out.str(&self.operation);
        out.str(&self.description);
        out.u32(self.priority);
        out.u8(self.status.code());
        out.str(&self.status_reason);
        out.u32(self.failures.len() as u32);
        for (code, reason) in &self.failures {
            out.str(code);
            out.str(reason);
        }
        out.u64(self.created_ms);
        out.opt_u64(self.suspended_ms);
        out.opt_u64(self.active_since_ms);
        out.opt_u64(self.terminated_ms);
        out.opt_u64(self.manifest_version);
        for number in [
            self.cursor,
            self.total,
            self.succeeded,
            self.failed,
            self.next_result,
        ] {
            out.u64(number);
        }
        out.u8(self.results_left as u8);
        match &self.listed_after {
            Some(key) => {
                out.u8(1);
                out.str(key);
            }
            None => out.u8(0),
        }
        out.u8(self.listing_done as u8);
        match &self.generated {
            Some((bucket, key, version, etag)) => {
                out.u8(1);
                out.str(bucket);
                out.str(key);
                out.str(version);
                out.str(etag);
            }
            None => out.u8(0),
        }
        out.0
    }

    fn decode(number: u64, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "batch job");
        if input.u8()? != FORMAT {
            return Err(input.corrupt());
        }
        let tag = input.u64()?;
        let (account, token) = (input.str()?, input.str()?);
        let bytes = |input: &mut Reader<'_>| -> Result<Vec<u8>, StorageError> {
            let len = input.count()?;
            let rest = input.rest();
            let taken = rest[..len].to_vec();
            *input = Reader::new(&rest[len..], "batch job");
            Ok(taken)
        };
        let fingerprint = bytes(&mut input)?;
        let user = if input.present()? {
            Some(input.str()?)
        } else {
            None
        };
        let user_id = input.str()?;
        let (operation, description) = (input.str()?, input.str()?);
        let priority = input.u32()?;
        let status = *STATUSES
            .get(input.u8()? as usize)
            .ok_or_else(|| input.corrupt())?;
        let status_reason = input.str()?;
        let failures = (0..input.count()?)
            .map(|_| Ok((input.str()?, input.str()?)))
            .collect::<Result<_, StorageError>>()?;
        let job = Job {
            number,
            tag,
            account,
            token,
            fingerprint,
            user,
            user_id,
            operation,
            description,
            priority,
            status,
            status_reason,
            failures,
            created_ms: input.u64()?,
            suspended_ms: input.opt_u64()?,
            active_since_ms: input.opt_u64()?,
            terminated_ms: input.opt_u64()?,
            manifest_version: input.opt_u64()?,
            cursor: input.u64()?,
            total: input.u64()?,
            succeeded: input.u64()?,
            failed: input.u64()?,
            next_result: input.u64()?,
            results_left: input.present()?,
            listed_after: if input.present()? {
                Some(input.str()?)
            } else {
                None
            },
            listing_done: input.present()?,
            generated: if input.present()? {
                Some((input.str()?, input.str()?, input.str()?, input.str()?))
            } else {
                None
            },
        };
        if !input.rest().is_empty() {
            return Err(input.corrupt());
        }
        Ok(job)
    }
}

/// One task's result, for the completion report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobResult {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub succeeded: bool,
    pub http_status: u16,
    pub error_code: String,
    pub message: String,
}

impl JobResult {
    fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.str(&self.bucket);
        out.str(&self.key);
        out.str(&self.version_id);
        out.u8(self.succeeded as u8);
        out.u32(u32::from(self.http_status));
        out.str(&self.error_code);
        out.str(&self.message);
        out.0
    }

    fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "batch job result");
        let result = JobResult {
            bucket: input.str()?,
            key: input.str()?,
            version_id: input.str()?,
            succeeded: input.present()?,
            http_status: u16::try_from(input.u32()?).map_err(|_| input.corrupt())?,
            error_code: input.str()?,
            message: input.str()?,
        };
        if !input.rest().is_empty() {
            return Err(input.corrupt());
        }
        Ok(result)
    }
}

fn token_key(account: &str, token: &str) -> Vec<u8> {
    [account.as_bytes(), &[0], token.as_bytes()].concat()
}

fn result_key(number: u64, seq: u64) -> Vec<u8> {
    [number.to_be_bytes(), seq.to_be_bytes()].concat()
}

impl ObjTxn<'_, '_> {
    /// Creates `job` with its request `request` (numbered here; `tag` is
    /// its id's random part), unless its account made one with its token
    /// already: then that job, and whether it was made now.
    pub fn create_job(
        &mut self,
        mut job: Job,
        request: &[u8],
        tag: u64,
        now_ms: u64,
    ) -> Result<(Job, bool), StorageError> {
        let token = token_key(&job.account, &job.token);
        if let Some(row) = self.trees.job_tokens().get(self.ctx, &token)? {
            let number = u64::from_be_bytes(
                row.as_slice()
                    .try_into()
                    .map_err(|_| crate::codec::corrupt("batch job token"))?,
            );
            let existing = self
                .job(number)?
                .ok_or_else(|| crate::codec::corrupt("batch job token"))?;
            return Ok((existing, false));
        }
        job.number = next_counter(self, NEXT_JOB_KEY)?;
        job.tag = tag;
        job.created_ms = now_ms;
        self.trees.job_tokens().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &token,
            &job.number.to_be_bytes(),
        )?;
        self.put_job(&job)?;
        let requests = self.trees.job_requests();
        self.put_document(requests, &job.number.to_be_bytes(), request)?;
        Ok((job, true))
    }

    pub fn job(&mut self, number: u64) -> Result<Option<Job>, StorageError> {
        read(self.ctx, &self.trees.jobs(), &number.to_be_bytes())?
            .map(|bytes| Job::decode(number, &bytes))
            .transpose()
    }

    /// A job's request, as it was created with.
    pub fn job_request(&mut self, number: u64) -> Result<Option<Vec<u8>>, StorageError> {
        read(self.ctx, &self.trees.job_requests(), &number.to_be_bytes())
    }

    /// Writes `job` as it now stands.
    pub fn put_job(&mut self, job: &Job) -> Result<(), StorageError> {
        let jobs = self.trees.jobs();
        self.put_document(jobs, &job.number.to_be_bytes(), &job.encode())
    }

    /// Records a task's result as the job's next.
    pub fn add_job_result(
        &mut self,
        job: &mut Job,
        result: &JobResult,
    ) -> Result<(), StorageError> {
        self.trees.job_results().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &result_key(job.number, job.next_result),
            &result.encode(),
        )?;
        job.next_result += 1;
        Ok(())
    }

    /// Adds an object to a generated manifest's list, as task `job.total`
    /// (which it counts): its key and S3 version id (empty: none).
    pub fn add_job_task(
        &mut self,
        job: &mut Job,
        key: &str,
        version_id: &str,
    ) -> Result<(), StorageError> {
        let mut row = Writer::default();
        row.str(key);
        row.str(version_id);
        self.trees.job_tasks().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &result_key(job.number, job.total),
            &row.0,
        )?;
        job.total += 1;
        Ok(())
    }

    /// Removes up to `limit` of a generated manifest's list; whether none
    /// are left.
    pub fn drop_job_tasks(&mut self, number: u64, limit: usize) -> Result<bool, StorageError> {
        let tree = self.trees.job_tasks();
        let rows = tree.scan_range_limit(
            self.ctx,
            Some(&result_key(number, 0)),
            Some(&result_key(number, u64::MAX)),
            limit + 1,
        )?;
        for (key, _) in rows.iter().take(limit) {
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
        }
        Ok(rows.len() <= limit)
    }

    /// Removes up to `limit` of a job's results; whether none are left.
    pub fn drop_job_results(&mut self, number: u64, limit: usize) -> Result<bool, StorageError> {
        let tree = self.trees.job_results();
        let rows = tree.scan_range_limit(
            self.ctx,
            Some(&result_key(number, 0)),
            Some(&result_key(number, u64::MAX)),
            limit + 1,
        )?;
        for (key, _) in rows.iter().take(limit) {
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
        }
        Ok(rows.len() <= limit)
    }

    /// Removes a job (its results gone already) and its token.
    pub fn drop_job(&mut self, job: &Job) -> Result<(), StorageError> {
        self.trees.job_tokens().delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &token_key(&job.account, &job.token),
        )?;
        let jobs = self.trees.jobs();
        self.delete_document(&jobs, &job.number.to_be_bytes())?;
        let requests = self.trees.job_requests();
        self.delete_document(&requests, &job.number.to_be_bytes())?;
        Ok(())
    }
}

impl ObjFile<'_> {
    /// Every job, by number.
    pub fn jobs(&mut self) -> Result<Vec<Job>, StorageError> {
        let Some(tree) = self.existing_tree(JOBS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut ctx = self.core.page_ctx(0);
        let mut jobs = Vec::new();
        let mut current: Option<(u64, Vec<u8>)> = None;
        // Chunk keys are the job number then the chunk index.
        for (key, chunk) in tree.scan(&mut ctx)? {
            let number = u64::from_be_bytes(
                key.get(..8)
                    .and_then(|head| head.try_into().ok())
                    .ok_or_else(|| crate::codec::corrupt("batch job"))?,
            );
            match &mut current {
                Some((at, bytes)) if *at == number => bytes.extend_from_slice(&chunk),
                _ => {
                    if let Some((at, bytes)) = current.take() {
                        jobs.push(Job::decode(at, &bytes)?);
                    }
                    current = Some((number, chunk));
                }
            }
        }
        if let Some((at, bytes)) = current {
            jobs.push(Job::decode(at, &bytes)?);
        }
        Ok(jobs)
    }

    pub fn job(&mut self, number: u64) -> Result<Option<Job>, StorageError> {
        let Some(tree) = self.existing_tree(JOBS_TREE)? else {
            return Ok(None);
        };
        let mut ctx = self.core.page_ctx(0);
        read(&mut ctx, &tree, &number.to_be_bytes())?
            .map(|bytes| Job::decode(number, &bytes))
            .transpose()
    }

    /// A job's request, as it was created with.
    pub fn job_request(&mut self, number: u64) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(tree) = self.existing_tree(JOB_REQUESTS_TREE)? else {
            return Ok(None);
        };
        let mut ctx = self.core.page_ctx(0);
        read(&mut ctx, &tree, &number.to_be_bytes())
    }

    /// Up to `limit` objects of a generated manifest's list from task
    /// `from`: (task, key, version id).
    pub fn job_tasks(
        &mut self,
        number: u64,
        from: u64,
        limit: usize,
    ) -> Result<Vec<(u64, String, String)>, StorageError> {
        let Some(tree) = self.existing_tree(JOB_TASKS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut ctx = self.core.page_ctx(0);
        tree.scan_range_limit(
            &mut ctx,
            Some(&result_key(number, from)),
            Some(&result_key(number, u64::MAX)),
            limit,
        )?
        .into_iter()
        .map(|(key, row)| {
            let seq = u64::from_be_bytes(
                key.get(8..16)
                    .and_then(|tail| tail.try_into().ok())
                    .ok_or_else(|| crate::codec::corrupt("batch job task"))?,
            );
            let mut input = Reader::new(&row, "batch job task");
            let task = (seq, input.str()?, input.str()?);
            if !input.rest().is_empty() {
                return Err(input.corrupt());
            }
            Ok(task)
        })
        .collect()
    }

    /// Up to `limit` of a job's results from sequence `from`.
    pub fn job_results(
        &mut self,
        number: u64,
        from: u64,
        limit: usize,
    ) -> Result<Vec<(u64, JobResult)>, StorageError> {
        let Some(tree) = self.existing_tree(JOB_RESULTS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut ctx = self.core.page_ctx(0);
        tree.scan_range_limit(
            &mut ctx,
            Some(&result_key(number, from)),
            Some(&result_key(number, u64::MAX)),
            limit,
        )?
        .into_iter()
        .map(|(key, row)| {
            let seq = u64::from_be_bytes(
                key.get(8..16)
                    .and_then(|tail| tail.try_into().ok())
                    .ok_or_else(|| crate::codec::corrupt("batch job result"))?,
            );
            Ok((seq, JobResult::decode(&row)?))
        })
        .collect()
    }
}
