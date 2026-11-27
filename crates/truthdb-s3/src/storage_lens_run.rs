//! Storage Lens's daily metrics: each bucket's requests, counted by day, and
//! each enabled configuration's report of a day.
//!
//! A day's report is made at the first pass after it ends (and, for a
//! configuration just made, at once): the storage metrics as they are at
//! that pass, the activity metrics of the day's requests. A day missed (the
//! server down past it) is not made later: its storage is no longer there to
//! count. A report is written in batches, then its day recorded as reported:
//! it is read whole or not at all (see `truthdb-object`'s `storage_lens`).
//!
//! Requests are counted from the server's request counters (since it
//! started), the growth since the last count going to the day of the count,
//! at most a minute (or a report) apart, and once more as the server stops:
//! those since the last count before a crash are not counted.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use tokio::sync::watch;
use truthdb_engine::metrics::RequestCounts;
use truthdb_object::{
    ActivityCounts, BUCKET_ACCELERATE, LENS_BATCH, LENS_KEEP_DAYS, LensRecord, LensRecordType,
    ListAfter, ObjFile, ReplicationStatus, STORAGE_LENS, Versioning,
};

use crate::batch_run::object;
use crate::objects::now_ms;
use crate::server::S3State;
use crate::storage_lens::{LensConfig, Level, Scope, bucket_of_arn};

const DAY_MS: u64 = 86_400_000;
const IDLE: Duration = Duration::from_secs(60);
/// The versions listed in one lock hold.
const PAGE: usize = 1000;
/// The uploads listed in one lock hold (each with all its parts).
const UPLOAD_PAGE: usize = 10;
/// The prefixes a bucket's prefix metrics are kept of while it is listed:
/// past them, a new prefix is not counted.
const MAX_PREFIXES: usize = 100_000;
/// Defaults of a prefix level's selection criteria (not stated by AWS).
const DEFAULT_MAX_DEPTH: u8 = 10;
const DEFAULT_MIN_PERCENTAGE: f64 = 1.0;
const SEVEN_DAYS_MS: u64 = 7 * DAY_MS;

static WAKE: LazyLock<tokio::sync::Notify> = LazyLock::new(tokio::sync::Notify::new);

/// Makes the pass look for a report to make at once (a configuration put).
pub(crate) fn wake() {
    WAKE.notify_one();
}

fn today() -> u32 {
    (now_ms() / DAY_MS) as u32
}

/// What the pass keeps between its runs.
#[derive(Default)]
struct Pass {
    /// The request counters as last counted (none: a standby, or just
    /// promoted; empty when the server starts, so that its first count
    /// takes what they hold).
    counted: Option<HashMap<String, RequestCounts>>,
    /// The day old counts and reports were last dropped.
    maintained: Option<u32>,
    /// The configurations whose report of a day failed: not tried again
    /// that day, so that one does not keep the others from theirs.
    failed: HashSet<(String, u32)>,
    /// Those whose export of a day failed, likewise.
    export_failed: HashSet<(String, u32)>,
}

/// Counts requests and makes reports until `shutdown` turns true.
pub async fn run(state: Arc<S3State>, mut shutdown: watch::Receiver<bool>) {
    let mut pass = Pass {
        counted: Some(HashMap::new()),
        ..Pass::default()
    };
    loop {
        if *shutdown.borrow() {
            // The requests since the last count, counted before stopping.
            if !state.storage.is_standby()
                && let Err(err) = count(&state, &mut pass).await
            {
                eprintln!("storage lens: {err}");
            }
            return;
        }
        if state.storage.is_standby() {
            // What a standby answers is not counted, now or once promoted.
            pass.counted = None;
        } else if let Err(err) = run_once(&state, &mut pass, &shutdown).await {
            eprintln!("storage lens: {err}");
        }
        tokio::select! {
            _ = tokio::time::sleep(IDLE) => {}
            _ = WAKE.notified() => {}
            _ = shutdown.changed() => {}
        }
    }
}

async fn run_once(
    state: &S3State,
    pass: &mut Pass,
    shutdown: &watch::Receiver<bool>,
) -> Result<(), String> {
    let day = today();
    if pass.maintained != Some(day) {
        maintain(state, day).await?;
        pass.maintained = Some(day);
        pass.failed.retain(|(_, failed)| *failed + 1 >= day);
        pass.export_failed.retain(|(_, failed)| *failed + 1 >= day);
    }
    loop {
        count(state, pass).await?;
        if *shutdown.borrow() {
            return Ok(());
        }
        match step(state, &pass.failed, shutdown).await {
            Ok(None) => break,
            Ok(Some(_)) => {}
            // Of one configuration: not tried again that day.
            Err(Failure::Of(key, day, err)) => {
                eprintln!("storage lens: the report of {key}: {err}");
                pass.failed.insert((key, day));
            }
            // Of none: tried again after the pass's wait.
            Err(Failure::Pass(err)) => return Err(err),
        }
    }
    // The reports made, their exports.
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match export_step(state, &pass.export_failed, shutdown).await {
            Ok(false) => return Ok(()),
            Ok(true) => {}
            Err(Failure::Of(key, day, err)) => {
                eprintln!("storage lens: the export of {key}: {err}");
                pass.export_failed.insert((key, day));
            }
            Err(Failure::Pass(err)) => return Err(err),
        }
    }
}

/// Makes the first due export not in `failed`; whether there was one.
async fn export_step(
    state: &S3State,
    failed: &HashSet<(String, u32)>,
    shutdown: &watch::Receiver<bool>,
) -> Result<bool, Failure> {
    let region = state.region.clone();
    let failed = failed.clone();
    let due = object(state, move |file| {
        for key in file.bucket_config_ids_with("", STORAGE_LENS, "")? {
            let Some(lens) = file.lens_state(&key)? else {
                continue;
            };
            let Some(day) = lens.reported else {
                continue;
            };
            if lens.exported.is_some_and(|last| last >= day) || failed.contains(&(key.clone(), day))
            {
                continue;
            }
            let Some(document) = file.bucket_config("", STORAGE_LENS, &key)? else {
                continue;
            };
            let Some((account, id)) = key.split_once('/') else {
                continue;
            };
            let text = String::from_utf8_lossy(&document).into_owned();
            let Ok(config) = crate::storage_lens::read(&text, &region, account) else {
                continue;
            };
            let Some(export) = config.export.filter(|_| config.enabled) else {
                continue;
            };
            return Ok(Some(crate::storage_lens_export::ExportDue {
                account: account.to_string(),
                id: id.to_string(),
                key: key.clone(),
                document,
                generation: lens.generation,
                day,
                export,
            }));
        }
        Ok(None)
    })
    .await
    .map_err(Failure::Pass)?;
    let Some(due) = due else {
        return Ok(false);
    };
    let (key, day) = (due.key.clone(), due.day);
    crate::storage_lens_export::export(state, due, shutdown)
        .await
        .map_err(|err| Failure::Of(key, day, err))?;
    Ok(true)
}

/// Drops, in batches, request counts and reports past keeping, and the
/// reports of deleted configurations.
async fn maintain(state: &S3State, day: u32) -> Result<(), String> {
    let keep_from = day.saturating_sub(LENS_KEEP_DAYS - 1);
    while object(state, move |file| {
        file.transaction(|tx| tx.expire_lens_activity(keep_from))
    })
    .await?
    {}
    while object(state, move |file| {
        file.transaction(|tx| tx.lens_maintain(keep_from))
    })
    .await?
    {}
    Ok(())
}

fn snapshot(state: &S3State) -> HashMap<String, RequestCounts> {
    state
        .request_metrics
        .snapshot()
        .into_iter()
        .map(|(bucket, counters)| (bucket, counters.counts()))
        .collect()
}

fn as_array(counts: &RequestCounts) -> ActivityCounts {
    [
        counts.all,
        counts.get,
        counts.put,
        counts.delete,
        counts.head,
        counts.post,
        counts.list,
        counts.bytes_downloaded,
        counts.bytes_uploaded,
        counts.errors_4xx,
        counts.errors_5xx,
    ]
}

/// What each bucket's counters grew by since `before` (a counter below
/// what it was: counted again from 0).
pub(crate) fn growth(
    now: &HashMap<String, RequestCounts>,
    before: &HashMap<String, RequestCounts>,
) -> Vec<(String, ActivityCounts)> {
    let mut out: Vec<(String, ActivityCounts)> = now
        .iter()
        .filter(|(bucket, _)| !bucket.is_empty())
        .filter_map(|(bucket, counts)| {
            let now = as_array(counts);
            let before = before.get(bucket).map(as_array).unwrap_or_default();
            let mut grown = [0u64; 11];
            for ((grown, now), before) in grown.iter_mut().zip(now).zip(before) {
                *grown = if now >= before { now - before } else { now };
            }
            grown
                .iter()
                .any(|&count| count > 0)
                .then(|| (bucket.clone(), grown))
        })
        .collect();
    out.sort();
    out
}

/// Adds the requests counted since the last count to today's counts of
/// their buckets (those that exist). The first count after starting
/// counts what the counters hold (the requests since the server started);
/// the first after a standby is promoted counts nothing.
async fn count(state: &S3State, pass: &mut Pass) -> Result<(), String> {
    let now = snapshot(state);
    let Some(before) = &pass.counted else {
        // Just promoted: from now on.
        pass.counted = Some(now);
        return Ok(());
    };
    let grown = growth(&now, before);
    let day = today();
    // One bucket's counts a row: a transaction of at most a batch. A batch
    // written is counted as such at once, so that one failing after it does
    // not have it counted again.
    for chunk in grown.chunks(LENS_BATCH) {
        let chunk = chunk.to_vec();
        let buckets: Vec<String> = chunk.iter().map(|(bucket, _)| bucket.clone()).collect();
        object(state, move |file| {
            let mut existing = Vec::new();
            for (bucket, add) in chunk {
                if let Some(info) = file.bucket(&bucket)? {
                    existing.push((bucket, info.created_ms, add));
                }
            }
            if existing.is_empty() {
                return Ok(());
            }
            file.transaction(|tx| {
                for (bucket, created_ms, add) in &existing {
                    tx.add_lens_activity(bucket, *created_ms, day, add)?;
                }
                Ok(())
            })
        })
        .await?;
        let counted = pass.counted.get_or_insert_with(HashMap::new);
        for bucket in buckets {
            if let Some(counts) = now.get(&bucket) {
                counted.insert(bucket, counts.clone());
            }
        }
    }
    pass.counted = Some(now);
    Ok(())
}

/// A configuration whose report is due.
struct Due {
    key: String,
    account: String,
    document: Vec<u8>,
    generation: u64,
    /// The day of its last report.
    reported: Option<u32>,
    config: LensConfig,
}

/// Why a step failed.
enum Failure {
    /// The report of a configuration, of a day.
    Of(String, u32, String),
    /// Finding a report due.
    Pass(String),
}

/// Whether configuration `key` has a report of `day` due: its `Due`.
fn due_of(
    file: &mut ObjFile<'_>,
    key: &str,
    day: u32,
    region: &str,
    now: u64,
) -> Result<Option<Due>, truthdb_core::storage::StorageError> {
    let lens = file.lens_state(key)?;
    if lens.is_some_and(|lens| lens.reported.is_some_and(|last| last >= day)) {
        return Ok(None);
    }
    let Some(document) = file.bucket_config("", STORAGE_LENS, key)? else {
        return Ok(None);
    };
    let Some((account, _)) = key.split_once('/') else {
        return Ok(None);
    };
    let text = String::from_utf8_lossy(&document).into_owned();
    let Ok(config) = crate::storage_lens::read(&text, region, account) else {
        return Ok(None);
    };
    if !config.enabled {
        return Ok(None);
    }
    // A configuration put before its state was kept is given one.
    let lens = match lens {
        Some(lens) => lens,
        None => {
            file.transaction(|tx| tx.track_lens(key, now))?;
            file.lens_state(key)?.expect("just tracked")
        }
    };
    Ok(Some(Due {
        account: account.to_string(),
        key: key.to_string(),
        document,
        generation: lens.generation,
        reported: lens.reported,
        config,
    }))
}

/// Makes the first due report not in `failed`; the configuration whose
/// report it was, if there was one.
async fn step(
    state: &S3State,
    failed: &HashSet<(String, u32)>,
    shutdown: &watch::Receiver<bool>,
) -> Result<Option<String>, Failure> {
    let day = today() - 1;
    let region = state.region.clone();
    let skip: HashSet<String> = failed
        .iter()
        .filter(|(_, failed)| *failed == day)
        .map(|(key, _)| key.clone())
        .collect();
    let now = now_ms();
    // A configuration whose state cannot be read is failed alone.
    let due = object(state, move |file| {
        for key in file.bucket_config_ids_with("", STORAGE_LENS, "")? {
            if skip.contains(&key) {
                continue;
            }
            match due_of(file, &key, day, &region, now) {
                Ok(None) => {}
                Ok(Some(due)) => return Ok(Ok(Some(due))),
                Err(err) => return Ok(Err((key, err.to_string()))),
            }
        }
        Ok(Ok(None))
    })
    .await
    .map_err(Failure::Pass)?
    .map_err(|(key, err)| Failure::Of(key, day, err))?;
    let Some(due) = due else {
        return Ok(None);
    };
    let key = due.key.clone();
    let failure = |err: String| Failure::Of(key.clone(), day, err);
    let Some(records) = report(state, &due, day, shutdown).await.map_err(failure)? else {
        return Ok(None);
    };
    write(state, &due, day, records).await.map_err(failure)?;
    Ok(Some(due.key))
}

/// Writes `due`'s report of `day`: what an earlier one cut short left is
/// cleared, the records written, then the day recorded as reported, each a
/// transaction of at most a batch. Each first finds the configuration as
/// it was when the report began (else nothing more is written: it is made
/// again, or not at all, from what it is now).
async fn write(
    state: &S3State,
    due: &Due,
    day: u32,
    records: Vec<LensRecord>,
) -> Result<(), String> {
    let (key, generation) = (due.key.clone(), due.generation);
    let document = Arc::new(due.document.clone());
    let unchanged = move |file: &mut ObjFile<'_>, key: &str, document: &[u8]| {
        Ok::<_, truthdb_core::storage::StorageError>(
            file.bucket_config("", STORAGE_LENS, key)?.as_deref() == Some(document)
                && file
                    .lens_state(key)?
                    .is_some_and(|lens| lens.generation == generation),
        )
    };
    let from = due.reported.map_or(0, |last| last + 1);
    loop {
        let (key, document) = (key.clone(), Arc::clone(&document));
        let more = object(state, move |file| {
            if !unchanged(file, &key, &document)? {
                return Ok(None);
            }
            file.transaction(|tx| tx.clear_lens_days(&key, generation, from))
                .map(Some)
        })
        .await?;
        match more {
            None => return Ok(()),
            Some(true) => continue,
            Some(false) => break,
        }
    }
    let ends = truthdb_object::lens_batches(&due.key, &records);
    let records = Arc::new(records);
    let mut start = 0;
    for end in ends {
        let (key, document, records) = (key.clone(), Arc::clone(&document), Arc::clone(&records));
        let written = object(state, move |file| {
            if !unchanged(file, &key, &document)? {
                return Ok(false);
            }
            file.transaction(|tx| {
                tx.put_lens_records(&key, generation, day, &records[start..end])
            })?;
            Ok(true)
        })
        .await?;
        start = end;
        if !written {
            return Ok(());
        }
    }
    object(state, move |file| {
        if unchanged(file, &key, &document)? {
            file.transaction(|tx| tx.lens_reported(&key, generation, day))?;
        }
        Ok(())
    })
    .await
}

/// One storage class's counts, of a bucket or of a prefix.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Counts {
    current_bytes: u64,
    /// Current versions, current delete markers included.
    current: u64,
    noncurrent_bytes: u64,
    noncurrent: u64,
    delete_markers: u64,
    upload_bytes: u64,
    uploads: u64,
    old_upload_bytes: u64,
    old_uploads: u64,
    encrypted_bytes: u64,
    encrypted: u64,
    replicated_from_bytes: u64,
    replicated_from: u64,
    replica_bytes: u64,
    replicas: u64,
    locked_bytes: u64,
    locked: u64,
}

impl Counts {
    fn add(&mut self, other: &Counts) {
        let Counts {
            current_bytes,
            current,
            noncurrent_bytes,
            noncurrent,
            delete_markers,
            upload_bytes,
            uploads,
            old_upload_bytes,
            old_uploads,
            encrypted_bytes,
            encrypted,
            replicated_from_bytes,
            replicated_from,
            replica_bytes,
            replicas,
            locked_bytes,
            locked,
        } = other;
        self.current_bytes += current_bytes;
        self.current += current;
        self.noncurrent_bytes += noncurrent_bytes;
        self.noncurrent += noncurrent;
        self.delete_markers += delete_markers;
        self.upload_bytes += upload_bytes;
        self.uploads += uploads;
        self.old_upload_bytes += old_upload_bytes;
        self.old_uploads += old_uploads;
        self.encrypted_bytes += encrypted_bytes;
        self.encrypted += encrypted;
        self.replicated_from_bytes += replicated_from_bytes;
        self.replicated_from += replicated_from;
        self.replica_bytes += replica_bytes;
        self.replicas += replicas;
        self.locked_bytes += locked_bytes;
        self.locked += locked;
    }

    fn storage_bytes(&self) -> u64 {
        // Delete markers are of no bytes; object metadata is not counted.
        self.current_bytes + self.noncurrent_bytes + self.upload_bytes
    }

    /// The free storage metrics, by their names.
    fn metrics(&self) -> [(&'static str, u64); 20] {
        [
            ("StorageBytes", self.storage_bytes()),
            ("ObjectCount", self.current + self.noncurrent + self.uploads),
            ("CurrentVersionStorageBytes", self.current_bytes),
            ("CurrentVersionObjectCount", self.current),
            ("NonCurrentVersionStorageBytes", self.noncurrent_bytes),
            ("NonCurrentVersionObjectCount", self.noncurrent),
            ("DeleteMarkerStorageBytes", 0),
            ("DeleteMarkerObjectCount", self.delete_markers),
            ("IncompleteMultipartUploadStorageBytes", self.upload_bytes),
            ("IncompleteMultipartUploadObjectCount", self.uploads),
            (
                "IncompleteMPUStorageBytesOlderThan7Days",
                self.old_upload_bytes,
            ),
            ("IncompleteMPUObjectCountOlderThan7Days", self.old_uploads),
            ("EncryptedStorageBytes", self.encrypted_bytes),
            ("EncryptedObjectCount", self.encrypted),
            ("ReplicatedStorageBytesSource", self.replicated_from_bytes),
            ("ReplicatedObjectCountSource", self.replicated_from),
            ("ReplicatedStorageBytes", self.replica_bytes),
            ("ReplicatedObjectCount", self.replicas),
            ("ObjectLockEnabledStorageBytes", self.locked_bytes),
            ("ObjectLockEnabledObjectCount", self.locked),
        ]
    }
}

/// What one bucket's records are made of.
#[derive(Debug, Default)]
pub(crate) struct BucketTally {
    pub(crate) name: String,
    /// By storage class.
    pub(crate) classes: BTreeMap<String, Counts>,
    /// By prefix, then storage class.
    pub(crate) prefixes: BTreeMap<String, BTreeMap<String, Counts>>,
    /// Its settings' metrics (bucket counts: 0 or 1).
    pub(crate) settings: Vec<(&'static str, u64)>,
    /// Its rule counts (advanced cost optimization, data protection).
    pub(crate) lifecycle_rules: Vec<(&'static str, u64)>,
    pub(crate) replication_rules: Vec<(&'static str, u64)>,
    /// Its requests of the day.
    pub(crate) activity: ActivityCounts,
}

const ACTIVITY: [&str; 11] = [
    "AllRequests",
    "GetRequests",
    "PutRequests",
    "DeleteRequests",
    "HeadRequests",
    "PostRequests",
    "ListRequests",
    "BytesDownloaded",
    "BytesUploaded",
    "4xxErrors",
    "5xxErrors",
];

/// The records of one level: a bucket's, or (its name empty) the
/// account's.
fn level_records(
    level: &Level,
    record_type: LensRecordType,
    tally: &BucketTally,
) -> Vec<LensRecord> {
    let record = |storage_class: &str, metric: &str, value: u64| LensRecord {
        record_type,
        bucket: tally.name.clone(),
        record_value: String::new(),
        storage_class: storage_class.to_string(),
        metric: metric.to_string(),
        value,
    };
    let mut out = Vec::new();
    for (class, counts) in &tally.classes {
        for (metric, value) in counts.metrics() {
            out.push(record(class, metric, value));
        }
    }
    let unclassed = tally
        .settings
        .iter()
        .chain(
            tally
                .lifecycle_rules
                .iter()
                .filter(|_| level.cost_optimization),
        )
        .chain(
            tally
                .replication_rules
                .iter()
                .filter(|_| level.data_protection),
        )
        .copied()
        .chain(
            ACTIVITY
                .iter()
                .zip(tally.activity)
                .filter(|_| level.activity)
                .map(|(metric, value)| (*metric, value)),
        );
    for (metric, value) in unclassed {
        out.push(record("", metric, value));
    }
    out
}

/// A configuration's records of its buckets' tallies: each bucket's, its
/// prefixes', and the account's (their sums).
pub(crate) fn records(config: &LensConfig, tallies: &[BucketTally]) -> Vec<LensRecord> {
    let mut out = Vec::new();
    let mut account = BucketTally::default();
    let sum = |into: &mut Vec<(&'static str, u64)>, from: &[(&'static str, u64)]| {
        for (metric, value) in from {
            match into.iter_mut().find(|(name, _)| name == metric) {
                Some((_, total)) => *total += value,
                None => into.push((metric, *value)),
            }
        }
    };
    for tally in tallies {
        out.extend(level_records(
            &config.bucket_level,
            LensRecordType::Bucket,
            tally,
        ));
        for (prefix, classes) in &tally.prefixes {
            for (class, counts) in classes {
                for (metric, value) in counts.metrics() {
                    out.push(LensRecord {
                        record_type: LensRecordType::Prefix,
                        bucket: tally.name.clone(),
                        record_value: prefix.clone(),
                        storage_class: class.clone(),
                        metric: metric.to_string(),
                        value,
                    });
                }
            }
        }
        for (class, counts) in &tally.classes {
            account
                .classes
                .entry(class.clone())
                .or_default()
                .add(counts);
        }
        sum(&mut account.settings, &tally.settings);
        sum(&mut account.lifecycle_rules, &tally.lifecycle_rules);
        sum(&mut account.replication_rules, &tally.replication_rules);
        for (total, value) in account.activity.iter_mut().zip(tally.activity) {
            *total += value;
        }
    }
    out.extend(level_records(
        &config.account_level,
        LensRecordType::Account,
        &account,
    ));
    out
}

/// Whether `config` covers bucket `name` (in this server's Region).
fn in_scope(config: &LensConfig, name: &str, region: &str) -> bool {
    let named = |buckets: &[String], regions: &[String]| {
        buckets.iter().any(|arn| bucket_of_arn(arn) == Some(name))
            || regions.iter().any(|named| named == region)
    };
    match &config.scope {
        Scope::All => true,
        Scope::Include { buckets, regions } => named(buckets, regions),
        Scope::Exclude { buckets, regions } => !named(buckets, regions),
    }
}

/// How a configuration's prefixes are made: the delimiter, the depth, and
/// the share of its bucket's storage a prefix has at least.
struct PrefixRule {
    delimiter: String,
    max_depth: u8,
    min_percentage: f64,
}

fn prefix_rule(config: &LensConfig) -> Option<PrefixRule> {
    let prefix = config
        .prefix_level
        .as_ref()
        .filter(|prefix| prefix.enabled)?;
    let delimiter = prefix
        .delimiter
        .clone()
        .or_else(|| config.prefix_delimiter.clone())
        .filter(|delimiter| !delimiter.is_empty())
        .unwrap_or_else(|| "/".to_string());
    Some(PrefixRule {
        delimiter,
        max_depth: prefix.max_depth.unwrap_or(DEFAULT_MAX_DEPTH),
        min_percentage: prefix
            .min_storage_percentage
            .unwrap_or(DEFAULT_MIN_PERCENTAGE),
    })
}

/// The prefixes of `key`, one a depth up to `max_depth`: each ends with the
/// delimiter.
fn prefixes_of<'a>(key: &'a str, rule: &PrefixRule) -> impl Iterator<Item = &'a str> {
    let delimiter = rule.delimiter.clone();
    key.match_indices(rule.delimiter.as_str())
        .take(rule.max_depth as usize)
        .map(move |(at, _)| &key[..at + delimiter.len()])
}

impl BucketTally {
    fn count(
        &mut self,
        key: &str,
        class: &str,
        rule: Option<&PrefixRule>,
        add: impl Fn(&mut Counts),
    ) {
        add(self.classes.entry(class.to_string()).or_default());
        let Some(rule) = rule else {
            return;
        };
        for prefix in prefixes_of(key, rule) {
            if !self.prefixes.contains_key(prefix) && self.prefixes.len() >= MAX_PREFIXES {
                continue;
            }
            add(self
                .prefixes
                .entry(prefix.to_string())
                .or_default()
                .entry(class.to_string())
                .or_default());
        }
    }

    /// Keeps the prefixes of at least `rule`'s share of the bucket's
    /// storage.
    fn select_prefixes(&mut self, rule: &PrefixRule) {
        let total: u64 = self.classes.values().map(Counts::storage_bytes).sum();
        let least = total as f64 * rule.min_percentage / 100.0;
        self.prefixes.retain(|_, classes| {
            let bytes: u64 = classes.values().map(Counts::storage_bytes).sum();
            total > 0 && bytes as f64 >= least
        });
    }
}

/// Makes `due`'s report of `day`: none when the server shuts down.
async fn report(
    state: &S3State,
    due: &Due,
    day: u32,
    shutdown: &watch::Receiver<bool>,
) -> Result<Option<Vec<LensRecord>>, String> {
    let (account, region) = (due.account.clone(), state.region.clone());
    let config = due.config.clone();
    let buckets = object(state, move |file| {
        Ok(file
            .list_buckets()?
            .into_iter()
            .filter(|info| info.owner == account && info.warehouse.is_none())
            .filter(|info| in_scope(&config, &info.name, &region))
            .map(|info| (info.name, info.created_ms))
            .collect::<Vec<_>>())
    })
    .await?;
    let rule = prefix_rule(&due.config).map(Arc::new);
    let mut tallies = Vec::new();
    for (name, created_ms) in buckets {
        if *shutdown.borrow() {
            return Ok(None);
        }
        if let Some(tally) = tally(state, &name, created_ms, day, rule.clone(), shutdown).await? {
            tallies.push(tally);
        }
    }
    Ok(Some(records(&due.config, &tallies)))
}

/// The settings and rule counts of a bucket (made at `created_ms`), and
/// its requests of `day`.
fn bucket_facts(
    file: &mut ObjFile<'_>,
    name: &str,
    created_ms: u64,
    day: u32,
    region: &str,
) -> Result<Option<BucketTally>, truthdb_core::storage::StorageError> {
    let Some(info) = file
        .bucket(name)?
        .filter(|info| info.created_ms == created_ms)
    else {
        return Ok(None);
    };
    let accelerated = file.bucket_config(name, BUCKET_ACCELERATE, "")?.as_deref() == Some(&[1][..]);
    let notified = file
        .notification(name)?
        .is_some_and(|config| !config.rules.is_empty());
    let settings = vec![
        (
            "VersioningEnabledBucketCount",
            u64::from(info.versioning == Versioning::Enabled),
        ),
        // Neither is offered here.
        ("MFADeleteEnabledBucketCount", 0),
        ("SSEKMSEnabledBucketCount", 0),
        // The only Object Ownership a bucket here has.
        ("ObjectOwnershipBucketOwnerEnforcedBucketCount", 1),
        ("ObjectOwnershipBucketOwnerPreferredBucketCount", 0),
        ("ObjectOwnershipObjectWriterBucketCount", 0),
        (
            "TransferAccelerationEnabledBucketCount",
            u64::from(accelerated),
        ),
        ("EventNotificationEnabledBucketCount", u64::from(notified)),
    ];
    let rules = file
        .lifecycle(name)?
        .map(|config| config.rules)
        .unwrap_or_default();
    let enabled: Vec<_> = rules.iter().filter(|rule| rule.enabled).collect();
    let having = |test: &dyn Fn(&truthdb_object::Rule) -> bool| {
        enabled.iter().filter(|rule| test(rule)).count() as u64
    };
    use truthdb_object::Expiration;
    let lifecycle_rules = vec![
        (
            "TransitionLifecycleRuleCount",
            having(&|rule| !rule.transitions.is_empty()),
        ),
        (
            "ExpirationLifecycleRuleCount",
            having(&|rule| matches!(rule.expiration, Some(Expiration::At(_)))),
        ),
        (
            "NoncurrentVersionTransitionLifecycleRuleCount",
            having(&|rule| !rule.noncurrent_transitions.is_empty()),
        ),
        (
            "NoncurrentVersionExpirationLifecycleRuleCount",
            having(&|rule| rule.noncurrent_expiration.is_some()),
        ),
        (
            "AbortIncompleteMPULifecycleRuleCount",
            having(&|rule| rule.abort_upload_days.is_some()),
        ),
        (
            "ExpiredObjectDeleteMarkerLifecycleRuleCount",
            having(&|rule| {
                matches!(
                    rule.expiration,
                    Some(Expiration::ExpiredObjectDeleteMarker(true))
                )
            }),
        ),
        ("TotalLifecycleRuleCount", enabled.len() as u64),
    ];
    let mut replication = [0u64; 5];
    for rule in file
        .replication(name)?
        .map(|config| config.rules)
        .unwrap_or_default()
        .into_iter()
        .filter(|rule| rule.enabled)
    {
        let destination = &rule.destination;
        let target = match file.bucket(&destination.bucket)? {
            Some(local) => Some((region.to_string(), local.owner)),
            None => file.remote_bucket(&destination.bucket)?.map(|remote| {
                (
                    remote.region,
                    destination.account.clone().unwrap_or(info.owner.clone()),
                )
            }),
        };
        let Some((target_region, target_account)) = target else {
            replication[4] += 1;
            continue;
        };
        replication[if target_region == region { 0 } else { 1 }] += 1;
        replication[if target_account == info.owner { 2 } else { 3 }] += 1;
    }
    let replication_rules = vec![
        ("SameRegionReplicationRuleCount", replication[0]),
        ("CrossRegionReplicationRuleCount", replication[1]),
        ("SameAccountReplicationRuleCount", replication[2]),
        ("CrossAccountReplicationRuleCount", replication[3]),
        ("InvalidDestinationReplicationRuleCount", replication[4]),
    ];
    Ok(Some(BucketTally {
        name: name.to_string(),
        settings,
        lifecycle_rules,
        replication_rules,
        activity: file
            .lens_activity(name, created_ms, day)?
            .unwrap_or_default(),
        ..BucketTally::default()
    }))
}

/// Lists bucket `name` into its tally: none when it goes (or is made
/// again) meanwhile, or the server shuts down.
async fn tally(
    state: &S3State,
    name: &str,
    created_ms: u64,
    day: u32,
    rule: Option<Arc<PrefixRule>>,
    shutdown: &watch::Receiver<bool>,
) -> Result<Option<BucketTally>, String> {
    let (bucket, region) = (name.to_string(), state.region.clone());
    let Some(mut tally) = object(state, move |file| {
        bucket_facts(file, &bucket, created_ms, day, &region)
    })
    .await?
    else {
        return Ok(None);
    };
    let encrypted_at_rest = state.storage.is_encrypted();
    let mut after: Option<(String, Option<u64>)> = None;
    loop {
        if *shutdown.borrow() {
            return Ok(None);
        }
        let (bucket, resume, page_rule) = (name.to_string(), after.clone(), rule.clone());
        let mut part = BucketTally::default();
        let page = object(state, move |file| {
            if file
                .bucket(&bucket)?
                .is_none_or(|info| info.created_ms != created_ms)
            {
                return Ok(None);
            }
            let after = resume.as_ref().map(|(key, version)| match version {
                Some(version) => ListAfter::Version(key, *version),
                None => ListAfter::Key(key),
            });
            let Ok(listing) = file.list_versions(&bucket, "", None, after, PAGE)? else {
                return Ok(None);
            };
            for object in &listing.objects {
                let info = &object.info;
                let class = info.storage_class.as_str();
                let (size, marker) = (info.size, info.delete_marker);
                let encrypted = !marker && (encrypted_at_rest || info.attrs.customer_key.is_some());
                let (status, locked) = match marker {
                    true => (None, false),
                    false => {
                        let lock = file.object_lock(&bucket, &object.key, info.version)?;
                        (
                            file.replication_status(&bucket, &object.key, info.version)?,
                            lock.retention.is_some() || lock.legal_hold == Some(true),
                        )
                    }
                };
                let latest = object.latest;
                part.count(&object.key, class, page_rule.as_deref(), |counts| {
                    if latest {
                        counts.current += 1;
                        counts.current_bytes += size;
                    } else {
                        counts.noncurrent += 1;
                        counts.noncurrent_bytes += size;
                    }
                    if marker {
                        counts.delete_markers += 1;
                        return;
                    }
                    if encrypted {
                        counts.encrypted += 1;
                        counts.encrypted_bytes += size;
                    }
                    match status {
                        Some(ReplicationStatus::Completed) => {
                            counts.replicated_from += 1;
                            counts.replicated_from_bytes += size;
                        }
                        Some(ReplicationStatus::Replica) => {
                            counts.replicas += 1;
                            counts.replica_bytes += size;
                        }
                        _ => {}
                    }
                    if locked {
                        counts.locked += 1;
                        counts.locked_bytes += size;
                    }
                });
            }
            let next = listing.last.map(|(key, _)| (key, listing.last_version));
            Ok(Some((part, next, listing.truncated)))
        })
        .await?;
        let Some((part, next, truncated)) = page else {
            return Ok(None);
        };
        tally.merge(part);
        if !truncated || next.is_none() {
            break;
        }
        after = next;
    }
    // Its incomplete uploads, with their parts' bytes.
    let now = now_ms();
    let mut after: Option<(String, u64)> = None;
    loop {
        if *shutdown.borrow() {
            return Ok(None);
        }
        let (bucket, resume, page_rule) = (name.to_string(), after.clone(), rule.clone());
        let mut part = BucketTally::default();
        let page = object(state, move |file| {
            if file
                .bucket(&bucket)?
                .is_none_or(|info| info.created_ms != created_ms)
            {
                return Ok(None);
            }
            let resume = resume.as_ref().map(|(key, upload)| (key.as_str(), *upload));
            let (uploads, more) = file.list_uploads(&bucket, "", resume, UPLOAD_PAGE)?;
            for upload in &uploads {
                let mut bytes = 0;
                let mut from = 0;
                loop {
                    let (parts, more) = file.list_parts(upload.upload, from, PAGE)?;
                    bytes += parts.iter().map(|part| part.size).sum::<u64>();
                    match (more, parts.last()) {
                        (true, Some(last)) => from = last.number,
                        _ => break,
                    }
                }
                let old = now.saturating_sub(upload.initiated_ms) > SEVEN_DAYS_MS;
                part.count(
                    &upload.key,
                    upload.storage_class.as_str(),
                    page_rule.as_deref(),
                    |counts| {
                        counts.uploads += 1;
                        counts.upload_bytes += bytes;
                        if old {
                            counts.old_uploads += 1;
                            counts.old_upload_bytes += bytes;
                        }
                    },
                );
            }
            let next = uploads
                .last()
                .map(|upload| (upload.key.clone(), upload.upload));
            Ok(Some((part, next, more)))
        })
        .await?;
        let Some((part, next, more)) = page else {
            return Ok(None);
        };
        tally.merge(part);
        if !more || next.is_none() {
            break;
        }
        after = next;
    }
    if let Some(rule) = &rule {
        tally.select_prefixes(rule);
    }
    Ok(Some(tally))
}

impl BucketTally {
    fn merge(&mut self, other: BucketTally) {
        for (class, counts) in other.classes {
            self.classes.entry(class).or_default().add(&counts);
        }
        for (prefix, classes) in other.prefixes {
            if !self.prefixes.contains_key(&prefix) && self.prefixes.len() >= MAX_PREFIXES {
                continue;
            }
            let into = self.prefixes.entry(prefix).or_default();
            for (class, counts) in classes {
                into.entry(class).or_default().add(&counts);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(all: u64, get: u64) -> RequestCounts {
        RequestCounts {
            all,
            get,
            ..RequestCounts::default()
        }
    }

    #[test]
    fn requests_are_counted_by_what_they_grew_by() {
        let before = HashMap::from([
            ("a".to_string(), counts(5, 2)),
            ("b".to_string(), counts(3, 0)),
        ]);
        let now = HashMap::from([
            ("a".to_string(), counts(8, 2)),
            // Counted again from 0 (the server restarted).
            ("b".to_string(), counts(1, 1)),
            ("c".to_string(), counts(2, 0)),
            ("".to_string(), counts(9, 9)),
        ]);
        let grown = growth(&now, &before);
        let mut a = [0u64; 11];
        a[0] = 3;
        let mut b = [0u64; 11];
        b[0] = 1;
        b[1] = 1;
        let mut c = [0u64; 11];
        c[0] = 2;
        assert_eq!(
            grown,
            vec![("a".into(), a), ("b".into(), b), ("c".into(), c)]
        );
        // Nothing grown: nothing to add.
        assert!(growth(&now, &now).is_empty());
    }

    fn config(xml: &str) -> LensConfig {
        crate::storage_lens::read(xml, "us-east-1", "111111111111").unwrap()
    }

    #[test]
    fn a_reports_records_are_of_its_buckets_and_their_sum() {
        let plain = config(
            "<Id>a</Id><AccountLevel><BucketLevel/></AccountLevel><IsEnabled>true</IsEnabled>",
        );
        let mut classes = BTreeMap::new();
        classes.insert(
            "STANDARD".to_string(),
            Counts {
                current: 2,
                current_bytes: 10,
                noncurrent: 1,
                noncurrent_bytes: 4,
                delete_markers: 1,
                uploads: 1,
                upload_bytes: 6,
                ..Counts::default()
            },
        );
        let mut activity = [0u64; 11];
        activity[0] = 7;
        let tally = |name: &str| BucketTally {
            name: name.into(),
            classes: classes.clone(),
            settings: vec![("VersioningEnabledBucketCount", 1)],
            lifecycle_rules: vec![("TotalLifecycleRuleCount", 2)],
            replication_rules: vec![("SameAccountReplicationRuleCount", 1)],
            activity,
            ..BucketTally::default()
        };
        let all = records(&plain, &[tally("one"), tally("two")]);
        let find = |record_type: LensRecordType, bucket: &str, metric: &str| {
            all.iter()
                .find(|record| {
                    record.record_type == record_type
                        && record.bucket == bucket
                        && record.metric == metric
                })
                .map(|record| record.value)
        };
        // StorageBytes holds the uploads' bytes; ObjectCount the uploads.
        assert_eq!(
            find(LensRecordType::Bucket, "one", "StorageBytes"),
            Some(20)
        );
        assert_eq!(find(LensRecordType::Bucket, "one", "ObjectCount"), Some(4));
        assert_eq!(find(LensRecordType::Account, "", "StorageBytes"), Some(40));
        assert_eq!(
            find(LensRecordType::Account, "", "VersioningEnabledBucketCount"),
            Some(2)
        );
        // Advanced metrics only where the configuration turns them on.
        assert_eq!(find(LensRecordType::Bucket, "one", "AllRequests"), None);
        assert_eq!(
            find(LensRecordType::Bucket, "one", "TotalLifecycleRuleCount"),
            None
        );
        let advanced = config(
            "<Id>a</Id><AccountLevel><ActivityMetrics><IsEnabled>true</IsEnabled></ActivityMetrics><AdvancedCostOptimizationMetrics><IsEnabled>true</IsEnabled></AdvancedCostOptimizationMetrics><AdvancedDataProtectionMetrics><IsEnabled>true</IsEnabled></AdvancedDataProtectionMetrics><BucketLevel><ActivityMetrics><IsEnabled>true</IsEnabled></ActivityMetrics><AdvancedCostOptimizationMetrics><IsEnabled>true</IsEnabled></AdvancedCostOptimizationMetrics><AdvancedDataProtectionMetrics><IsEnabled>true</IsEnabled></AdvancedDataProtectionMetrics></BucketLevel></AccountLevel><IsEnabled>true</IsEnabled>",
        );
        let all = records(&advanced, &[tally("one"), tally("two")]);
        let find = |record_type: LensRecordType, metric: &str| {
            all.iter()
                .find(|record| record.record_type == record_type && record.metric == metric)
                .map(|record| record.value)
        };
        assert_eq!(find(LensRecordType::Bucket, "AllRequests"), Some(7));
        assert_eq!(find(LensRecordType::Account, "AllRequests"), Some(14));
        assert_eq!(
            find(LensRecordType::Account, "TotalLifecycleRuleCount"),
            Some(4)
        );
        assert_eq!(
            find(LensRecordType::Account, "SameAccountReplicationRuleCount"),
            Some(2)
        );
    }

    #[test]
    fn prefixes_are_counted_to_their_depth_and_kept_by_their_share() {
        let rule = PrefixRule {
            delimiter: "/".into(),
            max_depth: 2,
            min_percentage: 10.0,
        };
        assert_eq!(
            prefixes_of("a/b/c/d", &rule).collect::<Vec<_>>(),
            ["a/", "a/b/"]
        );
        assert!(prefixes_of("top", &rule).next().is_none());
        let mut tally = BucketTally::default();
        for (key, size) in [("logs/x", 95), ("tmp/y", 5)] {
            tally.count(key, "STANDARD", Some(&rule), |counts| {
                counts.current += 1;
                counts.current_bytes += size;
            });
        }
        tally.select_prefixes(&rule);
        assert_eq!(tally.prefixes.keys().collect::<Vec<_>>(), ["logs/"]);
    }

    #[test]
    fn a_scope_takes_the_buckets_and_regions_it_names() {
        let scoped = |scope: &str| {
            config(&format!(
                "<Id>a</Id><AccountLevel><BucketLevel/></AccountLevel>{scope}<IsEnabled>true</IsEnabled>"
            ))
        };
        let include = scoped("<Include><Buckets><Arn>arn:aws:s3:::data</Arn></Buckets></Include>");
        assert!(
            in_scope(&include, "data", "us-east-1") && !in_scope(&include, "other", "us-east-1")
        );
        let region = scoped("<Include><Regions><Region>us-east-1</Region></Regions></Include>");
        assert!(
            in_scope(&region, "other", "us-east-1") && !in_scope(&region, "other", "eu-west-1")
        );
        let exclude = scoped("<Exclude><Buckets><Arn>arn:aws:s3:::data</Arn></Buckets></Exclude>");
        assert!(
            !in_scope(&exclude, "data", "us-east-1") && in_scope(&exclude, "other", "us-east-1")
        );
    }
}
