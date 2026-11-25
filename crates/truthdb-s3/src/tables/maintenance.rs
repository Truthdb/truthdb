//! S3 Tables' table maintenance: its configurations (Put/Get
//! TableMaintenanceConfiguration, and the table bucket's), each job's status
//! (GetTableMaintenanceJobStatus), and the jobs run in the background on
//! every table:
//!
//! - snapshot management, expiring the snapshots its settings keep no
//!   longer, as a commit of the table (the files only they referenced then
//!   marked noncurrent at once);
//! - unreferenced file removal (the table bucket's): the objects of a
//!   table's storage no snapshot references, older than unreferencedDays,
//!   marked noncurrent, and deleted nonCurrentDays after.
//!
//! Compaction is not built: its configuration is refused.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::sync::watch;
use truthdb_object::{ListAfter, MaintenanceRecord, TableInfo, VersionSel};

use super::{TError, read, write};
use crate::server::S3State;

pub(super) const SNAPSHOT_MANAGEMENT: &str = "icebergSnapshotManagement";
pub(super) const COMPACTION: &str = "icebergCompaction";
pub(super) const UNREFERENCED_FILE_REMOVAL: &str = "icebergUnreferencedFileRemoval";

/// Tables a pass reads at a time.
const PAGE: usize = 100;
/// Objects of a table's storage read at a time.
const LISTED: usize = 1000;
/// The longest wait between passes (a table is run once its interval since
/// its last run has passed).
const MAX_WAIT: Duration = Duration::from_secs(60);
/// A manifest list or manifest is read whole, up to this.
const MAX_MANIFEST_BYTES: u64 = 256 << 20;
/// A deleted object's body stays for readers that resolved it this long.
const DROP_GRACE_MS: u64 = 15 * 60 * 1000;

/// AWS's message for a commit that lost to another.
const STALE: &str = "Failed to commit because of out of date metadata. Maintenance will be retried at the next available opportunity.";
const GC_DISABLED: &str = "Iceberg snapshot management and unreferenced file removal is not supported when the 'gc.enabled' table property is false.";

/// A type's settings and their defaults: "MinimumSnapshots (1 by default)
/// and MaximumSnapshotAge (120 hours by default)", "unreferencedDays (3
/// days by default) and nonCurrentDays (10 days by default)".
fn defaults(kind: &str) -> &'static [(&'static str, i64)] {
    match kind {
        SNAPSHOT_MANAGEMENT => &[("minSnapshotsToKeep", 1), ("maxSnapshotAgeHours", 120)],
        UNREFERENCED_FILE_REMOVAL => &[("unreferencedDays", 3), ("nonCurrentDays", 10)],
        _ => &[],
    }
}

/// A configuration of `kind`: as put, or the default ("enabled by
/// default").
pub(super) fn configuration(kind: &str, stored: Option<&[u8]>) -> Value {
    stored
        .and_then(|value| serde_json::from_slice(value).ok())
        .unwrap_or_else(|| {
            let settings: Map<String, Value> = defaults(kind)
                .iter()
                .map(|(field, value)| (field.to_string(), json!(value)))
                .collect();
            json!({ "status": "enabled", "settings": { kind: settings } })
        })
}

/// The `value` of a Put…MaintenanceConfiguration of `kind`, checked, its
/// settings left out filled with the defaults: as kept.
fn checked_value(kind: &str, input: &Map<String, Value>) -> Result<Value, TError> {
    if let Some(field) = input.keys().find(|field| *field != "value") {
        return Err(TError::bad(format!(
            "{field} is not a field of this request."
        )));
    }
    let value = input
        .get("value")
        .and_then(Value::as_object)
        .ok_or_else(|| TError::bad("value is required."))?;
    if let Some(field) = value
        .keys()
        .find(|field| !matches!(field.as_str(), "status" | "settings"))
    {
        return Err(TError::bad(format!("{field} is not a field of value.")));
    }
    let status = match value.get("status").and_then(Value::as_str) {
        Some(status @ ("enabled" | "disabled")) => status,
        None => "enabled",
        Some(_) => return Err(TError::bad("status is enabled or disabled.")),
    };
    let mut kept: Map<String, Value> = defaults(kind)
        .iter()
        .map(|(field, value)| (field.to_string(), json!(value)))
        .collect();
    if let Some(settings) = value.get("settings").filter(|settings| !settings.is_null()) {
        let settings = settings
            .as_object()
            .ok_or_else(|| TError::bad("settings is an object."))?;
        if let Some(other) = settings.keys().find(|key| *key != kind) {
            return Err(TError::bad(format!(
                "settings.{other} is not of the {kind} type."
            )));
        }
        if let Some(own) = settings.get(kind) {
            let own = own
                .as_object()
                .ok_or_else(|| TError::bad(format!("settings.{kind} is an object.")))?;
            for (field, value) in own {
                if !kept.contains_key(field) {
                    return Err(TError::bad(format!("{field} is not a {kind} setting.")));
                }
                // PositiveInteger: 1 to 2147483647.
                let positive = value
                    .as_i64()
                    .filter(|value| (1..=i64::from(i32::MAX)).contains(value))
                    .ok_or_else(|| TError::bad(format!("{field} is from 1 to 2147483647.")))?;
                kept.insert(field.clone(), json!(positive));
            }
        }
    }
    Ok(json!({ "status": status, "settings": { kind: kept } }))
}

/// A PutTableMaintenanceConfiguration's `value`, checked.
pub(super) fn put_value(kind: &str, input: &Map<String, Value>) -> Result<Value, TError> {
    match kind {
        SNAPSHOT_MANAGEMENT => checked_value(kind, input),
        COMPACTION => Err(TError::not_implemented("Table compaction")),
        _ => Err(TError::bad(
            "type is icebergCompaction or icebergSnapshotManagement.",
        )),
    }
}

/// A PutTableBucketMaintenanceConfiguration's `value`, checked.
pub(super) fn put_bucket_value(kind: &str, input: &Map<String, Value>) -> Result<Value, TError> {
    match kind {
        UNREFERENCED_FILE_REMOVAL => checked_value(kind, input),
        _ => Err(TError::bad("type is icebergUnreferencedFileRemoval.")),
    }
}

/// A job's status as kept: its status, when it last ran (milliseconds),
/// and why it failed.
fn status_record(status: &str, last_run_ms: Option<u64>, failure: Option<&str>) -> Value {
    let mut record = json!({ "status": status });
    if let Some(at) = last_run_ms {
        record["lastRunMs"] = json!(at);
    }
    if let Some(failure) = failure {
        record["failureMessage"] = json!(failure);
    }
    record
}

/// A job's status as GetTableMaintenanceJobStatus answers it (`Not_Yet_Run`
/// when it has not run).
pub(super) fn job_status(stored: Option<&[u8]>) -> Value {
    let record: Value = stored
        .and_then(|value| serde_json::from_slice(value).ok())
        .unwrap_or_else(|| status_record("Not_Yet_Run", None, None));
    let mut answer = json!({ "status": record["status"] });
    if let Some(at) = record.get("lastRunMs").and_then(Value::as_u64) {
        answer["lastRunTimestamp"] = json!(crate::time::iso8601(at));
    }
    if let Some(failure) = record.get("failureMessage") {
        answer["failureMessage"] = failure.clone();
    }
    answer
}

/// Runs the tables' maintenance until `shutdown` turns true (a standby's
/// tables are its primary's to maintain).
pub async fn run(state: Arc<S3State>, mut shutdown: watch::Receiver<bool>) {
    let scope = crate::scope::Scope {
        key: None,
        buckets: Vec::new(),
        origin: None,
    };
    loop {
        if *shutdown.borrow() {
            return;
        }
        if !state.storage.is_standby() {
            let pass = crate::scope::within(scope.clone(), pass(&state)).await;
            if let Err(err) = pass {
                eprintln!("table maintenance: {}", err.message);
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(state.table_maintenance.min(MAX_WAIT)) => {}
            _ = shutdown.changed() => {}
        }
    }
}

/// A maintenance job of a table.
#[derive(Clone, Copy)]
enum Job {
    /// Its own configuration's.
    Snapshots,
    /// Its table bucket's.
    Files,
}

impl Job {
    fn kind(self) -> &'static str {
        match self {
            Job::Snapshots => SNAPSHOT_MANAGEMENT,
            Job::Files => UNREFERENCED_FILE_REMOVAL,
        }
    }
}

/// One pass: every table's jobs that are due.
async fn pass(state: &S3State) -> Result<(), TError> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        let cursor = after.clone();
        let tables = read(state, move |file| file.all_tables(cursor.as_deref(), PAGE)).await?;
        let Some((last, ..)) = tables.last() else {
            return Ok(());
        };
        after = Some(last.clone());
        // One table's (or job's) failure stops no other's.
        for (_, account, bucket, table) in tables {
            for job in [Job::Snapshots, Job::Files] {
                if let Err(err) = maintain(state, (&account, &bucket), &table, job).await {
                    eprintln!("table maintenance of {}: {}", table.id, err.message);
                }
            }
        }
    }
}

/// Job `job` of `listed` (as the pass listed it), if it is due: run, and
/// its status kept.
async fn maintain(
    state: &S3State,
    (account, bucket): (&str, &str),
    listed: &TableInfo,
    job: Job,
) -> Result<(), TError> {
    let kind = job.kind();
    let (owner, looked, id) = (account.to_string(), bucket.to_string(), listed.id.clone());
    // The table as it is now (a job before this one may have committed
    // it since it was listed), with its configuration and status.
    let (table, stored, status) = read(state, move |file| {
        let at = (owner.as_str(), looked.as_str());
        let of = match job {
            Job::Snapshots => Some(id.as_str()),
            Job::Files => None,
        };
        Ok((
            file.table_by_id(at, &id)?,
            file.maintenance(at, of, (MaintenanceRecord::Configuration, kind))?,
            file.maintenance(at, Some(&id), (MaintenanceRecord::JobStatus, kind))?,
        ))
    })
    .await?;
    let Some(table) = table else {
        return Ok(());
    };
    let table = &table;
    let configuration = configuration(kind, stored.as_deref());
    let status: Option<Value> = status.and_then(|status| serde_json::from_slice(&status).ok());
    let now = crate::objects::now_ms();
    let record = if configuration["status"] != "enabled" {
        if status
            .as_ref()
            .is_some_and(|status| status["status"] == "Disabled")
        {
            return Ok(());
        }
        status_record("Disabled", None, None)
    } else {
        let last = status
            .as_ref()
            .filter(|status| status["status"] != "Disabled")
            .and_then(|status| status.get("lastRunMs"))
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let interval = state.table_maintenance.as_millis() as u64;
        // Not an Iceberg table yet, or run within its interval.
        if table.metadata_location.is_none() || (last != 0 && now < last + interval) {
            return Ok(());
        }
        let settings = &configuration["settings"][kind];
        let setting = |field: &str| {
            settings[field].as_i64().unwrap_or_else(|| {
                defaults(kind)
                    .iter()
                    .find(|(name, _)| *name == field)
                    .map_or(1, |(_, value)| *value)
            })
        };
        let outcome = match job {
            Job::Snapshots => {
                let keep = setting("minSnapshotsToKeep");
                let age_ms = setting("maxSnapshotAgeHours").saturating_mul(3_600_000);
                expire(state, (account, bucket), table, (keep, age_ms), now).await
            }
            Job::Files => {
                let day = state.table_maintenance_day.as_millis() as i64;
                let unreferenced = setting("unreferencedDays").saturating_mul(day) as u64;
                let noncurrent = setting("nonCurrentDays").saturating_mul(day) as u64;
                remove_unreferenced(
                    state,
                    (account, bucket),
                    table,
                    (unreferenced, noncurrent),
                    now,
                )
                .await
            }
        };
        match outcome {
            Ok(()) => status_record("Successful", Some(now), None),
            // Lost to another commit: retried on the next pass ("at the
            // next available opportunity"), its last run as it was.
            Err(failure) if failure == STALE => {
                status_record("Failed", (last != 0).then_some(last), Some(&failure))
            }
            Err(failure) => status_record("Failed", Some(now), Some(&failure)),
        }
    };
    let (owner, bucket, id) = (account.to_string(), bucket.to_string(), table.id.clone());
    let record = record.to_string();
    // A table deleted meanwhile has no status to keep.
    let _ = write(state, move |tx| {
        tx.put_maintenance(
            (&owner, &bucket),
            Some(&id),
            (MaintenanceRecord::JobStatus, kind),
            record.as_bytes(),
        )
    })
    .await;
    Ok(())
}

/// The property `key` of a table's metadata.
fn property<'a>(metadata: &'a Value, key: &str) -> Option<&'a Value> {
    metadata.get("properties")?.get(key)
}

/// The snapshots of `table` its settings no longer keep, expired: those
/// neither among the last `keep` of its main branch nor younger than
/// `age_ms` ("preserved only when both criteria are satisfied"); the
/// objects only they referenced then marked noncurrent ("immediately ...
/// regardless of its age"). Why not, if it fails.
async fn expire(
    state: &S3State,
    (account, bucket): (&str, &str),
    table: &TableInfo,
    (keep, age_ms): (i64, i64),
    now: u64,
) -> Result<(), String> {
    let base = super::iceberg::read_current(state, table)
        .await
        .map_err(|err| err.message)?;
    if property(&base, "gc.enabled").and_then(Value::as_str) == Some("false") {
        return Err(GC_DISABLED.to_string());
    }
    if property(&base, "history.expire.max-snapshot-age-ms").is_some()
        || property(&base, "history.expire.min-snapshots-to-keep").is_some()
    {
        return Err(
            "Snapshot management does not support retention set as Apache Iceberg table properties (history.expire.*)."
                .to_string(),
        );
    }
    let refs = base.get("refs").and_then(Value::as_object);
    if refs.is_some_and(|refs| refs.keys().any(|name| name != "main")) {
        return Err(
            "Snapshot management does not support tables with user-defined tags or branches."
                .to_string(),
        );
    }
    let Some(current) = base.get("current-snapshot-id").and_then(Value::as_i64) else {
        return Ok(());
    };
    let snapshots = base
        .get("snapshots")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let parent_of = |id: i64| {
        snapshots
            .iter()
            .find(|snapshot| snapshot["snapshot-id"].as_i64() == Some(id))
            .and_then(|snapshot| snapshot["parent-snapshot-id"].as_i64())
    };
    // The last `keep` of the main branch: the current, and its ancestors.
    let mut kept = Vec::new();
    let mut at = Some(current);
    while let Some(id) = at {
        if kept.len() as i64 >= keep || kept.contains(&id) {
            break;
        }
        kept.push(id);
        at = parent_of(id);
    }
    let oldest = now as i64 - age_ms;
    let (expired, _): (Vec<Value>, Vec<Value>) = snapshots.iter().cloned().partition(|snapshot| {
        let id = snapshot["snapshot-id"].as_i64();
        id.is_some_and(|id| id != current && !kept.contains(&id))
            && snapshot["timestamp-ms"]
                .as_i64()
                .is_some_and(|at| at < oldest)
    });
    if expired.is_empty() {
        return Ok(());
    }
    let ids: Vec<i64> = expired
        .iter()
        .filter_map(|snapshot| snapshot["snapshot-id"].as_i64())
        .collect();
    let location = table
        .metadata_location
        .clone()
        .expect("read: it has a metadata file");
    // As a commit: only from the table as read (its main branch where it
    // has one; the version token swapped holds it either way).
    let mut requirements = vec![json!({"type": "assert-table-uuid", "uuid": base["table-uuid"]})];
    if refs.is_some_and(|refs| refs.contains_key("main")) {
        requirements
            .push(json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": current}));
    }
    let updates = [json!({"action": "remove-snapshots", "snapshot-ids": ids})];
    let next = match crate::iceberg_commit::apply(&base, &location, &requirements, &updates, now) {
        Ok(Some(next)) => next,
        Ok(None) => return Ok(()),
        Err(crate::iceberg_commit::CommitError::Failed(_)) => return Err(STALE.to_string()),
        Err(crate::iceberg_commit::CommitError::Invalid(message)) => return Err(message),
    };
    let at = (table.namespace.clone(), table.name.clone());
    let next_location = match super::iceberg::write_next(
        state,
        (account, bucket),
        account,
        (table, &at),
        (&location, &next),
        now,
    )
    .await
    {
        Ok(next_location) => next_location,
        Err(err) if err.status == 409 => return Err(STALE.to_string()),
        Err(err) => return Err(err.message),
    };
    // The objects only the expired snapshots referenced: noncurrent now.
    // (Unmarked, they are still unreferenced file removal's to find.)
    let gone = references(state, table, None, &expired, true).await?;
    let next_metadata = (next_location.as_str(), &next);
    let kept = references(
        state,
        table,
        Some(next_metadata),
        next["snapshots"].as_array().map_or(&[], Vec::as_slice),
        false,
    )
    .await;
    // What is kept not known (a list outside its storage): nothing marked.
    let Ok(kept) = kept else {
        return Ok(());
    };
    let marked: Vec<String> = gone.difference(&kept).cloned().collect();
    mark(state, (account, bucket), table, &marked, now).await
}

/// The keys of `table`'s storage `snapshots` reference (their manifest
/// lists, those lists' manifests, those manifests' data and delete files),
/// and, with `metadata` (a metadata file's location and contents), that
/// file, the files of its metadata log and its statistics files. Files
/// outside the table's storage are not its own, and not read. `tolerant`:
/// a file that cannot be read (an expired snapshot's, gone already) names
/// none, rather than failing (what is still referenced is never guessed).
async fn references(
    state: &S3State,
    table: &TableInfo,
    metadata: Option<(&str, &Value)>,
    snapshots: &[Value],
    tolerant: bool,
) -> Result<HashSet<String>, String> {
    let prefix = format!("{}/", table.warehouse_location);
    // `s3a://` and `s3n://` name the same objects as `s3://`.
    let key_of = |location: &str| {
        let location = ["s3a://", "s3n://"]
            .iter()
            .find_map(|scheme| location.strip_prefix(scheme))
            .map_or_else(|| location.to_string(), |rest| format!("s3://{rest}"));
        location.strip_prefix(&prefix).map(str::to_string)
    };
    // A list or manifest not read hides the files it names: outside the
    // table's storage (or of no form known) it fails the job, unless what
    // is sought is files no longer referenced (`tolerant`: fewer found).
    let unread = |what: String| -> Result<(), String> {
        if tolerant {
            Ok(())
        } else {
            Err(format!(
                "{what}: what it names is not known, so no file is removed."
            ))
        }
    };
    let mut keys = HashSet::new();
    if let Some((location, metadata)) = metadata {
        keys.extend(key_of(location));
        let files = |field: &str, name: &str| {
            metadata[field]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|entry| entry[name].as_str().and_then(key_of))
                .collect::<Vec<_>>()
        };
        keys.extend(files("metadata-log", "metadata-file"));
        keys.extend(files("statistics", "statistics-path"));
        keys.extend(files("partition-statistics", "statistics-path"));
    }
    let mut manifests = HashSet::new();
    for snapshot in snapshots {
        // Its manifest list's manifests, or (format version 1) its own.
        let listed = match (
            snapshot["manifest-list"].as_str(),
            snapshot["manifests"].as_array(),
        ) {
            (Some(list), _) => {
                let Some(list_key) = key_of(list) else {
                    unread(format!(
                        "The manifest list {list} is outside the table's storage"
                    ))?;
                    continue;
                };
                keys.insert(list_key);
                let named = stored(state, list)
                    .await
                    .and_then(|bytes| crate::iceberg_manifests::manifest_list(&bytes));
                match named {
                    Ok(listed) => listed,
                    Err(_) if tolerant => continue,
                    Err(err) => return Err(err),
                }
            }
            (None, Some(inline)) => inline
                .iter()
                .filter_map(|manifest| manifest.as_str().map(str::to_string))
                .collect(),
            (None, None) => {
                unread(format!(
                    "The snapshot {} has no manifest list",
                    snapshot["snapshot-id"]
                ))?;
                continue;
            }
        };
        for manifest in listed {
            let Some(manifest_key) = key_of(&manifest) else {
                unread(format!(
                    "The manifest {manifest} is outside the table's storage"
                ))?;
                continue;
            };
            if manifests.insert(manifest_key.clone()) {
                keys.insert(manifest_key);
                let files = stored(state, &manifest)
                    .await
                    .and_then(|bytes| crate::iceberg_manifests::manifest_files(&bytes));
                match files {
                    Ok(files) => keys.extend(files.iter().filter_map(|file| key_of(file))),
                    Err(_) if tolerant => {}
                    Err(err) => return Err(err),
                }
            }
        }
    }
    Ok(keys)
}

/// The bytes of the table's file at `location`.
async fn stored(state: &S3State, location: &str) -> Result<Vec<u8>, String> {
    let location = location.to_string();
    read(state, move |file| {
        super::iceberg::stored_file(file, &location, MAX_MANIFEST_BYTES)
    })
    .await
    .map_err(|err| err.message)?
}

/// Marks the objects `keys` of `table`'s storage noncurrent at `now` (those
/// marked already keep their time).
async fn mark(
    state: &S3State,
    (account, bucket): (&str, &str),
    table: &TableInfo,
    keys: &[String],
    now: u64,
) -> Result<(), String> {
    for key in keys {
        let (owner, bucket, id, key) = (
            account.to_string(),
            bucket.to_string(),
            table.id.clone(),
            key.clone(),
        );
        let (looked, place, of, named) = (owner.clone(), bucket.clone(), id.clone(), key.clone());
        let known = read(state, move |file| {
            file.maintenance(
                (&looked, &place),
                Some(&of),
                (MaintenanceRecord::Noncurrent, &named),
            )
        })
        .await
        .map_err(|err| err.message)?;
        if known.is_some() {
            continue;
        }
        write(state, move |tx| {
            tx.put_maintenance(
                (&owner, &bucket),
                Some(&id),
                (MaintenanceRecord::Noncurrent, &key),
                now.to_string().as_bytes(),
            )
        })
        .await
        .map_err(|err| err.message)?;
    }
    Ok(())
}

/// Unreferenced file removal of `table`: the objects of its storage no
/// snapshot (nor its metadata) references, older than `unreferenced_ms`,
/// marked noncurrent; those marked `noncurrent_ms` ago deleted for good
/// ("Deletes of noncurrent objects are permanent"); a marked one referenced
/// again, or written again since it was marked, unmarked.
async fn remove_unreferenced(
    state: &S3State,
    (account, bucket): (&str, &str),
    table: &TableInfo,
    (unreferenced_ms, noncurrent_ms): (u64, u64),
    now: u64,
) -> Result<(), String> {
    let location = table
        .metadata_location
        .clone()
        .expect("run only with a metadata file");
    let metadata = super::iceberg::read_current(state, table)
        .await
        .map_err(|err| err.message)?;
    if property(&metadata, "gc.enabled").and_then(Value::as_str) == Some("false") {
        return Err(GC_DISABLED.to_string());
    }
    let snapshots = metadata["snapshots"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let referenced = references(
        state,
        table,
        Some((&location, &metadata)),
        &snapshots,
        false,
    )
    .await?;
    let (owner, looked, id) = (account.to_string(), bucket.to_string(), table.id.clone());
    let mut marked: BTreeMap<String, u64> = read(state, move |file| {
        file.maintenance_records((&owner, &looked), Some(&id), MaintenanceRecord::Noncurrent)
    })
    .await
    .map_err(|err| err.message)?
    .into_iter()
    .map(|(key, value)| {
        let at = std::str::from_utf8(&value)
            .ok()
            .and_then(|at| at.parse().ok())
            .unwrap_or(now);
        (key, at)
    })
    .collect();
    let warehouse = truthdb_object::warehouse_bucket(&table.id);
    let mut after: Option<String> = None;
    let mut to_mark = Vec::new();
    loop {
        let (listed, cursor) = (warehouse.clone(), after.clone());
        let page = read(state, move |file| {
            file.list_objects(
                &listed,
                "",
                None,
                cursor.as_deref().map(ListAfter::Key),
                LISTED,
            )
        })
        .await
        .map_err(|err| err.message)?
        .map_err(|err| format!("The table's storage cannot be listed: {err:?}"))?;
        for object in &page.objects {
            let key = &object.key;
            match marked.remove(key) {
                // Referenced again, or written again since: current.
                Some(at) if referenced.contains(key) || object.info.mtime_ms > at => {
                    unmark(state, (account, bucket), table, key).await?;
                }
                Some(at) if now >= at.saturating_add(noncurrent_ms) => {
                    delete(
                        state,
                        (account, bucket),
                        table,
                        (key, object.info.version),
                        now,
                    )
                    .await?;
                }
                Some(_) => {}
                None if !referenced.contains(key)
                    && now >= object.info.mtime_ms.saturating_add(unreferenced_ms) =>
                {
                    to_mark.push(key.clone());
                }
                None => {}
            }
        }
        match (page.truncated, page.objects.last()) {
            (true, Some(last)) => after = Some(last.key.clone()),
            _ => break,
        }
    }
    // Marks of objects no longer there.
    for key in marked.keys() {
        unmark(state, (account, bucket), table, key).await?;
    }
    mark(state, (account, bucket), table, &to_mark, now).await
}

async fn unmark(
    state: &S3State,
    (account, bucket): (&str, &str),
    table: &TableInfo,
    key: &str,
) -> Result<(), String> {
    let (owner, bucket, id, key) = (
        account.to_string(),
        bucket.to_string(),
        table.id.clone(),
        key.to_string(),
    );
    write(state, move |tx| {
        tx.delete_maintenance_record(
            (&owner, &bucket),
            Some(&id),
            (MaintenanceRecord::Noncurrent, &key),
        )
        .map(Ok)
    })
    .await
    .map_err(|err| err.message)
}

/// Deletes noncurrent object `key` (its version `version`) of `table`'s
/// storage for good, and its mark, if the table is still as it was read
/// (a commit since may reference it again: then the pass stops, retried
/// on the next).
async fn delete(
    state: &S3State,
    (account, bucket): (&str, &str),
    table: &TableInfo,
    (key, version): (&str, u64),
    now: u64,
) -> Result<(), String> {
    let (owner, bucket, id, key) = (
        account.to_string(),
        bucket.to_string(),
        table.id.clone(),
        key.to_string(),
    );
    let token = table.version_token.clone();
    write(state, move |tx| {
        if tx.table_version((&owner, &bucket), &id)?.as_deref() != Some(token.as_str()) {
            return Ok(Err(truthdb_object::TableError::VersionMismatch));
        }
        // Gone since (or written again: its version another) is no error.
        let _ = tx.delete_object(
            &truthdb_object::warehouse_bucket(&id),
            &key,
            Some(VersionSel::Seq(version)),
            true,
            now,
            DROP_GRACE_MS,
        )?;
        tx.delete_maintenance_record(
            (&owner, &bucket),
            Some(&id),
            (MaintenanceRecord::Noncurrent, &key),
        )
        .map(Ok)
    })
    .await
    .map_err(|err| {
        if err.status == 409 {
            STALE.to_string()
        } else {
            err.message
        }
    })
}
