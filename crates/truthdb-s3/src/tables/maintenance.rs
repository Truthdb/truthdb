//! S3 Tables' table maintenance: its configurations (Put/Get
//! TableMaintenanceConfiguration, and the table bucket's), each job's status
//! (GetTableMaintenanceJobStatus), and the job run in the background on
//! every table: snapshot management, expiring the snapshots its settings
//! keep no longer, as a commit of the table. Compaction and unreferenced file
//! removal are not built: their configurations are refused.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::sync::watch;
use truthdb_object::{MaintenanceRecord, TableInfo};

use super::{TError, read, write};
use crate::server::S3State;

pub(super) const SNAPSHOT_MANAGEMENT: &str = "icebergSnapshotManagement";
pub(super) const COMPACTION: &str = "icebergCompaction";
pub(super) const UNREFERENCED_FILE_REMOVAL: &str = "icebergUnreferencedFileRemoval";

/// "MinimumSnapshots (1 by default) and MaximumSnapshotAge (120 hours by
/// default)".
const MIN_SNAPSHOTS_TO_KEEP: i64 = 1;
const MAX_SNAPSHOT_AGE_HOURS: i64 = 120;
/// Tables a pass reads at a time.
const PAGE: usize = 100;
/// The longest wait between passes (a table is run once its interval since
/// its last run has passed).
const MAX_WAIT: Duration = Duration::from_secs(60);

/// AWS's message for a commit that lost to another.
const STALE: &str = "Failed to commit because of out of date metadata. Maintenance will be retried at the next available opportunity.";

/// A table's snapshot management configuration: as put, or the default
/// ("enabled by default").
pub(super) fn snapshot_configuration(stored: Option<&[u8]>) -> Value {
    stored
        .and_then(|value| serde_json::from_slice(value).ok())
        .unwrap_or_else(|| {
            json!({
                "status": "enabled",
                "settings": { SNAPSHOT_MANAGEMENT: {
                    "minSnapshotsToKeep": MIN_SNAPSHOTS_TO_KEEP,
                    "maxSnapshotAgeHours": MAX_SNAPSHOT_AGE_HOURS,
                } },
            })
        })
}

/// The `value` of a PutTableMaintenanceConfiguration of `kind`, checked, its
/// settings left out filled with the defaults: as kept.
pub(super) fn put_value(kind: &str, input: &Map<String, Value>) -> Result<Value, TError> {
    match kind {
        SNAPSHOT_MANAGEMENT => {}
        COMPACTION => return Err(TError::not_implemented("Table compaction")),
        _ => {
            return Err(TError::bad(
                "type is icebergCompaction or icebergSnapshotManagement.",
            ));
        }
    }
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
    let mut kept = Map::new();
    kept.insert(
        "minSnapshotsToKeep".to_string(),
        json!(MIN_SNAPSHOTS_TO_KEEP),
    );
    kept.insert(
        "maxSnapshotAgeHours".to_string(),
        json!(MAX_SNAPSHOT_AGE_HOURS),
    );
    if let Some(settings) = value.get("settings").filter(|settings| !settings.is_null()) {
        let settings = settings
            .as_object()
            .ok_or_else(|| TError::bad("settings is an object."))?;
        if let Some(other) = settings.keys().find(|key| *key != SNAPSHOT_MANAGEMENT) {
            return Err(TError::bad(format!(
                "settings.{other} is not of the icebergSnapshotManagement type."
            )));
        }
        if let Some(own) = settings.get(SNAPSHOT_MANAGEMENT) {
            let own = own
                .as_object()
                .ok_or_else(|| TError::bad("settings.icebergSnapshotManagement is an object."))?;
            for (field, value) in own {
                if !matches!(field.as_str(), "minSnapshotsToKeep" | "maxSnapshotAgeHours") {
                    return Err(TError::bad(format!(
                        "{field} is not a snapshot management setting."
                    )));
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
    Ok(json!({ "status": status, "settings": { SNAPSHOT_MANAGEMENT: kept } }))
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

/// One pass: every table whose snapshot management is due.
async fn pass(state: &S3State) -> Result<(), TError> {
    let mut after: Option<Vec<u8>> = None;
    loop {
        let cursor = after.clone();
        let tables = read(state, move |file| file.all_tables(cursor.as_deref(), PAGE)).await?;
        let Some((last, ..)) = tables.last() else {
            return Ok(());
        };
        after = Some(last.clone());
        for (_, account, bucket, table) in tables {
            manage_snapshots(state, (&account, &bucket), &table).await?;
        }
    }
}

/// Snapshot management of `table`, if it is due: run, and its status kept.
async fn manage_snapshots(
    state: &S3State,
    (account, bucket): (&str, &str),
    table: &TableInfo,
) -> Result<(), TError> {
    let (owner, looked, id) = (account.to_string(), bucket.to_string(), table.id.clone());
    let (configuration, status) = read(state, move |file| {
        let at = (owner.as_str(), looked.as_str());
        Ok((
            file.maintenance(
                at,
                Some(&id),
                (MaintenanceRecord::Configuration, SNAPSHOT_MANAGEMENT),
            )?,
            file.maintenance(
                at,
                Some(&id),
                (MaintenanceRecord::JobStatus, SNAPSHOT_MANAGEMENT),
            )?,
        ))
    })
    .await?;
    let configuration = snapshot_configuration(configuration.as_deref());
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
        let settings = &configuration["settings"][SNAPSHOT_MANAGEMENT];
        let keep = settings["minSnapshotsToKeep"]
            .as_i64()
            .unwrap_or(MIN_SNAPSHOTS_TO_KEEP);
        let age_ms = settings["maxSnapshotAgeHours"]
            .as_i64()
            .unwrap_or(MAX_SNAPSHOT_AGE_HOURS)
            .saturating_mul(3_600_000);
        match expire(state, (account, bucket), table, (keep, age_ms), now).await {
            Ok(()) => status_record("Successful", Some(now), None),
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
            (MaintenanceRecord::JobStatus, SNAPSHOT_MANAGEMENT),
            record.as_bytes(),
        )
    })
    .await;
    Ok(())
}

/// The snapshots of `table` its settings no longer keep, expired: those
/// neither among the last `keep` of its main branch nor younger than
/// `age_ms` ("preserved only when both criteria are satisfied"). Why not,
/// if it fails.
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
    let properties = base.get("properties").and_then(Value::as_object);
    let property = |key: &str| properties.and_then(|properties| properties.get(key));
    if property("gc.enabled").and_then(Value::as_str) == Some("false") {
        return Err("Iceberg snapshot management and unreferenced file removal is not supported when the 'gc.enabled' table property is false.".to_string());
    }
    if property("history.expire.max-snapshot-age-ms").is_some()
        || property("history.expire.min-snapshots-to-keep").is_some()
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
    let expired: Vec<i64> = snapshots
        .iter()
        .filter(|snapshot| {
            let id = snapshot["snapshot-id"].as_i64();
            id.is_some_and(|id| id != current && !kept.contains(&id))
                && snapshot["timestamp-ms"]
                    .as_i64()
                    .is_some_and(|at| at < oldest)
        })
        .filter_map(|snapshot| snapshot["snapshot-id"].as_i64())
        .collect();
    if expired.is_empty() {
        return Ok(());
    }
    let location = table
        .metadata_location
        .clone()
        .expect("read: it has a metadata file");
    // As a commit: only from the table as read.
    let requirements = [
        json!({"type": "assert-table-uuid", "uuid": base["table-uuid"]}),
        json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": current}),
    ];
    let updates = [json!({"action": "remove-snapshots", "snapshot-ids": expired})];
    let next = match crate::iceberg_commit::apply(&base, &location, &requirements, &updates, now) {
        Ok(Some(next)) => next,
        Ok(None) => return Ok(()),
        Err(crate::iceberg_commit::CommitError::Failed(_)) => return Err(STALE.to_string()),
        Err(crate::iceberg_commit::CommitError::Invalid(message)) => return Err(message),
    };
    let at = (table.namespace.clone(), table.name.clone());
    match super::iceberg::write_next(
        state,
        (account, bucket),
        account,
        (table, &at),
        (&location, &next),
        now,
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(err) if err.status == 409 => Err(STALE.to_string()),
        Err(err) => Err(err.message),
    }
}
