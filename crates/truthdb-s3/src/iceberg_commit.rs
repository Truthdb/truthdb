//! An Iceberg table's commit (the REST catalog's `updateTable`): its
//! requirements checked against the table's current metadata, then its
//! updates applied, as Iceberg's reference implementations do (PyIceberg's
//! `update_table_metadata`): the table's next metadata.

use serde_json::{Map, Value, json};

/// Why a commit is not made.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CommitError {
    /// A requirement does not hold: the table changed (409, the client may
    /// retry).
    Failed(String),
    /// The commit is not one (400).
    Invalid(String),
}

fn invalid(message: impl Into<String>) -> CommitError {
    CommitError::Invalid(message.into())
}

fn failed(message: impl Into<String>) -> CommitError {
    CommitError::Failed(message.into())
}

/// The format version supported.
const FORMAT_VERSION: i64 = 2;
/// Metadata log entries kept when the table's properties say none.
const PREVIOUS_VERSIONS_MAX: i64 = 100;

/// The table's next metadata: `base` (at `location`) with `requirements`
/// checked and `updates` applied at `now_ms`; `None` when nothing changes.
pub(crate) fn apply(
    base: &Value,
    location: &str,
    requirements: &[Value],
    updates: &[Value],
    now_ms: u64,
) -> Result<Option<Value>, CommitError> {
    let base = base
        .as_object()
        .ok_or_else(|| invalid("The table's metadata is not an object."))?;
    for requirement in requirements {
        check(base, requirement)?;
    }
    let mut table = base.clone();
    let mut added = Added::default();
    for update in updates {
        apply_update(&mut table, update, &mut added)?;
    }
    if !added.changed {
        return Ok(None);
    }
    // The file it replaces, in its log.
    let kept = property(&table, "write.metadata.previous-versions-max")
        .and_then(|max| max.parse::<i64>().ok())
        .unwrap_or(PREVIOUS_VERSIONS_MAX)
        .max(1) as usize;
    let mut log = list(base, "metadata-log");
    if log.len() >= kept {
        log.drain(..log.len() + 1 - kept);
    }
    log.push(json!({
        "metadata-file": location,
        "timestamp-ms": int(base, "last-updated-ms").unwrap_or_default(),
    }));
    table.insert("metadata-log".to_string(), Value::Array(log));
    if table.get("last-updated-ms") == base.get("last-updated-ms") {
        table.insert("last-updated-ms".to_string(), json!(now_ms));
    }
    // Never before an entry of its logs (Iceberg's Java reader refuses it): a
    // snapshot's client clock behind the last commit's is not taken.
    let latest = ["metadata-log", "snapshot-log"]
        .iter()
        .flat_map(|field| list(&table, field))
        .filter_map(|entry| entry.get("timestamp-ms").and_then(Value::as_i64))
        .max()
        .unwrap_or_default();
    if int(&table, "last-updated-ms").unwrap_or_default() < latest {
        table.insert(
            "last-updated-ms".to_string(),
            json!(latest.max(now_ms as i64)),
        );
    }
    // The default spec and sort order apply to the current schema.
    let schema = by_id(
        &table,
        "schemas",
        "schema-id",
        int(&table, "current-schema-id"),
    )
    .ok_or_else(|| invalid("The current schema does not exist."))?;
    let spec = by_id(
        &table,
        "partition-specs",
        "spec-id",
        int(&table, "default-spec-id"),
    )
    .ok_or_else(|| invalid("The default partition spec does not exist."))?;
    let order = by_id(
        &table,
        "sort-orders",
        "order-id",
        int(&table, "default-sort-order-id"),
    )
    .ok_or_else(|| invalid("The default sort order does not exist."))?;
    crate::iceberg_metadata::check_spec(schema, spec).map_err(CommitError::Invalid)?;
    crate::iceberg_metadata::check_order(schema, order).map_err(CommitError::Invalid)?;
    // Each spec and order added applies to the current schema too.
    for id in &added.specs {
        if let Some(spec) = by_id(&table, "partition-specs", "spec-id", Some(*id)) {
            crate::iceberg_metadata::check_spec(schema, spec).map_err(CommitError::Invalid)?;
        }
    }
    for id in &added.orders {
        if let Some(order) = by_id(&table, "sort-orders", "order-id", Some(*id)) {
            crate::iceberg_metadata::check_order(schema, order).map_err(CommitError::Invalid)?;
        }
    }
    Ok(Some(Value::Object(table)))
}

/// What this commit's updates added (the `-1` of a "set" names the last),
/// and whether any changed the table.
#[derive(Default)]
struct Added {
    snapshots: Vec<i64>,
    schemas: Vec<i64>,
    specs: Vec<i64>,
    orders: Vec<i64>,
    changed: bool,
}

fn int(map: &Map<String, Value>, field: &str) -> Option<i64> {
    map.get(field).and_then(Value::as_i64)
}

fn list(map: &Map<String, Value>, field: &str) -> Vec<Value> {
    map.get(field)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn property<'a>(table: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    table.get("properties")?.get(key)?.as_str()
}

/// The element of list `field` whose `id_field` is `id`.
fn by_id<'a>(
    table: &'a Map<String, Value>,
    field: &str,
    id_field: &str,
    id: Option<i64>,
) -> Option<&'a Value> {
    let id = id?;
    table
        .get(field)?
        .as_array()?
        .iter()
        .find(|item| item.get(id_field).and_then(Value::as_i64) == Some(id))
}

fn object<'a>(value: &'a Value, what: &str) -> Result<&'a Map<String, Value>, CommitError> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{what} is an object.")))
}

fn number(map: &Map<String, Value>, field: &str, what: &str) -> Result<i64, CommitError> {
    int(map, field).ok_or_else(|| invalid(format!("{what}.{field} is a whole number.")))
}

fn text<'a>(map: &'a Map<String, Value>, field: &str, what: &str) -> Result<&'a str, CommitError> {
    map.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{what}.{field} is a string.")))
}

/// Whether `text` is a UUID (8-4-4-4-12 hexadecimal digits).
fn is_uuid(text: &str) -> bool {
    text.len() == 36
        && text.char_indices().all(|(at, c)| {
            if matches!(at, 8 | 13 | 18 | 23) {
                c == '-'
            } else {
                c.is_ascii_hexdigit()
            }
        })
}

/// A statistics file (`partition`: a partition statistics file), with the
/// fields Iceberg's readers require.
fn check_statistics(file: &Value, partition: bool, what: &str) -> Result<i64, CommitError> {
    let map = object(file, what)?;
    let id = number(map, "snapshot-id", what)?;
    text(map, "statistics-path", what)?;
    number(map, "file-size-in-bytes", what)?;
    if partition {
        return Ok(id);
    }
    number(map, "file-footer-size-in-bytes", what)?;
    if map
        .get("key-metadata")
        .is_some_and(|key| !key.is_null() && !key.is_string())
    {
        return Err(invalid(format!("{what}.key-metadata is a string.")));
    }
    let blobs = map
        .get("blob-metadata")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid(format!("{what}.blob-metadata is a list.")))?;
    for blob in blobs {
        let what = &format!("{what}.blob-metadata[]");
        let blob = object(blob, what)?;
        if !matches!(
            blob.get("type").and_then(Value::as_str),
            Some("apache-datasketches-theta-v1" | "deletion-vector-v1")
        ) {
            return Err(invalid(format!("{what}.type is a blob type.")));
        }
        number(blob, "snapshot-id", what)?;
        number(blob, "sequence-number", what)?;
        ids(blob, "fields", what)?;
        if blob.get("properties").is_some_and(|properties| {
            !properties.is_null()
                && !properties
                    .as_object()
                    .is_some_and(|map| map.values().all(Value::is_string))
        }) {
            return Err(invalid(format!("{what}.properties are strings.")));
        }
    }
    Ok(id)
}

/// A requirement, against the table's current metadata.
fn check(table: &Map<String, Value>, requirement: &Value) -> Result<(), CommitError> {
    let what = "requirement";
    let map = object(requirement, what)?;
    let kind = text(map, "type", what)?;
    let matches = |field: &str, own: &str| -> Result<(), CommitError> {
        let wanted = map.get(field).filter(|value| !value.is_null());
        if wanted == table.get(own).filter(|value| !value.is_null()) {
            Ok(())
        } else {
            Err(failed(format!(
                "Requirement failed: {own} has changed: expected {}, found {}",
                wanted.unwrap_or(&Value::Null),
                table.get(own).unwrap_or(&Value::Null)
            )))
        }
    };
    match kind {
        // The table exists.
        "assert-create" => Err(failed("Table already exists")),
        "assert-table-uuid" => {
            let uuid = text(map, "uuid", what)?;
            let own = table
                .get("table-uuid")
                .and_then(Value::as_str)
                .unwrap_or("");
            if uuid.eq_ignore_ascii_case(own) {
                Ok(())
            } else {
                Err(failed(format!(
                    "Table UUID does not match: {uuid} != {own}"
                )))
            }
        }
        "assert-ref-snapshot-id" => {
            let name = text(map, "ref", what)?;
            let wanted = map.get("snapshot-id").and_then(Value::as_i64);
            let snapshots = list(table, "snapshots");
            if snapshots.is_empty() && name != "main" {
                return Err(failed(
                    "Requirement failed: Table has no snapshots and can only be written to the main BRANCH.",
                ));
            }
            let current = table
                .get("refs")
                .and_then(|refs| refs.get(name))
                .and_then(|reference| reference.get("snapshot-id"))
                .and_then(Value::as_i64);
            match (wanted, current) {
                (None, None) => Ok(()),
                (Some(wanted), Some(current)) if wanted == current => Ok(()),
                (None, Some(_)) => Err(failed(format!(
                    "Requirement failed: {name} was created concurrently"
                ))),
                (Some(wanted), Some(current)) => Err(failed(format!(
                    "Requirement failed: {name} has changed: expected id {wanted}, found {current}"
                ))),
                (Some(wanted), None) => Err(failed(format!(
                    "Requirement failed: branch or tag {name} is missing, expected {wanted}"
                ))),
            }
        }
        "assert-last-assigned-field-id" => matches("last-assigned-field-id", "last-column-id"),
        "assert-current-schema-id" => matches("current-schema-id", "current-schema-id"),
        "assert-last-assigned-partition-id" => {
            matches("last-assigned-partition-id", "last-partition-id")
        }
        "assert-default-spec-id" => matches("default-spec-id", "default-spec-id"),
        "assert-default-sort-order-id" => matches("default-sort-order-id", "default-sort-order-id"),
        _ => Err(invalid(format!("{kind} is not a table requirement."))),
    }
}

/// The id a "set" names: its own, or (`-1`) the last added in this commit.
fn chosen(id: i64, added: &[i64], what: &str) -> Result<i64, CommitError> {
    if id != -1 {
        return Ok(id);
    }
    added.last().copied().ok_or_else(|| {
        invalid(format!(
            "Cannot set the current {what} to the last added one when none has been added"
        ))
    })
}

/// Adds `item` (of id `id`) to list `field`, unless one of its id is there
/// already: the same one is not added twice, another is refused.
fn add_unique(
    table: &mut Map<String, Value>,
    field: &str,
    id_field: &str,
    item: &Value,
    what: &str,
) -> Result<i64, CommitError> {
    let id = number(object(item, what)?, id_field, what)?;
    let mut items = list(table, field);
    match items
        .iter()
        .find(|existing| existing.get(id_field).and_then(Value::as_i64) == Some(id))
    {
        Some(existing) if existing == item => {}
        Some(_) => return Err(invalid(format!("A {what} with id {id} already exists."))),
        None => items.push(item.clone()),
    }
    table.insert(field.to_string(), Value::Array(items));
    Ok(id)
}

/// Removes the elements of list `field` whose `id_field` is in `ids`,
/// each of which must be there.
fn remove_ids(
    table: &mut Map<String, Value>,
    field: &str,
    id_field: &str,
    ids: &[i64],
    what: &str,
) -> Result<(), CommitError> {
    let mut items = list(table, field);
    for id in ids {
        if !items
            .iter()
            .any(|item| item.get(id_field).and_then(Value::as_i64) == Some(*id))
        {
            return Err(invalid(format!("The {what} {id} does not exist.")));
        }
    }
    items.retain(|item| {
        !item
            .get(id_field)
            .and_then(Value::as_i64)
            .is_some_and(|id| ids.contains(&id))
    });
    table.insert(field.to_string(), Value::Array(items));
    Ok(())
}

fn ids(map: &Map<String, Value>, field: &str, what: &str) -> Result<Vec<i64>, CommitError> {
    map.get(field)
        .and_then(Value::as_array)
        .and_then(|items| items.iter().map(Value::as_i64).collect::<Option<Vec<_>>>())
        .ok_or_else(|| invalid(format!("{what}.{field} is a list of whole numbers.")))
}

/// Drops ref `name`; the main branch's makes the table have no current
/// snapshot.
fn drop_ref(table: &mut Map<String, Value>, name: &str) {
    if let Some(refs) = table.get_mut("refs").and_then(Value::as_object_mut) {
        refs.remove(name);
    }
    if name == "main" {
        table.remove("current-snapshot-id");
    }
}

fn apply_update(
    table: &mut Map<String, Value>,
    update: &Value,
    added: &mut Added,
) -> Result<(), CommitError> {
    let what = "update";
    let map = object(update, what)?;
    let action = text(map, "action", what)?;
    let what = &format!("update {action}");
    match action {
        "assign-uuid" => {
            let uuid = text(map, "uuid", what)?;
            if !is_uuid(uuid) {
                return Err(invalid(format!("{what}.uuid is a UUID.")));
            }
            if table.get("table-uuid").and_then(Value::as_str) != Some(uuid) {
                table.insert("table-uuid".to_string(), json!(uuid));
                added.changed = true;
            }
        }
        "upgrade-format-version" => {
            let version = number(map, "format-version", what)?;
            let own = int(table, "format-version").unwrap_or(FORMAT_VERSION);
            if version > FORMAT_VERSION {
                return Err(invalid(format!(
                    "Unsupported table format version: {version}"
                )));
            }
            if version < own {
                return Err(invalid(format!(
                    "Cannot downgrade v{own} table to v{version}"
                )));
            }
        }
        "set-properties" => {
            let updates = object(map.get("updates").unwrap_or(&Value::Null), what)?;
            if updates.values().any(|value| !value.is_string()) {
                return Err(invalid("A table's properties are strings."));
            }
            if !updates.is_empty() {
                let mut properties = table
                    .get("properties")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                properties.extend(updates.clone());
                table.insert("properties".to_string(), Value::Object(properties));
                added.changed = true;
            }
        }
        "remove-properties" => {
            let removals = map
                .get("removals")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid(format!("{what}.removals is a list.")))?;
            if !removals.is_empty() {
                let mut properties = table
                    .get("properties")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                for key in removals {
                    let key = key
                        .as_str()
                        .ok_or_else(|| invalid(format!("{what}.removals are strings.")))?;
                    properties.remove(key);
                }
                table.insert("properties".to_string(), Value::Object(properties));
                added.changed = true;
            }
        }
        "add-schema" => {
            let schema = map
                .get("schema")
                .ok_or_else(|| invalid(format!("{what}.schema is required.")))?;
            let highest =
                crate::iceberg_metadata::check_schema(schema).map_err(CommitError::Invalid)?;
            let id = add_unique(table, "schemas", "schema-id", schema, "schema")?;
            let last = int(table, "last-column-id")
                .unwrap_or_default()
                .max(highest);
            table.insert("last-column-id".to_string(), json!(last));
            added.schemas.push(id);
            added.changed = true;
        }
        "set-current-schema" => {
            let id = chosen(number(map, "schema-id", what)?, &added.schemas, "schema")?;
            if int(table, "current-schema-id") != Some(id) {
                by_id(table, "schemas", "schema-id", Some(id))
                    .ok_or_else(|| invalid(format!("Schema with id {id} does not exist")))?;
                table.insert("current-schema-id".to_string(), json!(id));
                added.changed = true;
            }
        }
        "add-spec" => {
            let spec = map
                .get("spec")
                .ok_or_else(|| invalid(format!("{what}.spec is required.")))?;
            let fields = spec
                .get("fields")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid(format!("{what}.spec.fields is a list.")))?;
            let mut highest = int(table, "last-partition-id").unwrap_or(999);
            for field in fields {
                let id = field
                    .get("field-id")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| invalid(format!("{what}: each field has its field-id.")))?;
                highest = highest.max(id);
            }
            let id = add_unique(table, "partition-specs", "spec-id", spec, "partition spec")?;
            table.insert("last-partition-id".to_string(), json!(highest));
            added.specs.push(id);
            added.changed = true;
        }
        "set-default-spec" => {
            let id = chosen(
                number(map, "spec-id", what)?,
                &added.specs,
                "partition spec",
            )?;
            if int(table, "default-spec-id") != Some(id) {
                by_id(table, "partition-specs", "spec-id", Some(id))
                    .ok_or_else(|| invalid(format!("Failed to find spec with id {id}")))?;
                table.insert("default-spec-id".to_string(), json!(id));
                added.changed = true;
            }
        }
        "add-sort-order" => {
            let order = map
                .get("sort-order")
                .ok_or_else(|| invalid(format!("{what}.sort-order is required.")))?;
            let id = add_unique(table, "sort-orders", "order-id", order, "sort order")?;
            added.orders.push(id);
            added.changed = true;
        }
        "set-default-sort-order" => {
            let id = chosen(
                number(map, "sort-order-id", what)?,
                &added.orders,
                "sort order",
            )?;
            if int(table, "default-sort-order-id") != Some(id) {
                by_id(table, "sort-orders", "order-id", Some(id))
                    .ok_or_else(|| invalid(format!("Sort order with id {id} does not exist")))?;
                table.insert("default-sort-order-id".to_string(), json!(id));
                added.changed = true;
            }
        }
        "add-snapshot" => {
            let snapshot = map
                .get("snapshot")
                .ok_or_else(|| invalid(format!("{what}.snapshot is required.")))?;
            let fields = object(snapshot, "snapshot")?;
            let id = number(fields, "snapshot-id", "snapshot")?;
            let at = number(fields, "timestamp-ms", "snapshot")?;
            let sequence = number(fields, "sequence-number", "snapshot")?;
            text(fields, "manifest-list", "snapshot")?;
            // Its summary's operation is one of Iceberg's.
            if let Some(summary) = fields.get("summary").filter(|summary| !summary.is_null()) {
                let summary = object(summary, "snapshot.summary")?;
                if !matches!(
                    summary.get("operation").and_then(Value::as_str),
                    Some("append" | "replace" | "overwrite" | "delete")
                ) || summary.values().any(|value| !value.is_string())
                {
                    return Err(invalid(
                        "snapshot.summary is strings, its operation append, replace, overwrite or delete.",
                    ));
                }
            }
            for field in [
                "schema-id",
                "parent-snapshot-id",
                "first-row-id",
                "added-rows",
            ] {
                if fields
                    .get(field)
                    .is_some_and(|id| !id.is_null() && !id.is_i64())
                {
                    return Err(invalid(format!("snapshot.{field} is a whole number.")));
                }
            }
            if by_id(table, "snapshots", "snapshot-id", Some(id)).is_some() {
                return Err(invalid(format!("Snapshot with id {id} already exists")));
            }
            let last = int(table, "last-sequence-number").unwrap_or_default();
            let parent = fields
                .get("parent-snapshot-id")
                .filter(|parent| !parent.is_null());
            if sequence <= last && parent.is_some() {
                return Err(invalid(format!(
                    "Cannot add snapshot with sequence number {sequence} older than last sequence number {last}"
                )));
            }
            let mut snapshots = list(table, "snapshots");
            snapshots.push(snapshot.clone());
            table.insert("snapshots".to_string(), Value::Array(snapshots));
            table.insert("last-updated-ms".to_string(), json!(at));
            table.insert("last-sequence-number".to_string(), json!(sequence));
            added.snapshots.push(id);
            added.changed = true;
        }
        "set-snapshot-ref" => {
            let name = text(map, "ref-name", what)?;
            let kind = text(map, "type", what)?;
            if !matches!(kind, "branch" | "tag") {
                return Err(invalid(format!("{what}.type is branch or tag.")));
            }
            let id = number(map, "snapshot-id", what)?;
            let mut reference = Map::new();
            reference.insert("snapshot-id".to_string(), json!(id));
            reference.insert("type".to_string(), json!(kind));
            // Positive whole numbers; a tag keeps none of a branch's
            // snapshots (as Iceberg's SnapshotRef has them).
            for field in [
                "max-ref-age-ms",
                "max-snapshot-age-ms",
                "min-snapshots-to-keep",
            ] {
                if let Some(value) = map.get(field).filter(|value| !value.is_null()) {
                    if value.as_i64().is_none_or(|value| value <= 0) {
                        return Err(invalid(format!(
                            "{what}.{field} is a positive whole number."
                        )));
                    }
                    if kind == "tag" && field != "max-ref-age-ms" {
                        return Err(invalid(format!("A tag does not take {field}.")));
                    }
                    reference.insert(field.to_string(), value.clone());
                }
            }
            let reference = Value::Object(reference);
            if table.get("refs").and_then(|refs| refs.get(name)) == Some(&reference) {
                return Ok(());
            }
            let at = by_id(table, "snapshots", "snapshot-id", Some(id))
                .ok_or_else(|| invalid(format!("Cannot set {name} to unknown snapshot {id}")))?
                .get("timestamp-ms")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let mut stamp = None;
            if added.snapshots.contains(&id) {
                stamp = Some(at);
            }
            if name == "main" {
                table.insert("current-snapshot-id".to_string(), json!(id));
                let mut log = list(table, "snapshot-log");
                // In order: never before the log's last entry.
                let last = log
                    .last()
                    .and_then(|entry| entry.get("timestamp-ms"))
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                let at = stamp
                    .unwrap_or_else(|| crate::objects::now_ms() as i64)
                    .max(last);
                stamp = Some(at);
                log.push(json!({ "snapshot-id": id, "timestamp-ms": at }));
                table.insert("snapshot-log".to_string(), Value::Array(log));
            }
            if let Some(at) = stamp {
                table.insert("last-updated-ms".to_string(), json!(at));
            }
            let mut refs = table
                .get("refs")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            refs.insert(name.to_string(), reference);
            table.insert("refs".to_string(), Value::Object(refs));
            added.changed = true;
        }
        "remove-snapshots" => {
            let removed = ids(map, "snapshot-ids", what)?;
            remove_ids(table, "snapshots", "snapshot-id", &removed, "snapshot")?;
            // Their children have no parent; their refs, log entries and
            // statistics go.
            let mut snapshots = list(table, "snapshots");
            for snapshot in &mut snapshots {
                if snapshot
                    .get("parent-snapshot-id")
                    .and_then(Value::as_i64)
                    .is_some_and(|parent| removed.contains(&parent))
                    && let Some(fields) = snapshot.as_object_mut()
                {
                    fields.remove("parent-snapshot-id");
                }
            }
            table.insert("snapshots".to_string(), Value::Array(snapshots));
            let mut log = list(table, "snapshot-log");
            log.retain(|entry| {
                !entry
                    .get("snapshot-id")
                    .and_then(Value::as_i64)
                    .is_some_and(|id| removed.contains(&id))
            });
            table.insert("snapshot-log".to_string(), Value::Array(log));
            let names: Vec<String> = table
                .get("refs")
                .and_then(Value::as_object)
                .map(|refs| {
                    refs.iter()
                        .filter(|(_, reference)| {
                            reference
                                .get("snapshot-id")
                                .and_then(Value::as_i64)
                                .is_some_and(|id| removed.contains(&id))
                        })
                        .map(|(name, _)| name.clone())
                        .collect()
                })
                .unwrap_or_default();
            for name in names {
                drop_ref(table, &name);
            }
            for field in ["statistics", "partition-statistics"] {
                if table.contains_key(field) {
                    let mut stats = list(table, field);
                    stats.retain(|stat| {
                        !stat
                            .get("snapshot-id")
                            .and_then(Value::as_i64)
                            .is_some_and(|id| removed.contains(&id))
                    });
                    table.insert(field.to_string(), Value::Array(stats));
                }
            }
            added.changed = true;
        }
        "remove-snapshot-ref" => {
            let name = text(map, "ref-name", what)?;
            if table.get("refs").and_then(|refs| refs.get(name)).is_some() {
                drop_ref(table, name);
                added.changed = true;
            }
        }
        // A table's location is its own storage.
        "set-location" => {
            return Err(invalid(
                "A table's location is its own storage: it is not set.",
            ));
        }
        "set-statistics" | "set-partition-statistics" => {
            let (field, member) = if action == "set-statistics" {
                ("statistics", "statistics")
            } else {
                ("partition-statistics", "partition-statistics")
            };
            let file = map
                .get(member)
                .ok_or_else(|| invalid(format!("{what}.{member} is required.")))?;
            let id = check_statistics(file, action == "set-partition-statistics", member)?;
            let mut stats = list(table, field);
            stats.retain(|stat| stat.get("snapshot-id").and_then(Value::as_i64) != Some(id));
            stats.push(file.clone());
            table.insert(field.to_string(), Value::Array(stats));
            added.changed = true;
        }
        "remove-statistics" | "remove-partition-statistics" => {
            let field = if action == "remove-statistics" {
                "statistics"
            } else {
                "partition-statistics"
            };
            let id = number(map, "snapshot-id", what)?;
            remove_ids(table, field, "snapshot-id", &[id], "statistics of snapshot")?;
            added.changed = true;
        }
        "remove-partition-specs" => {
            let removed = ids(map, "spec-ids", what)?;
            if int(table, "default-spec-id").is_some_and(|id| removed.contains(&id)) {
                return Err(invalid("Cannot remove the default partition spec."));
            }
            remove_ids(
                table,
                "partition-specs",
                "spec-id",
                &removed,
                "partition spec",
            )?;
            added.changed = true;
        }
        "remove-schemas" => {
            let removed = ids(map, "schema-ids", what)?;
            if int(table, "current-schema-id").is_some_and(|id| removed.contains(&id)) {
                return Err(invalid("Cannot remove the current schema."));
            }
            remove_ids(table, "schemas", "schema-id", &removed, "schema")?;
            added.changed = true;
        }
        _ => {
            return Err(invalid(format!(
                "{action} is not a table update served here."
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Value {
        crate::iceberg_metadata::initial(
            &json!({"iceberg": {"schema": {"fields": [
                {"name": "id", "type": "long", "required": true},
                {"name": "day", "type": "date"},
            ]}, "properties": {"a": "1"}}}),
            "s3://t--table-s3",
            "0b1e0f1c-6c1a-4a8e-9a39-2f3c2b9b8a11",
            10,
        )
        .unwrap()
    }

    fn snapshot(id: i64, parent: Option<i64>, sequence: i64) -> Value {
        let mut snapshot = json!({
            "snapshot-id": id,
            "sequence-number": sequence,
            "timestamp-ms": 100 + id,
            "manifest-list": format!("s3://t--table-s3/metadata/snap-{id}.avro"),
            "summary": {"operation": "append"},
            "schema-id": 0,
        });
        if let Some(parent) = parent {
            snapshot["parent-snapshot-id"] = json!(parent);
        }
        snapshot
    }

    fn append(id: i64, parent: Option<i64>, sequence: i64) -> Vec<Value> {
        vec![
            json!({"action": "add-snapshot", "snapshot": snapshot(id, parent, sequence)}),
            json!({"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": id}),
        ]
    }

    const AT: &str = "s3://t--table-s3/metadata/00000-a.metadata.json";

    #[test]
    fn an_append_adds_its_snapshot_and_moves_main_with_the_file_logged() {
        let table = base();
        let requirements = [
            json!({"type": "assert-table-uuid", "uuid": "0b1e0f1c-6c1a-4a8e-9a39-2f3c2b9b8a11"}),
            json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}),
        ];
        let next = apply(&table, AT, &requirements, &append(1, None, 1), 999)
            .unwrap()
            .unwrap();
        assert_eq!(next["current-snapshot-id"], 1);
        assert_eq!(
            next["refs"]["main"],
            json!({"snapshot-id": 1, "type": "branch"})
        );
        assert_eq!(next["last-sequence-number"], 1);
        assert_eq!(next["last-updated-ms"], 101);
        assert_eq!(
            next["snapshot-log"],
            json!([{"snapshot-id": 1, "timestamp-ms": 101}])
        );
        assert_eq!(
            next["metadata-log"],
            json!([{"metadata-file": AT, "timestamp-ms": 10}])
        );
        // A second append on it; a stale one (main moved since) fails.
        let second = apply(
            &next,
            "s3://t--table-s3/metadata/00001-b.metadata.json",
            &[json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1})],
            &append(2, Some(1), 2),
            999,
        )
        .unwrap()
        .unwrap();
        assert_eq!(second["snapshots"].as_array().unwrap().len(), 2);
        assert_eq!(second["metadata-log"].as_array().unwrap().len(), 2);
        let stale = [json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1})];
        assert!(matches!(
            apply(&second, AT, &stale, &append(3, Some(1), 3), 999),
            Err(CommitError::Failed(_))
        ));
        // A sequence number not past the last, with a parent: refused.
        assert!(matches!(
            apply(&second, AT, &[], &append(3, Some(2), 2), 999),
            Err(CommitError::Invalid(_))
        ));
        // Nothing to change: no next metadata.
        assert_eq!(apply(&second, AT, &[], &[], 999), Ok(None));
        // A snapshot stamped before the last commit (a client's clock
        // behind): the table's time does not run back.
        let behind = apply(
            &second,
            "s3://t--table-s3/metadata/00002-c.metadata.json",
            &[],
            &append(9, Some(2), 3)
                .into_iter()
                .map(|mut update| {
                    if update["action"] == "add-snapshot" {
                        update["snapshot"]["timestamp-ms"] = json!(5);
                    }
                    update
                })
                .collect::<Vec<_>>(),
            999,
        )
        .unwrap()
        .unwrap();
        assert_eq!(behind["last-updated-ms"], 999);
        // A snapshot added without moving main, stamped before the logs'
        // last entry: the table's time does not run back either.
        let branchless = apply(
            &second,
            "s3://t--table-s3/metadata/00002-d.metadata.json",
            &[],
            &[json!({"action": "add-snapshot", "snapshot": {"snapshot-id": 8, "parent-snapshot-id": 2,
                "sequence-number": 3, "timestamp-ms": 5, "manifest-list": "s3://t--table-s3/m8.avro"}})],
            999,
        )
        .unwrap()
        .unwrap();
        assert_eq!(branchless["last-updated-ms"], 999);
        // Its snapshot log stays in order: logged no earlier than the last.
        let log = behind["snapshot-log"].as_array().unwrap();
        assert_eq!(
            log[log.len() - 1]["timestamp-ms"],
            log[log.len() - 2]["timestamp-ms"]
        );
    }

    #[test]
    fn requirements_are_checked_against_the_current_metadata() {
        let table = base();
        for (requirement, holds) in [
            (json!({"type": "assert-create"}), false),
            (
                json!({"type": "assert-table-uuid", "uuid": "0B1E0F1C-6C1A-4A8E-9A39-2F3C2B9B8A11"}),
                true,
            ),
            (
                json!({"type": "assert-table-uuid", "uuid": "00000000-0000-0000-0000-000000000000"}),
                false,
            ),
            (
                json!({"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 4}),
                false,
            ),
            (
                json!({"type": "assert-ref-snapshot-id", "ref": "dev", "snapshot-id": null}),
                false,
            ),
            (
                json!({"type": "assert-last-assigned-field-id", "last-assigned-field-id": 2}),
                true,
            ),
            (
                json!({"type": "assert-last-assigned-field-id", "last-assigned-field-id": 3}),
                false,
            ),
            (
                json!({"type": "assert-current-schema-id", "current-schema-id": 0}),
                true,
            ),
            (
                json!({"type": "assert-last-assigned-partition-id", "last-assigned-partition-id": 999}),
                true,
            ),
            (
                json!({"type": "assert-last-assigned-partition-id", "last-assigned-partition-id": 1000}),
                false,
            ),
            (
                json!({"type": "assert-default-spec-id", "default-spec-id": 0}),
                true,
            ),
            (
                json!({"type": "assert-default-sort-order-id", "default-sort-order-id": 1}),
                false,
            ),
        ] {
            let result = apply(
                &table,
                AT,
                &[requirement.clone()],
                &[json!({"action": "set-properties", "updates": {"b": "2"}})],
                999,
            );
            assert_eq!(result.is_ok(), holds, "{requirement}: {result:?}");
            if !holds {
                assert!(
                    matches!(result, Err(CommitError::Failed(_))),
                    "{requirement}"
                );
            }
        }
        assert!(matches!(
            apply(&table, AT, &[json!({"type": "assert-other"})], &[], 999),
            Err(CommitError::Invalid(_))
        ));
    }

    #[test]
    fn schemas_specs_orders_and_properties_evolve_as_iceberg_has_them() {
        let table = base();
        let schema = json!({"type": "struct", "schema-id": 1, "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "day", "required": false, "type": "date"},
            {"id": 3, "name": "note", "required": false, "type": "string"},
        ]});
        let updates = [
            json!({"action": "add-schema", "schema": schema}),
            json!({"action": "set-current-schema", "schema-id": -1}),
            json!({"action": "add-spec", "spec": {"spec-id": 1, "fields": [
                {"source-id": 2, "field-id": 1000, "transform": "month", "name": "day_month"}]}}),
            json!({"action": "set-default-spec", "spec-id": -1}),
            json!({"action": "add-sort-order", "sort-order": {"order-id": 1, "fields": [
                {"source-id": 3, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}}),
            json!({"action": "set-default-sort-order", "sort-order-id": -1}),
            json!({"action": "set-properties", "updates": {"b": "2"}}),
            json!({"action": "remove-properties", "removals": ["a", "missing"]}),
        ];
        let next = apply(&table, AT, &[], &updates, 999).unwrap().unwrap();
        assert_eq!(
            (
                &next["current-schema-id"],
                &next["last-column-id"],
                &next["default-spec-id"],
                &next["last-partition-id"],
                &next["default-sort-order-id"],
            ),
            (&json!(1), &json!(3), &json!(1), &json!(1000), &json!(1))
        );
        assert_eq!(next["properties"], json!({"b": "2"}));
        assert_eq!(next["last-updated-ms"], 999);
        for (bad, why) in [
            // -1 with none added.
            (
                json!({"action": "set-current-schema", "schema-id": -1}),
                "none has been added",
            ),
            (
                json!({"action": "set-current-schema", "schema-id": 7}),
                "does not exist",
            ),
            (
                json!({"action": "add-schema", "schema": {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "other", "required": true, "type": "long"}]}}),
                "already exists",
            ),
            (
                json!({"action": "add-schema", "schema": {"type": "struct", "schema-id": 1, "fields": [
                {"id": 1, "name": "x", "required": true, "type": "varchar"}]}}),
                "is not an Iceberg",
            ),
            (
                json!({"action": "add-spec", "spec": {"spec-id": 1, "fields": [
                {"source-id": 2, "transform": "month", "name": "m"}]}}),
                "field-id",
            ),
            (
                json!({"action": "set-location", "location": "s3://elsewhere"}),
                "location",
            ),
            (
                json!({"action": "upgrade-format-version", "format-version": 3}),
                "Unsupported",
            ),
            (
                json!({"action": "upgrade-format-version", "format-version": 1}),
                "downgrade",
            ),
            (
                json!({"action": "remove-schemas", "schema-ids": [0]}),
                "current schema",
            ),
            (
                json!({"action": "remove-partition-specs", "spec-ids": [0]}),
                "default partition spec",
            ),
            (
                json!({"action": "remove-snapshots", "snapshot-ids": [9]}),
                "does not exist",
            ),
            (
                json!({"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": 9}),
                "unknown snapshot",
            ),
            // A ref's retention: positive whole numbers, a branch's only.
            (
                json!({"action": "set-snapshot-ref", "ref-name": "t", "type": "tag",
                    "snapshot-id": 9, "min-snapshots-to-keep": 1}),
                "A tag does not take",
            ),
            (
                json!({"action": "set-snapshot-ref", "ref-name": "b", "type": "branch",
                    "snapshot-id": 9, "max-ref-age-ms": -1}),
                "positive whole number",
            ),
            (
                json!({"action": "set-snapshot-ref", "ref-name": "b", "type": "branch",
                    "snapshot-id": 9, "min-snapshots-to-keep": 1.5}),
                "positive whole number",
            ),
            (
                json!({"action": "add-snapshot", "snapshot": {"snapshot-id": 5, "sequence-number": 1,
                    "timestamp-ms": 1, "manifest-list": "s3://t--table-s3/m.avro",
                    "parent-snapshot-id": "abc"}}),
                "parent-snapshot-id is a whole number",
            ),
            (
                json!({"action": "add-snapshot", "snapshot": {"snapshot-id": 5, "sequence-number": 1,
                    "timestamp-ms": 1, "manifest-list": "s3://t--table-s3/m.avro", "added-rows": "x"}}),
                "added-rows is a whole number",
            ),
            // A spec added, not the default: its source checked too.
            (
                json!({"action": "add-spec", "spec": {"spec-id": 4, "fields": [
                    {"source-id": 9, "field-id": 1000, "transform": "identity", "name": "p"}]}}),
                "source field 9",
            ),
            (
                json!({"action": "assign-uuid", "uuid": "not-a-uuid"}),
                "is a UUID",
            ),
            (
                json!({"action": "set-statistics", "statistics": {"snapshot-id": 1}}),
                "statistics-path",
            ),
            (
                json!({"action": "set-statistics", "statistics": {"snapshot-id": 1,
                "statistics-path": "s3://t--table-s3/s.puffin", "file-size-in-bytes": 1,
                "file-footer-size-in-bytes": 1, "blob-metadata": [{"type": "other", "snapshot-id": 1,
                "sequence-number": 1, "fields": [1]}]}}),
                "blob type",
            ),
            (
                json!({"action": "set-partition-statistics", "partition-statistics": {"snapshot-id": 1,
                "statistics-path": "s3://t--table-s3/p.parquet"}}),
                "file-size-in-bytes",
            ),
            // An order added, not the default: its fields checked too.
            (
                json!({"action": "add-sort-order", "sort-order": {"order-id": 3, "fields": [{"bogus": 1}]}}),
                "bogus is not a member",
            ),
            (
                json!({"action": "add-snapshot", "snapshot": {"snapshot-id": 5, "sequence-number": 1,
                "timestamp-ms": 1, "manifest-list": "s3://t--table-s3/m.avro",
                "summary": {"operation": "rewrite"}}}),
                "operation",
            ),
            (
                json!({"action": "add-encryption-key", "encryption-key": {}}),
                "not a table update",
            ),
        ] {
            match apply(&table, AT, &[], &[bad.clone()], 999) {
                Err(CommitError::Invalid(message)) => {
                    assert!(message.contains(why), "{bad}: {message}")
                }
                other => panic!("{bad}: {other:?}"),
            }
        }
        // A spec whose source the current schema does not have: refused.
        let wrong = [
            json!({"action": "add-spec", "spec": {"spec-id": 1, "fields": [
                {"source-id": 9, "field-id": 1000, "transform": "identity", "name": "p"}]}}),
            json!({"action": "set-default-spec", "spec-id": 1}),
        ];
        assert!(matches!(
            apply(&table, AT, &[], &wrong, 999),
            Err(CommitError::Invalid(_))
        ));
    }

    #[test]
    fn removing_snapshots_takes_their_refs_log_and_parents_and_the_log_is_bounded() {
        let first = apply(&base(), AT, &[], &append(1, None, 1), 999)
            .unwrap()
            .unwrap();
        let second = apply(&first, AT, &[], &append(2, Some(1), 2), 999)
            .unwrap()
            .unwrap();
        let tagged = apply(
            &second,
            AT,
            &[],
            &[json!({"action": "set-snapshot-ref", "ref-name": "v1", "type": "tag", "snapshot-id": 1})],
            999,
        )
        .unwrap()
        .unwrap();
        let removed = apply(
            &tagged,
            AT,
            &[],
            &[json!({"action": "remove-snapshots", "snapshot-ids": [1]})],
            999,
        )
        .unwrap()
        .unwrap();
        assert_eq!(removed["snapshots"].as_array().unwrap().len(), 1);
        assert!(removed["snapshots"][0].get("parent-snapshot-id").is_none());
        assert!(removed["refs"].get("v1").is_none());
        assert_eq!(removed["current-snapshot-id"], 2);
        assert_eq!(
            removed["snapshot-log"],
            json!([{"snapshot-id": 2, "timestamp-ms": 102}])
        );
        // The main branch removed: no current snapshot.
        let unreferenced = apply(
            &removed,
            AT,
            &[],
            &[json!({"action": "remove-snapshot-ref", "ref-name": "main"})],
            999,
        )
        .unwrap()
        .unwrap();
        assert!(unreferenced.get("current-snapshot-id").is_none());
        // The metadata log keeps write.metadata.previous-versions-max.
        let mut table = apply(
            &base(),
            AT,
            &[],
            &[json!({"action": "set-properties", "updates": {"write.metadata.previous-versions-max": "2"}})],
            999,
        )
        .unwrap()
        .unwrap();
        for n in 0..4 {
            table = apply(
                &table,
                &format!("s3://t--table-s3/metadata/0000{n}.metadata.json"),
                &[],
                &[json!({"action": "set-properties", "updates": {"n": n.to_string()}})],
                999,
            )
            .unwrap()
            .unwrap();
        }
        assert_eq!(
            table["metadata-log"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| entry["metadata-file"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [
                "s3://t--table-s3/metadata/00002.metadata.json",
                "s3://t--table-s3/metadata/00003.metadata.json",
            ]
        );
    }
}
