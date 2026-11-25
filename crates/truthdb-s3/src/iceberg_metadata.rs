//! The first Iceberg metadata file (format version 2) of an S3 Tables table
//! made with CreateTable's `metadata`: its schema, partition spec, write
//! order and properties, checked as Iceberg's readers check them (so the
//! file loads), with the ids the request leaves out assigned and its types
//! written in their canonical form.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value, json};

/// The partition field ids assigned start here (as Iceberg's reference
/// implementation's do); an unpartitioned table's last is one below.
const FIRST_PARTITION_FIELD_ID: i64 = 1000;
/// Iceberg's ids are 32-bit.
const MAX_ID: i64 = i32::MAX as i64;

/// The metadata file of a table at `location` (its warehouse location),
/// of id `table_uuid`, made at `now_ms`, from CreateTable's `metadata`;
/// why not, if it is not one.
pub(crate) fn initial(
    metadata: &Value,
    location: &str,
    table_uuid: &str,
    now_ms: u64,
) -> Result<Value, String> {
    let metadata = object(metadata, "metadata")?;
    only(metadata, "metadata", &["iceberg"])?;
    let iceberg = object(
        metadata.get("iceberg").ok_or("metadata has iceberg.")?,
        "metadata.iceberg",
    )?;
    only(
        iceberg,
        "metadata.iceberg",
        &[
            "schema",
            "schemaV2",
            "partitionSpec",
            "writeOrder",
            "properties",
        ],
    )?;
    let mut columns = Columns::default();
    let schema = match (iceberg.get("schema"), iceberg.get("schemaV2")) {
        (Some(schema), None) => schema_v1(schema, &mut columns)?,
        (None, Some(schema)) => schema_v2(schema, &mut columns)?,
        _ => return Err("metadata.iceberg has one of schema and schemaV2.".to_string()),
    };
    let last_column_id = columns.ids.iter().max().copied().unwrap_or(0);
    let schema_id = schema["schema-id"].clone();
    let (spec, last_partition_id) = match iceberg.get("partitionSpec") {
        Some(spec) => partition_spec(spec, &columns)?,
        None => (
            json!({ "spec-id": 0, "fields": [] }),
            FIRST_PARTITION_FIELD_ID - 1,
        ),
    };
    let order = match iceberg.get("writeOrder") {
        Some(order) => write_order(order, &columns)?,
        // Order 0 is the unsorted order.
        None => json!({ "order-id": 0, "fields": [] }),
    };
    let properties = match iceberg.get("properties") {
        None => Map::new(),
        Some(properties) => {
            let properties = object(properties, "metadata.iceberg.properties")?;
            if properties.values().any(|value| !value.is_string()) {
                return Err("metadata.iceberg.properties are strings.".to_string());
            }
            properties.clone()
        }
    };
    Ok(json!({
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": now_ms,
        "last-column-id": last_column_id,
        "current-schema-id": schema_id,
        "schemas": [schema],
        "default-spec-id": spec["spec-id"],
        "partition-specs": [spec],
        "last-partition-id": last_partition_id,
        "default-sort-order-id": order["order-id"],
        "sort-orders": [order],
        "properties": properties,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {},
    }))
}

/// What a schema's fields are, as its partition spec, write order and
/// identifier fields refer to them.
#[derive(Default)]
struct Columns {
    /// Every field id (elements', keys' and values' too).
    ids: BTreeSet<i64>,
    /// The fields a partition or sort may take as its source (primitive,
    /// in no list or map): their type (`decimal`, `fixed` without their
    /// arguments) and full name.
    sources: BTreeMap<i64, (String, String)>,
    /// Every field's full name (`a.b` for `b` in struct `a`).
    names: BTreeSet<String>,
    /// The fields that may be identifier fields: required primitives, not
    /// float or double, through required structs only.
    identifiers: BTreeSet<i64>,
}

fn object<'a>(value: &'a Value, what: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| format!("{what} is an object."))
}

fn array<'a>(value: Option<&'a Value>, what: &str) -> Result<&'a Vec<Value>, String> {
    value
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{what} is a list."))
}

/// Refuses any member of `map` not in `known`.
fn only(map: &Map<String, Value>, what: &str, known: &[&str]) -> Result<(), String> {
    match map.keys().find(|key| !known.contains(&key.as_str())) {
        Some(key) => Err(format!("{key} is not a member of {what}.")),
        None => Ok(()),
    }
}

fn text<'a>(map: &'a Map<String, Value>, field: &str, what: &str) -> Result<&'a str, String> {
    map.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{what}.{field} is a string."))
}

fn id(value: Option<&Value>, what: &str) -> Result<Option<i64>, String> {
    match value {
        None => Ok(None),
        Some(value) => match value.as_i64() {
            Some(id) if (1..=MAX_ID).contains(&id) => Ok(Some(id)),
            _ => Err(format!("{what} is a positive whole number.")),
        },
    }
}

fn flag(map: &Map<String, Value>, field: &str, what: &str) -> Result<bool, String> {
    match map.get(field) {
        None => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(format!("{what}.{field} is true or false.")),
    }
}

/// Field ids, each new.
fn add_id(ids: &mut BTreeSet<i64>, id: i64) -> Result<(), String> {
    if ids.insert(id) {
        Ok(())
    } else {
        Err(format!("The field id {id} is used more than once."))
    }
}

/// The next id after `last` to assign, if there is one.
fn next_id(last: i64) -> Result<i64, String> {
    if last < MAX_ID {
        Ok(last + 1)
    } else {
        Err("No field id is left to assign (they are 32-bit).".to_string())
    }
}

/// A count in a type or transform's brackets: digits only, more than 0.
fn count(text: &str) -> Option<u32> {
    (!text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| text.parse::<u32>().ok())
        .flatten()
        .filter(|n| *n > 0)
}

/// One of Iceberg's primitive types (format version 2), as its readers
/// parse them (`fixed[L]`, `decimal(P, S)`): its canonical name, and its
/// kind (the name without its arguments).
fn primitive(name: &str) -> Option<(String, &'static str)> {
    const PLAIN: [&str; 12] = [
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "date",
        "time",
        "timestamp",
        "timestamptz",
        "string",
        "uuid",
        "binary",
    ];
    if let Some(&kind) = PLAIN.iter().find(|kind| **kind == name) {
        return Some((kind.to_string(), kind));
    }
    if let Some(length) = name
        .strip_prefix("fixed[")
        .and_then(|rest| rest.strip_suffix(']'))
        .and_then(count)
    {
        return Some((format!("fixed[{length}]"), "fixed"));
    }
    let (precision, scale) = name
        .strip_prefix("decimal(")
        .and_then(|rest| rest.strip_suffix(')'))?
        .split_once(',')?;
    let (precision, scale) = (count(precision)?, scale.trim_start_matches(' '));
    let scale = if scale == "0" { 0 } else { count(scale)? };
    ((1..=38).contains(&precision) && scale <= precision)
        .then(|| (format!("decimal({precision}, {scale})"), "decimal"))
}

/// A schema of primitive fields (`schema`): its Iceberg form. Ids left out
/// are assigned after the highest given.
fn schema_v1(schema: &Value, columns: &mut Columns) -> Result<Value, String> {
    let what = "metadata.iceberg.schema";
    let schema = object(schema, what)?;
    only(schema, what, &["fields"])?;
    let fields = array(schema.get("fields"), &format!("{what}.fields"))?;
    let mut given = Vec::new();
    for field in fields {
        let what = format!("{what}.fields[]");
        let field = object(field, &what)?;
        only(field, &what, &["id", "name", "type", "required"])?;
        let name = text(field, "name", &what)?;
        let kind = text(field, "type", &what)?;
        let (kind, base) =
            primitive(kind).ok_or_else(|| format!("{kind} is not an Iceberg primitive type."))?;
        if !columns.names.insert(name.to_string()) {
            return Err(format!("The field {name} is named more than once."));
        }
        let field_id = id(field.get("id"), &format!("{what}.id"))?;
        if let Some(field_id) = field_id {
            add_id(&mut columns.ids, field_id)?;
        }
        given.push((field_id, name, kind, base, flag(field, "required", &what)?));
    }
    let mut last = columns.ids.iter().max().copied().unwrap_or(0);
    let mut out = Vec::new();
    for (field_id, name, kind, base, required) in given {
        let field_id = match field_id {
            Some(field_id) => field_id,
            None => {
                last = next_id(last)?;
                last
            }
        };
        columns.ids.insert(field_id);
        columns
            .sources
            .insert(field_id, (base.to_string(), name.to_string()));
        out.push(json!({ "id": field_id, "name": name, "required": required, "type": kind }));
    }
    Ok(json!({ "type": "struct", "schema-id": 0, "fields": out }))
}

/// A schema with nested types (`schemaV2`): as given, its types canonical
/// and its schema id 0 if none.
fn schema_v2(schema: &Value, columns: &mut Columns) -> Result<Value, String> {
    let what = "metadata.iceberg.schemaV2";
    let map = object(schema, what)?;
    only(
        map,
        what,
        &["type", "fields", "schema-id", "identifier-field-ids"],
    )?;
    if map.get("type").and_then(Value::as_str) != Some("struct") {
        return Err(format!("{what}.type is struct."));
    }
    let schema_id = match map.get("schema-id") {
        None => 0,
        Some(value) => value
            .as_i64()
            .filter(|id| (0..=MAX_ID).contains(id))
            .ok_or_else(|| format!("{what}.schema-id is a whole number."))?,
    };
    let fields = array(map.get("fields"), &format!("{what}.fields"))?;
    let fields = struct_fields(fields, what, "", columns, (true, true))?;
    let mut out = json!({ "type": "struct", "schema-id": schema_id, "fields": fields });
    if let Some(identifiers) = map.get("identifier-field-ids") {
        let what = format!("{what}.identifier-field-ids");
        let mut kept = Vec::new();
        for identifier in array(Some(identifiers), &what)? {
            let identifier = id(Some(identifier), &what)?.unwrap_or_default();
            // Iceberg: required primitive fields (not float or double),
            // through required structs only.
            if !columns.identifiers.contains(&identifier) {
                return Err(format!(
                    "The identifier field {identifier} is not a required primitive field (not float or double) of the schema's required structs."
                ));
            }
            kept.push(identifier);
        }
        out["identifier-field-ids"] = json!(kept);
    }
    Ok(out)
}

/// A struct's fields (their names under `prefix`), canonical; each id new.
/// `(sources, identifiers)`: whether its primitive fields may be sources
/// (in no list or map) and identifier fields (through required structs).
fn struct_fields(
    fields: &[Value],
    what: &str,
    prefix: &str,
    columns: &mut Columns,
    (sources, identifiers): (bool, bool),
) -> Result<Vec<Value>, String> {
    let mut names = BTreeSet::new();
    let mut out = Vec::new();
    for field in fields {
        let what = format!("{what}.fields[]");
        let field = object(field, &what)?;
        only(field, &what, &["id", "name", "required", "type", "doc"])?;
        let name = text(field, "name", &what)?;
        if !names.insert(name) {
            return Err(format!("The field {name} is named more than once."));
        }
        let full = format!("{prefix}{name}");
        columns.names.insert(full.clone());
        let field_id = id(field.get("id"), &format!("{what}.id"))?
            .ok_or_else(|| format!("{what}.id is required."))?;
        if !field.get("required").is_some_and(Value::is_boolean) {
            return Err(format!("{what}.required is true or false."));
        }
        let required = flag(field, "required", &what)?;
        if field.get("doc").is_some_and(|doc| !doc.is_string()) {
            return Err(format!("{what}.doc is a string."));
        }
        add_id(&mut columns.ids, field_id)?;
        let kind = field
            .get("type")
            .ok_or_else(|| format!("{what}.type is required."))?;
        let (kind, base) = nested_type(
            kind,
            &what,
            &full,
            columns,
            (sources, identifiers && required),
        )?;
        if let Some(base) = base {
            if sources {
                columns.sources.insert(field_id, (base.to_string(), full));
            }
            if identifiers && required && !matches!(base, "float" | "double") {
                columns.identifiers.insert(field_id);
            }
        }
        let mut kept = field.clone();
        kept.insert("type".to_string(), kind);
        out.push(Value::Object(kept));
    }
    Ok(out)
}

/// A field's (or element's, key's, value's) type, canonical, and its kind
/// if it is primitive. What is in a list or a map is no partition or sort
/// source ("cannot be contained in a map or list") nor identifier field.
fn nested_type(
    kind: &Value,
    what: &str,
    path: &str,
    columns: &mut Columns,
    (sources, identifiers): (bool, bool),
) -> Result<(Value, Option<&'static str>), String> {
    if let Some(name) = kind.as_str() {
        let (name, base) =
            primitive(name).ok_or_else(|| format!("{name} is not an Iceberg type."))?;
        return Ok((json!(name), Some(base)));
    }
    let what = format!("{what}.type");
    let map = object(kind, &what)?;
    let mut out = map.clone();
    match map.get("type").and_then(Value::as_str) {
        Some("struct") => {
            only(map, &what, &["type", "fields"])?;
            let fields = array(map.get("fields"), &format!("{what}.fields"))?;
            let fields = struct_fields(
                fields,
                &what,
                &format!("{path}."),
                columns,
                (sources, identifiers),
            )?;
            out.insert("fields".to_string(), json!(fields));
        }
        Some("list") => {
            only(
                map,
                &what,
                &["type", "element-id", "element", "element-required"],
            )?;
            let element = id(map.get("element-id"), &format!("{what}.element-id"))?
                .ok_or_else(|| format!("{what}.element-id is required."))?;
            add_id(&mut columns.ids, element)?;
            if !map.get("element-required").is_some_and(Value::is_boolean) {
                return Err(format!("{what}.element-required is true or false."));
            }
            let element_type = map
                .get("element")
                .ok_or_else(|| format!("{what}.element is required."))?;
            let path = format!("{path}.element");
            columns.names.insert(path.clone());
            let (element_type, _) =
                nested_type(element_type, &what, &path, columns, (false, false))?;
            out.insert("element".to_string(), element_type);
        }
        Some("map") => {
            only(
                map,
                &what,
                &[
                    "type",
                    "key-id",
                    "key",
                    "value-id",
                    "value",
                    "value-required",
                ],
            )?;
            for (id_name, type_name) in [("key-id", "key"), ("value-id", "value")] {
                let part = id(map.get(id_name), &format!("{what}.{id_name}"))?
                    .ok_or_else(|| format!("{what}.{id_name} is required."))?;
                add_id(&mut columns.ids, part)?;
                let part_type = map
                    .get(type_name)
                    .ok_or_else(|| format!("{what}.{type_name} is required."))?;
                let path = format!("{path}.{type_name}");
                columns.names.insert(path.clone());
                let (part_type, _) = nested_type(part_type, &what, &path, columns, (false, false))?;
                out.insert(type_name.to_string(), part_type);
            }
            if !map.get("value-required").is_some_and(Value::is_boolean) {
                return Err(format!("{what}.value-required is true or false."));
            }
        }
        _ => return Err(format!("{what} is a type name, a struct, a list or a map.")),
    }
    Ok((Value::Object(out), None))
}

/// A partition or sort field's source (a primitive field of the schema, in
/// no list or map) and transform (one that applies to its type): its id,
/// transform and the source's full name.
fn source<'a>(
    field: &'a Map<String, Value>,
    what: &str,
    columns: &'a Columns,
) -> Result<(i64, &'a str, &'a str), String> {
    let source = id(field.get("source-id"), &format!("{what}.source-id"))?
        .ok_or_else(|| format!("{what}.source-id is required."))?;
    let (kind, name) = columns.sources.get(&source).ok_or_else(|| {
        format!(
            "The source field {source} is not a primitive field of the schema (in no list or map)."
        )
    })?;
    let transform = text(field, "transform", what)?;
    let sized = |prefix: &str| {
        transform
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(']'))
            .and_then(count)
            .is_some()
    };
    // Iceberg's transforms and the types each applies to.
    let applies = match transform {
        "identity" | "void" => true,
        "year" | "month" | "day" => matches!(kind.as_str(), "date" | "timestamp" | "timestamptz"),
        "hour" => matches!(kind.as_str(), "timestamp" | "timestamptz"),
        _ if sized("bucket[") => matches!(
            kind.as_str(),
            "int"
                | "long"
                | "decimal"
                | "date"
                | "time"
                | "timestamp"
                | "timestamptz"
                | "string"
                | "uuid"
                | "fixed"
                | "binary"
        ),
        _ if sized("truncate[") => {
            matches!(
                kind.as_str(),
                "int" | "long" | "decimal" | "string" | "binary"
            )
        }
        _ => return Err(format!("{transform} is not an Iceberg transform.")),
    };
    if !applies {
        return Err(format!(
            "The transform {transform} does not apply to the {kind} field {name}."
        ));
    }
    Ok((source, transform, name))
}

/// The partition spec (spec id 0 if none), and the last partition field id.
/// Field ids left out are assigned from 1000, after the highest given.
fn partition_spec(spec: &Value, columns: &Columns) -> Result<(Value, i64), String> {
    let what = "metadata.iceberg.partitionSpec";
    let map = object(spec, what)?;
    only(map, what, &["fields", "spec-id"])?;
    let spec_id = match map.get("spec-id") {
        None => 0,
        Some(value) => value
            .as_i64()
            .filter(|id| (0..=MAX_ID).contains(id))
            .ok_or_else(|| format!("{what}.spec-id is a whole number."))?,
    };
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    let mut given = Vec::new();
    for field in array(map.get("fields"), &format!("{what}.fields"))? {
        let what = format!("{what}.fields[]");
        let field = object(field, &what)?;
        only(
            field,
            &what,
            &["source-id", "transform", "name", "field-id"],
        )?;
        let (source_id, transform, source_name) = source(field, &what, columns)?;
        let name = text(field, "name", &what)?;
        if !names.insert(name) {
            return Err(format!(
                "The partition field {name} is named more than once."
            ));
        }
        // A schema field's name only for that field's identity.
        if columns.names.contains(name) && !(transform == "identity" && source_name == name) {
            return Err(format!(
                "The partition field {name} has a schema field's name."
            ));
        }
        let field_id = id(field.get("field-id"), &format!("{what}.field-id"))?;
        if let Some(field_id) = field_id {
            add_id(&mut ids, field_id)?;
        }
        given.push((source_id, transform, name, field_id));
    }
    let mut last = ids
        .iter()
        .max()
        .copied()
        .unwrap_or(0)
        .max(FIRST_PARTITION_FIELD_ID - 1);
    let mut highest = FIRST_PARTITION_FIELD_ID - 1;
    let mut out = Vec::new();
    for (source_id, transform, name, field_id) in given {
        let field_id = match field_id {
            Some(field_id) => field_id,
            None => {
                last = next_id(last)?;
                last
            }
        };
        highest = highest.max(field_id);
        out.push(json!({
            "source-id": source_id,
            "field-id": field_id,
            "transform": transform,
            "name": name,
        }));
    }
    Ok((json!({ "spec-id": spec_id, "fields": out }), highest))
}

/// The write order: its order id (1 if none) and fields; order 0 is the
/// unsorted order, of no fields.
fn write_order(order: &Value, columns: &Columns) -> Result<Value, String> {
    let what = "metadata.iceberg.writeOrder";
    let map = object(order, what)?;
    only(map, what, &["order-id", "fields"])?;
    let order_id = match map.get("order-id") {
        None => 1,
        Some(value) => value
            .as_i64()
            .filter(|id| (0..=MAX_ID).contains(id))
            .ok_or_else(|| format!("{what}.order-id is a whole number."))?,
    };
    let fields = array(map.get("fields"), &format!("{what}.fields"))?;
    if (order_id == 0) != fields.is_empty() {
        return Err(format!(
            "{what} is the unsorted order (order-id 0, no fields) or one of fields."
        ));
    }
    let mut out = Vec::new();
    for field in fields {
        let what = format!("{what}.fields[]");
        let field = object(field, &what)?;
        only(
            field,
            &what,
            &["source-id", "transform", "direction", "null-order"],
        )?;
        let (source_id, transform, _) = source(field, &what, columns)?;
        let direction = text(field, "direction", &what)?;
        let nulls = text(field, "null-order", &what)?;
        if !matches!(direction, "asc" | "desc") {
            return Err(format!("{what}.direction is asc or desc."));
        }
        if !matches!(nulls, "nulls-first" | "nulls-last") {
            return Err(format!("{what}.null-order is nulls-first or nulls-last."));
        }
        out.push(json!({
            "source-id": source_id,
            "transform": transform,
            "direction": direction,
            "null-order": nulls,
        }));
    }
    Ok(json!({ "order-id": order_id, "fields": out }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn made(metadata: Value) -> Result<Value, String> {
        initial(&metadata, "s3://t-1--table-s3", "u-1", 7)
    }

    #[test]
    fn a_primitive_schema_gets_its_ids_and_an_unpartitioned_unsorted_table() {
        let file = made(json!({"iceberg": {"schema": {"fields": [
            {"name": "id", "type": "long", "required": true},
            {"name": "at", "type": "timestamptz", "id": 5},
            {"name": "amount", "type": "decimal(10,2)"},
            {"name": "raw", "type": "fixed[016]"},
        ]}, "properties": {"owner": "sales"}}}))
        .unwrap();
        assert_eq!(
            file,
            json!({
                "format-version": 2,
                "table-uuid": "u-1",
                "location": "s3://t-1--table-s3",
                "last-sequence-number": 0,
                "last-updated-ms": 7,
                "last-column-id": 8,
                "current-schema-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                    {"id": 6, "name": "id", "required": true, "type": "long"},
                    {"id": 5, "name": "at", "required": false, "type": "timestamptz"},
                    {"id": 7, "name": "amount", "required": false, "type": "decimal(10, 2)"},
                    {"id": 8, "name": "raw", "required": false, "type": "fixed[16]"},
                ]}],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "last-partition-id": 999,
                "default-sort-order-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "properties": {"owner": "sales"},
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "refs": {},
            })
        );
    }

    #[test]
    fn a_nested_schema_a_partition_spec_and_a_write_order_are_checked_and_kept() {
        let file = made(json!({"iceberg": {
            "schemaV2": {"type": "struct", "schema-id": 3, "identifier-field-ids": [1, 11], "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "day", "required": false, "type": "date"},
                {"id": 3, "name": "tags", "required": false, "type":
                    {"type": "list", "element-id": 4, "element": "fixed[16]", "element-required": false}},
                {"id": 5, "name": "attrs", "required": false, "type":
                    {"type": "map", "key-id": 6, "key": "string", "value-id": 7,
                     "value": {"type": "struct", "fields": [
                        {"id": 8, "name": "v", "required": false, "type": "decimal(9,0)"}]},
                     "value-required": false}},
                {"id": 9, "name": "point", "required": true, "type": {"type": "struct", "fields": [
                    {"id": 10, "name": "x", "required": false, "type": "double"},
                    {"id": 11, "name": "key", "required": true, "type": "string"}]}},
            ]},
            "partitionSpec": {"spec-id": 2, "fields": [
                {"source-id": 2, "transform": "month", "name": "day_month"},
                {"source-id": 1, "transform": "bucket[16]", "name": "id_bucket", "field-id": 1005},
                {"source-id": 2, "transform": "identity", "name": "day"},
            ]},
            "writeOrder": {"fields": [
                {"source-id": 10, "transform": "identity", "direction": "desc", "null-order": "nulls-last"}]},
        }}))
        .unwrap();
        assert_eq!(file["last-column-id"], 11);
        assert_eq!(file["current-schema-id"], 3);
        let schema = &file["schemas"][0];
        assert_eq!(schema["schema-id"], 3);
        assert_eq!(schema["identifier-field-ids"], json!([1, 11]));
        assert_eq!(schema["fields"][2]["type"]["element"], "fixed[16]");
        assert_eq!(
            schema["fields"][3]["type"]["value"]["fields"][0]["type"],
            "decimal(9, 0)"
        );
        assert_eq!(file["default-spec-id"], 2);
        assert_eq!(
            file["partition-specs"][0]["fields"],
            json!([
                {"source-id": 2, "field-id": 1006, "transform": "month", "name": "day_month"},
                {"source-id": 1, "field-id": 1005, "transform": "bucket[16]", "name": "id_bucket"},
                {"source-id": 2, "field-id": 1007, "transform": "identity", "name": "day"},
            ])
        );
        assert_eq!(file["last-partition-id"], 1007);
        assert_eq!(file["default-sort-order-id"], 1);
        assert_eq!(file["sort-orders"][0]["fields"][0]["source-id"], 10);
        // A partition field id given below 1000: those assigned still start
        // there.
        let low = made(json!({"iceberg": {
            "schema": {"fields": [{"name": "id", "type": "long", "id": 1}]},
            "partitionSpec": {"fields": [
                {"source-id": 1, "transform": "identity", "name": "a", "field-id": 5},
                {"source-id": 1, "transform": "bucket[2]", "name": "b"},
            ]},
        }}))
        .unwrap();
        assert_eq!(low["partition-specs"][0]["fields"][1]["field-id"], 1000);
        assert_eq!(low["last-partition-id"], 1000);
        // The unsorted order, said.
        let unsorted = made(json!({"iceberg": {
            "schema": {"fields": [{"name": "id", "type": "long"}]},
            "writeOrder": {"order-id": 0, "fields": []},
        }}))
        .unwrap();
        assert_eq!(
            unsorted["sort-orders"],
            json!([{"order-id": 0, "fields": []}])
        );
    }

    #[test]
    fn what_is_not_an_iceberg_tables_metadata_is_refused() {
        let schema = |fields: Value| json!({"iceberg": {"schema": {"fields": fields}}});
        let long = json!([{"name": "id", "type": "long", "id": 1}]);
        let typed = json!([{"name": "id", "type": "long", "id": 1},
            {"name": "day", "type": "date", "id": 2}, {"name": "ratio", "type": "double", "id": 3}]);
        let partitioned = |field: Value| {
            json!({"iceberg": {"schema": {"fields": typed.clone()},
                "partitionSpec": {"fields": [field]}}})
        };
        for (bad, why) in [
            (json!({}), "metadata has iceberg."),
            (json!({"iceberg": {}}), "one of schema and schemaV2"),
            (json!({"other": 1, "iceberg": {}}), "other is not a member"),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "schemaV2":
                {"type": "struct", "fields": []}}}),
                "one of schema and schemaV2",
            ),
            (
                schema(json!([{"name": "x", "type": "varchar"}])),
                "varchar is not",
            ),
            (
                schema(json!([{"name": "x", "type": "list"}])),
                "list is not",
            ),
            // Types as Iceberg's readers parse them, and no other way.
            (
                schema(json!([{"name": "x", "type": "decimal(40, 2)"}])),
                "is not",
            ),
            (
                schema(json!([{"name": "x", "type": "decimal(10, 12)"}])),
                "is not",
            ),
            (
                schema(json!([{"name": "x", "type": "decimal(10,2]"}])),
                "is not",
            ),
            (
                schema(json!([{"name": "x", "type": "decimal( 10, 2)"}])),
                "is not",
            ),
            (
                schema(json!([{"name": "x", "type": "decimal(+10, 2)"}])),
                "is not",
            ),
            (schema(json!([{"name": "x", "type": "fixed[0]"}])), "is not"),
            (schema(json!([{"name": "x", "type": "fixed[4)"}])), "is not"),
            (
                schema(json!([{"name": "x", "type": "fixed[ 4 ]"}])),
                "is not",
            ),
            (
                schema(json!([{"name": "x", "type": "fixed[+4]"}])),
                "is not",
            ),
            (
                schema(json!([{"name": "x", "type": "int", "id": 1},
                {"name": "y", "type": "int", "id": 1}])),
                "used more than once",
            ),
            (
                schema(json!([{"name": "x", "type": "int"}, {"name": "x", "type": "long"}])),
                "named more than once",
            ),
            (
                schema(json!([{"name": "x", "type": "int", "id": 0}])),
                "positive whole number",
            ),
            (
                schema(json!([{"name": "x", "type": "int", "id": 2147483648_i64}])),
                "positive whole number",
            ),
            // An id to assign past the 32-bit ids.
            (
                schema(json!([{"name": "x", "type": "int", "id": 2147483647_i64},
                {"name": "y", "type": "int"}])),
                "No field id is left",
            ),
            (
                schema(json!([{"name": "x", "type": "int", "required": "yes"}])),
                "true or false",
            ),
            (schema(json!([{"type": "int"}])), "name is a string"),
            (
                json!({"iceberg": {"schemaV2": {"type": "list", "fields": []}}}),
                "type is struct",
            ),
            (
                json!({"iceberg": {"schemaV2": {"type": "struct", "fields": [
                {"name": "x", "required": true, "type": "int"}]}}}),
                "id is required",
            ),
            (
                json!({"iceberg": {"schemaV2": {"type": "struct", "fields": [
                {"id": 1, "name": "x", "required": true, "type":
                    {"type": "list", "element-id": 1, "element": "int", "element-required": true}}]}}}),
                "used more than once",
            ),
            (
                json!({"iceberg": {"schemaV2": {"type": "struct", "identifier-field-ids": [1], "fields": [
                {"id": 1, "name": "x", "required": false, "type": "int"}]}}}),
                "not a required primitive field",
            ),
            // Identifier fields: not float or double, nor in an optional struct.
            (
                json!({"iceberg": {"schemaV2": {"type": "struct", "identifier-field-ids": [1], "fields": [
                {"id": 1, "name": "x", "required": true, "type": "double"}]}}}),
                "not a required primitive field",
            ),
            (
                json!({"iceberg": {"schemaV2": {"type": "struct", "identifier-field-ids": [2], "fields": [
                {"id": 1, "name": "s", "required": false, "type": {"type": "struct", "fields": [
                    {"id": 2, "name": "x", "required": true, "type": "int"}]}}]}}}),
                "not a required primitive field",
            ),
            (
                partitioned(json!({"source-id": 9, "transform": "identity", "name": "p"})),
                "source field 9",
            ),
            (
                partitioned(json!({"source-id": 1, "transform": "bucket[0]", "name": "p"})),
                "not an Iceberg transform",
            ),
            (
                partitioned(json!({"source-id": 1, "transform": "bucket[+16]", "name": "p"})),
                "not an Iceberg transform",
            ),
            (
                partitioned(json!({"source-id": 1, "transform": "truncate[ 4]", "name": "p"})),
                "not an Iceberg transform",
            ),
            // Transforms on the types they apply to.
            (
                partitioned(json!({"source-id": 1, "transform": "month", "name": "p"})),
                "does not apply to the long field id",
            ),
            (
                partitioned(json!({"source-id": 2, "transform": "hour", "name": "p"})),
                "does not apply to the date field day",
            ),
            (
                partitioned(json!({"source-id": 3, "transform": "bucket[4]", "name": "p"})),
                "does not apply to the double field ratio",
            ),
            (
                partitioned(json!({"source-id": 3, "transform": "truncate[4]", "name": "p"})),
                "does not apply",
            ),
            // A schema field's name: only for its own identity.
            (
                partitioned(json!({"source-id": 2, "transform": "month", "name": "day"})),
                "has a schema field's name",
            ),
            (
                partitioned(json!({"source-id": 1, "transform": "identity", "name": "day"})),
                "has a schema field's name",
            ),
            // In a list (a struct's field in it too): no source.
            (
                json!({"iceberg": {"schemaV2": {"type": "struct", "fields": [
                {"id": 1, "name": "x", "required": true, "type":
                    {"type": "list", "element-id": 2, "element-required": true, "element":
                        {"type": "struct", "fields": [{"id": 3, "name": "y", "required": true, "type": "int"}]}}}]},
                "partitionSpec": {"fields": [{"source-id": 3, "transform": "identity", "name": "p"}]}}}),
                "source field 3",
            ),
            (
                json!({"iceberg": {"schemaV2": {"type": "struct", "fields": [
                {"id": 1, "name": "x", "required": true, "type":
                    {"type": "list", "element-id": 2, "element": "int", "element-required": true}}]},
                "partitionSpec": {"fields": [{"source-id": 2, "transform": "identity", "name": "p"}]}}}),
                "source field 2",
            ),
            (
                json!({"iceberg": {"schema": {"fields": typed.clone()}, "partitionSpec": {"fields": [
                {"source-id": 1, "transform": "identity", "name": "p"},
                {"source-id": 1, "transform": "bucket[4]", "name": "p"}]}}}),
                "named more than once",
            ),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "writeOrder": {"order-id": 0,
                "fields": [{"source-id": 1, "transform": "identity", "direction": "asc",
                "null-order": "nulls-first"}]}}}),
                "unsorted order",
            ),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "writeOrder": {"fields": []}}}),
                "unsorted order",
            ),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "writeOrder": {"fields": [
                {"source-id": 1, "transform": "identity", "direction": "up",
                "null-order": "nulls-first"}]}}}),
                "asc or desc",
            ),
            (
                json!({"iceberg": {"schema": {"fields": typed.clone()}, "writeOrder": {"fields": [
                {"source-id": 3, "transform": "day", "direction": "asc",
                "null-order": "nulls-first"}]}}}),
                "does not apply",
            ),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "properties": {"a": 1}}}),
                "properties are strings",
            ),
        ] {
            let err = made(bad.clone()).expect_err(&bad.to_string());
            assert!(err.contains(why), "{bad}: {err}");
        }
    }
}
