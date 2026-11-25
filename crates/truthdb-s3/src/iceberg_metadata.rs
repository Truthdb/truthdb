//! The first Iceberg metadata file (format version 2) of an S3 Tables table
//! made with CreateTable's `metadata`: its schema, partition spec, write
//! order and properties, checked and with the ids the request leaves out
//! assigned.

use std::collections::BTreeSet;

use serde_json::{Map, Value, json};

/// The partition field ids assigned start here (as Iceberg's reference
/// implementation's do); an unpartitioned table's last is one below.
const FIRST_PARTITION_FIELD_ID: i64 = 1000;

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
    let (schema, ids, primitives) = match (iceberg.get("schema"), iceberg.get("schemaV2")) {
        (Some(schema), None) => schema_v1(schema)?,
        (None, Some(schema)) => schema_v2(schema)?,
        _ => return Err("metadata.iceberg has one of schema and schemaV2.".to_string()),
    };
    let last_column_id = ids.iter().max().copied().unwrap_or(0);
    let schema_id = schema["schema-id"].clone();
    let (spec, last_partition_id) = match iceberg.get("partitionSpec") {
        Some(spec) => partition_spec(spec, &primitives)?,
        None => (
            json!({ "spec-id": 0, "fields": [] }),
            FIRST_PARTITION_FIELD_ID - 1,
        ),
    };
    let order = match iceberg.get("writeOrder") {
        Some(order) => write_order(order, &primitives)?,
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
            Some(id) if id > 0 && id <= i64::from(i32::MAX) => Ok(Some(id)),
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

/// Whether `name` is one of Iceberg's primitive types (format version 2).
fn primitive(name: &str) -> bool {
    let sized = |prefix: &str, rest: &dyn Fn(&str) -> bool| {
        name.strip_prefix(prefix)
            .and_then(|args| args.strip_suffix(']').or_else(|| args.strip_suffix(')')))
            .is_some_and(rest)
    };
    let number = |text: &str| text.trim().parse::<u32>().is_ok_and(|n| n > 0);
    matches!(
        name,
        "boolean"
            | "int"
            | "long"
            | "float"
            | "double"
            | "date"
            | "time"
            | "timestamp"
            | "timestamptz"
            | "string"
            | "uuid"
            | "binary"
    ) || sized("fixed[", &number)
        || sized("decimal(", &|args: &str| {
            args.split_once(',').is_some_and(|(precision, scale)| {
                let (precision, scale) =
                    (precision.trim().parse::<u32>(), scale.trim().parse::<u32>());
                matches!((precision, scale), (Ok(p), Ok(s)) if (1..=38).contains(&p) && s <= p)
            })
        })
}

/// Field ids, each new.
fn add_id(ids: &mut BTreeSet<i64>, id: i64) -> Result<(), String> {
    if ids.insert(id) {
        Ok(())
    } else {
        Err(format!("The field id {id} is used more than once."))
    }
}

/// A schema of primitive fields (`schema`): its Iceberg form, every field
/// id, and the primitive ones (all of them). Ids left out are assigned
/// after the highest given.
fn schema_v1(schema: &Value) -> Result<(Value, BTreeSet<i64>, BTreeSet<i64>), String> {
    let what = "metadata.iceberg.schema";
    let schema = object(schema, what)?;
    only(schema, what, &["fields"])?;
    let fields = array(schema.get("fields"), &format!("{what}.fields"))?;
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    let mut given = Vec::new();
    for field in fields {
        let what = format!("{what}.fields[]");
        let field = object(field, &what)?;
        only(field, &what, &["id", "name", "type", "required"])?;
        let name = text(field, "name", &what)?;
        let kind = text(field, "type", &what)?;
        if !primitive(kind) {
            return Err(format!("{kind} is not an Iceberg primitive type."));
        }
        if !names.insert(name) {
            return Err(format!("The field {name} is named more than once."));
        }
        let field_id = id(field.get("id"), &format!("{what}.id"))?;
        if let Some(field_id) = field_id {
            add_id(&mut ids, field_id)?;
        }
        given.push((field_id, name, kind, flag(field, "required", &what)?));
    }
    let mut next = ids.iter().max().copied().unwrap_or(0) + 1;
    let mut out = Vec::new();
    for (field_id, name, kind, required) in given {
        let field_id = field_id.unwrap_or_else(|| {
            next += 1;
            next - 1
        });
        ids.insert(field_id);
        out.push(json!({ "id": field_id, "name": name, "required": required, "type": kind }));
    }
    let schema = json!({ "type": "struct", "schema-id": 0, "fields": out });
    Ok((schema, ids.clone(), ids))
}

/// A schema with nested types (`schemaV2`): as given (its schema id 0 if
/// none), every field id in it, and the primitive ones.
fn schema_v2(schema: &Value) -> Result<(Value, BTreeSet<i64>, BTreeSet<i64>), String> {
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
            .filter(|id| *id >= 0)
            .ok_or_else(|| format!("{what}.schema-id is a whole number."))?,
    };
    let (mut ids, mut primitives) = (BTreeSet::new(), BTreeSet::new());
    let mut required_primitives = BTreeSet::new();
    let fields = array(map.get("fields"), &format!("{what}.fields"))?;
    struct_fields(
        fields,
        what,
        (&mut ids, &mut primitives),
        Some(&mut required_primitives),
    )?;
    let mut out = map.clone();
    out.insert("schema-id".to_string(), json!(schema_id));
    if let Some(identifiers) = map.get("identifier-field-ids") {
        let what = format!("{what}.identifier-field-ids");
        for identifier in array(Some(identifiers), &what)? {
            let identifier = id(Some(identifier), &what)?.unwrap_or_default();
            // Iceberg: required primitive fields of the schema's own.
            if !required_primitives.contains(&identifier) {
                return Err(format!(
                    "The identifier field {identifier} is not a required primitive field of the schema."
                ));
            }
        }
    }
    Ok((Value::Object(out), ids, primitives))
}

/// A struct's fields: each id new, each type one (`top` collects the
/// required primitive fields of the schema's own struct).
fn struct_fields(
    fields: &[Value],
    what: &str,
    (ids, primitives): (&mut BTreeSet<i64>, &mut BTreeSet<i64>),
    mut top: Option<&mut BTreeSet<i64>>,
) -> Result<(), String> {
    let mut names = BTreeSet::new();
    for field in fields {
        let what = format!("{what}.fields[]");
        let field = object(field, &what)?;
        only(field, &what, &["id", "name", "required", "type", "doc"])?;
        let name = text(field, "name", &what)?;
        if !names.insert(name) {
            return Err(format!("The field {name} is named more than once."));
        }
        let field_id = id(field.get("id"), &format!("{what}.id"))?
            .ok_or_else(|| format!("{what}.id is required."))?;
        if !field.get("required").is_some_and(Value::is_boolean) {
            return Err(format!("{what}.required is true or false."));
        }
        if field.get("doc").is_some_and(|doc| !doc.is_string()) {
            return Err(format!("{what}.doc is a string."));
        }
        add_id(ids, field_id)?;
        let kind = field
            .get("type")
            .ok_or_else(|| format!("{what}.type is required."))?;
        if nested_type(kind, &what, (ids, primitives))? {
            primitives.insert(field_id);
            if flag(field, "required", &what)?
                && let Some(top) = top.as_deref_mut()
            {
                top.insert(field_id);
            }
        }
    }
    Ok(())
}

/// A field's (or element's, key's, value's) type: whether it is primitive.
/// What is in a list or a map is no partition or sort source ("cannot be
/// contained in a map or list"): only its ids are collected.
fn nested_type(
    kind: &Value,
    what: &str,
    (ids, primitives): (&mut BTreeSet<i64>, &mut BTreeSet<i64>),
) -> Result<bool, String> {
    if let Some(name) = kind.as_str() {
        return if primitive(name) {
            Ok(true)
        } else {
            Err(format!("{name} is not an Iceberg type."))
        };
    }
    let what = format!("{what}.type");
    let map = object(kind, &what)?;
    match map.get("type").and_then(Value::as_str) {
        Some("struct") => {
            only(map, &what, &["type", "fields"])?;
            let fields = array(map.get("fields"), &format!("{what}.fields"))?;
            struct_fields(fields, &what, (ids, primitives), None)?;
        }
        Some("list") => {
            only(
                map,
                &what,
                &["type", "element-id", "element", "element-required"],
            )?;
            let element = id(map.get("element-id"), &format!("{what}.element-id"))?
                .ok_or_else(|| format!("{what}.element-id is required."))?;
            add_id(ids, element)?;
            if !map.get("element-required").is_some_and(Value::is_boolean) {
                return Err(format!("{what}.element-required is true or false."));
            }
            let element_type = map
                .get("element")
                .ok_or_else(|| format!("{what}.element is required."))?;
            nested_type(element_type, &what, (ids, &mut BTreeSet::new()))?;
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
                add_id(ids, part)?;
                let part_type = map
                    .get(type_name)
                    .ok_or_else(|| format!("{what}.{type_name} is required."))?;
                nested_type(part_type, &what, (ids, &mut BTreeSet::new()))?;
            }
            if !map.get("value-required").is_some_and(Value::is_boolean) {
                return Err(format!("{what}.value-required is true or false."));
            }
        }
        _ => return Err(format!("{what} is a type name, a struct, a list or a map.")),
    }
    Ok(false)
}

/// Whether `transform` is one of Iceberg's partition (and sort) transforms.
fn transform(transform: &str) -> bool {
    let sized = |prefix: &str| {
        transform
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(']'))
            .is_some_and(|n| n.parse::<u32>().is_ok_and(|n| n > 0))
    };
    matches!(
        transform,
        "identity" | "year" | "month" | "day" | "hour" | "void"
    ) || sized("bucket[")
        || sized("truncate[")
}

/// A field's source: a primitive field of the schema.
fn source(
    field: &Map<String, Value>,
    what: &str,
    primitives: &BTreeSet<i64>,
) -> Result<i64, String> {
    let source = id(field.get("source-id"), &format!("{what}.source-id"))?
        .ok_or_else(|| format!("{what}.source-id is required."))?;
    if !primitives.contains(&source) {
        return Err(format!(
            "The source field {source} is not a primitive field of the schema."
        ));
    }
    let kind = text(field, "transform", what)?;
    if !transform(kind) {
        return Err(format!("{kind} is not an Iceberg transform."));
    }
    Ok(source)
}

/// The partition spec (spec id 0 if none), and the last partition field id.
/// Field ids left out are assigned from 1000, after the highest given.
fn partition_spec(spec: &Value, primitives: &BTreeSet<i64>) -> Result<(Value, i64), String> {
    let what = "metadata.iceberg.partitionSpec";
    let map = object(spec, what)?;
    only(map, what, &["fields", "spec-id"])?;
    let spec_id = match map.get("spec-id") {
        None => 0,
        Some(value) => value
            .as_i64()
            .filter(|id| *id >= 0)
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
        let source_id = source(field, &what, primitives)?;
        let name = text(field, "name", &what)?;
        if !names.insert(name) {
            return Err(format!(
                "The partition field {name} is named more than once."
            ));
        }
        let field_id = id(field.get("field-id"), &format!("{what}.field-id"))?;
        if let Some(field_id) = field_id {
            add_id(&mut ids, field_id)?;
        }
        given.push((source_id, text(field, "transform", &what)?, name, field_id));
    }
    let mut next = ids
        .iter()
        .max()
        .map_or(FIRST_PARTITION_FIELD_ID, |last| last + 1)
        .max(FIRST_PARTITION_FIELD_ID);
    let mut last = FIRST_PARTITION_FIELD_ID - 1;
    let mut out = Vec::new();
    for (source_id, kind, name, field_id) in given {
        let field_id = field_id.unwrap_or_else(|| {
            next += 1;
            next - 1
        });
        last = last.max(field_id);
        out.push(json!({
            "source-id": source_id,
            "field-id": field_id,
            "transform": kind,
            "name": name,
        }));
    }
    Ok((json!({ "spec-id": spec_id, "fields": out }), last))
}

/// The write order: its order id (1 if none; 0 is the unsorted order's)
/// and fields.
fn write_order(order: &Value, primitives: &BTreeSet<i64>) -> Result<Value, String> {
    let what = "metadata.iceberg.writeOrder";
    let map = object(order, what)?;
    only(map, what, &["order-id", "fields"])?;
    let order_id = match map.get("order-id") {
        None => 1,
        Some(value) => value.as_i64().filter(|id| *id > 0).ok_or_else(|| {
            format!("{what}.order-id is a positive whole number (0 is unsorted).")
        })?,
    };
    let fields = array(map.get("fields"), &format!("{what}.fields"))?;
    if fields.is_empty() {
        return Err(format!("{what}.fields has a field."));
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
        let source_id = source(field, &what, primitives)?;
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
            "transform": text(field, "transform", &what)?,
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
            {"name": "amount", "type": "decimal(10, 2)"},
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
                "last-column-id": 7,
                "current-schema-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                    {"id": 6, "name": "id", "required": true, "type": "long"},
                    {"id": 5, "name": "at", "required": false, "type": "timestamptz"},
                    {"id": 7, "name": "amount", "required": false, "type": "decimal(10, 2)"},
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
            "schemaV2": {"type": "struct", "schema-id": 3, "identifier-field-ids": [1], "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "day", "required": false, "type": "date"},
                {"id": 3, "name": "tags", "required": false, "type":
                    {"type": "list", "element-id": 4, "element": "string", "element-required": false}},
                {"id": 5, "name": "attrs", "required": false, "type":
                    {"type": "map", "key-id": 6, "key": "string", "value-id": 7,
                     "value": {"type": "struct", "fields": [
                        {"id": 8, "name": "v", "required": false, "type": "int"}]},
                     "value-required": false}},
                {"id": 9, "name": "point", "required": false, "type": {"type": "struct", "fields": [
                    {"id": 10, "name": "x", "required": false, "type": "double"}]}},
            ]},
            "partitionSpec": {"spec-id": 2, "fields": [
                {"source-id": 2, "transform": "month", "name": "day_month"},
                {"source-id": 1, "transform": "bucket[16]", "name": "id_bucket", "field-id": 1005},
            ]},
            "writeOrder": {"fields": [
                {"source-id": 10, "transform": "identity", "direction": "desc", "null-order": "nulls-last"}]},
        }}))
        .unwrap();
        assert_eq!(file["last-column-id"], 10);
        assert_eq!(file["current-schema-id"], 3);
        assert_eq!(file["schemas"][0]["schema-id"], 3);
        assert_eq!(file["schemas"][0]["fields"][2]["type"]["element-id"], 4);
        assert_eq!(file["default-spec-id"], 2);
        assert_eq!(
            file["partition-specs"][0]["fields"],
            json!([
                {"source-id": 2, "field-id": 1006, "transform": "month", "name": "day_month"},
                {"source-id": 1, "field-id": 1005, "transform": "bucket[16]", "name": "id_bucket"},
            ])
        );
        assert_eq!(file["last-partition-id"], 1006);
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
        assert_eq!(file["default-sort-order-id"], 1);
        assert_eq!(file["sort-orders"][0]["fields"][0]["source-id"], 10);
    }

    #[test]
    fn what_is_not_an_iceberg_tables_metadata_is_refused() {
        let schema = |fields: Value| json!({"iceberg": {"schema": {"fields": fields}}});
        let long = json!([{"name": "id", "type": "long", "id": 1}]);
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
            (
                schema(json!([{"name": "x", "type": "decimal(40, 2)"}])),
                "is not",
            ),
            (schema(json!([{"name": "x", "type": "fixed[0]"}])), "is not"),
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
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "partitionSpec": {"fields": [
                {"source-id": 9, "transform": "identity", "name": "p"}]}}}),
                "source field 9",
            ),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "partitionSpec": {"fields": [
                {"source-id": 1, "transform": "bucket[0]", "name": "p"}]}}}),
                "not an Iceberg transform",
            ),
            // In a list: no source.
            (
                json!({"iceberg": {"schemaV2": {"type": "struct", "fields": [
                {"id": 1, "name": "x", "required": true, "type":
                    {"type": "list", "element-id": 2, "element": "int", "element-required": true}}]},
                "partitionSpec": {"fields": [{"source-id": 2, "transform": "identity", "name": "p"}]}}}),
                "source field 2",
            ),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "partitionSpec": {"fields": [
                {"source-id": 1, "transform": "identity", "name": "p"},
                {"source-id": 1, "transform": "bucket[4]", "name": "p"}]}}}),
                "named more than once",
            ),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "writeOrder": {"order-id": 0,
                "fields": [{"source-id": 1, "transform": "identity", "direction": "asc",
                "null-order": "nulls-first"}]}}}),
                "0 is unsorted",
            ),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "writeOrder": {"fields": []}}}),
                "has a field",
            ),
            (
                json!({"iceberg": {"schema": {"fields": long.clone()}, "writeOrder": {"fields": [
                {"source-id": 1, "transform": "identity", "direction": "up",
                "null-order": "nulls-first"}]}}}),
                "asc or desc",
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
