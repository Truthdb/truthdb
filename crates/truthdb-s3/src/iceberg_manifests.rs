//! Reading an Iceberg table's manifest lists and manifests (Avro object
//! container files): the files they name, so maintenance knows what a table
//! still references.

use apache_avro::Reader;
use apache_avro::types::Value as Avro;

/// The field `name` of record `record`, a union's value unwrapped.
fn field<'a>(record: &'a Avro, name: &str) -> Option<&'a Avro> {
    let Avro::Record(fields) = record else {
        return None;
    };
    let mut value = &fields.iter().find(|(field, _)| field == name)?.1;
    while let Avro::Union(_, inner) = value {
        value = inner;
    }
    Some(value)
}

fn text(value: Option<&Avro>) -> Option<&str> {
    match value? {
        Avro::String(text) => Some(text),
        _ => None,
    }
}

/// Each record of an Avro object container file.
fn records(bytes: &[u8], what: &str) -> Result<Vec<Avro>, String> {
    let reader = Reader::new(bytes).map_err(|err| format!("{what} is not Avro: {err}"))?;
    reader
        .map(|record| record.map_err(|err| format!("{what} cannot be read: {err}")))
        .collect()
}

/// The manifests a manifest list names (its entries' `manifest_path`).
pub(crate) fn manifest_list(bytes: &[u8]) -> Result<Vec<String>, String> {
    records(bytes, "A manifest list")?
        .iter()
        .map(|entry| {
            text(field(entry, "manifest_path"))
                .map(str::to_string)
                .ok_or_else(|| "A manifest list entry has no manifest_path.".to_string())
        })
        .collect()
}

/// The data and delete files a manifest names: every entry's
/// `data_file.file_path`, deleted entries' too (a file one names may be an
/// older snapshot's still).
pub(crate) fn manifest_files(bytes: &[u8]) -> Result<Vec<String>, String> {
    records(bytes, "A manifest")?
        .iter()
        .map(|entry| {
            field(entry, "data_file")
                .and_then(|file| text(field(file, "file_path")))
                .map(str::to_string)
                .ok_or_else(|| "A manifest entry has no data_file.file_path.".to_string())
        })
        .collect()
}

/// An Avro object container file as read: its writer schema, its user
/// metadata (Iceberg's `schema`, `partition-spec`, `format-version`,
/// `content` and the like) and its records.
pub(crate) struct AvroFile {
    pub(crate) schema: apache_avro::Schema,
    pub(crate) metadata: Vec<(String, Vec<u8>)>,
    pub(crate) records: Vec<Avro>,
}

/// Reads `bytes` as an Avro object container file.
pub(crate) fn avro_file(bytes: &[u8], what: &str) -> Result<AvroFile, String> {
    let reader = Reader::new(bytes).map_err(|err| format!("{what} is not Avro: {err}"))?;
    let schema = reader.writer_schema().clone();
    let mut metadata: Vec<(String, Vec<u8>)> = reader
        .user_metadata()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    metadata.sort();
    let records = reader
        .map(|record| record.map_err(|err| format!("{what} cannot be read: {err}")))
        .collect::<Result<_, _>>()?;
    Ok(AvroFile {
        schema,
        metadata,
        records,
    })
}

/// `records` as an Avro object container file of `schema` with `metadata`,
/// deflated (Iceberg's default Avro codec).
pub(crate) fn write_avro(
    schema: &apache_avro::Schema,
    metadata: &[(String, Vec<u8>)],
    records: &[Avro],
) -> Result<Vec<u8>, String> {
    let fail = |err: apache_avro::Error| format!("An Avro file cannot be written: {err}");
    let mut writer = apache_avro::Writer::with_codec(
        schema,
        Vec::new(),
        apache_avro::Codec::Deflate(Default::default()),
    )
    .map_err(fail)?;
    for (key, value) in metadata {
        writer.add_user_metadata(key.clone(), value).map_err(fail)?;
    }
    for record in records {
        writer.append_value_ref(record).map_err(fail)?;
    }
    writer.into_inner().map_err(fail)
}

/// The record fields of `schema` (a record, or a nullable one), with the
/// named schemas `names` resolves references through.
fn record_fields<'a>(
    schema: &'a apache_avro::Schema,
    names: &'a apache_avro::schema::NamesRef<'a>,
) -> Option<&'a [apache_avro::schema::RecordField]> {
    use apache_avro::Schema;
    match schema {
        Schema::Record(record) => Some(&record.fields),
        Schema::Ref { name } => record_fields(names.get(name)?, names),
        Schema::Union(union) => union
            .variants()
            .iter()
            .find_map(|variant| record_fields(variant, names)),
        _ => None,
    }
}

/// An Iceberg field's id, as its Avro field carries it.
fn field_id(field: &apache_avro::schema::RecordField) -> Option<i64> {
    field.custom_attributes.get("field-id")?.as_i64()
}

/// The names of the fields of record schema `schema` (within `root`), by
/// their Iceberg field ids.
pub(crate) fn ids(
    root: &apache_avro::Schema,
    schema: &apache_avro::Schema,
) -> Result<std::collections::HashMap<i64, String>, String> {
    let resolved = apache_avro::schema::ResolvedSchema::try_from(root)
        .map_err(|err| format!("An Avro schema cannot be read: {err}"))?;
    let fields = record_fields(schema, resolved.get_names())
        .ok_or_else(|| "An Avro schema is not a record.".to_string())?;
    Ok(fields
        .iter()
        .filter_map(|field| Some((field_id(field)?, field.name.clone())))
        .collect())
}

/// The value of the field of id `id` of `record`, by the names [`ids`]
/// gave, a union's value unwrapped.
pub(crate) fn get<'a>(
    record: &'a Avro,
    names: &std::collections::HashMap<i64, String>,
    id: i64,
) -> Option<&'a Avro> {
    field(record, names.get(&id)?)
}

/// What a record field is set to in [`build`]: a value (wrapped into its
/// union when the field's schema is one), null, or the template's.
pub(crate) enum Set {
    To(Avro),
    Null,
}

/// A record of `schema` (a record schema within `root`): each field set
/// by its Iceberg field id from `set`, else as it is in `template` (by
/// name), else null where the field is nullable. A field that is none of
/// these is an error: what it should hold is not known.
pub(crate) fn build(
    root: &apache_avro::Schema,
    schema: &apache_avro::Schema,
    template: Option<&Avro>,
    set: &[(i64, Set)],
) -> Result<Avro, String> {
    use apache_avro::Schema;
    let resolved = apache_avro::schema::ResolvedSchema::try_from(root)
        .map_err(|err| format!("An Avro schema cannot be read: {err}"))?;
    let names = resolved.get_names();
    let fields = record_fields(schema, names)
        .ok_or_else(|| "An Avro schema is not a record.".to_string())?;
    let mut out = Vec::with_capacity(fields.len());
    for field in fields {
        let given = field_id(field)
            .and_then(|id| set.iter().find(|(of, _)| *of == id).map(|(_, value)| value));
        let null_at = match &field.schema {
            Schema::Union(union) => union
                .variants()
                .iter()
                .position(|variant| matches!(variant, Schema::Null)),
            _ => None,
        };
        let wrap = |value: Avro| -> Avro {
            match (&field.schema, null_at) {
                (Schema::Union(union), Some(null)) => {
                    let at = (0..union.variants().len())
                        .find(|&at| at != null)
                        .unwrap_or(null);
                    Avro::Union(at as u32, Box::new(value))
                }
                _ => value,
            }
        };
        let value = match given {
            Some(Set::To(value)) => wrap(value.clone()),
            Some(Set::Null) => match null_at {
                Some(null) => Avro::Union(null as u32, Box::new(Avro::Null)),
                None => {
                    return Err(format!(
                        "The manifest field {} cannot be left out.",
                        field.name
                    ));
                }
            },
            None => match template.and_then(|template| {
                let Avro::Record(fields) = template else {
                    return None;
                };
                fields
                    .iter()
                    .find(|(name, _)| *name == field.name)
                    .map(|(_, value)| value.clone())
            }) {
                Some(value) => value,
                None => match null_at {
                    Some(null) => Avro::Union(null as u32, Box::new(Avro::Null)),
                    None => {
                        return Err(format!("The manifest field {} is not known.", field.name));
                    }
                },
            },
        };
        out.push((field.name.clone(), value));
    }
    Ok(Avro::Record(out))
}

/// The schema of the field of id `id` in record schema `schema` (within
/// `root`), a nullable one's non-null variant.
pub(crate) fn field_schema(
    root: &apache_avro::Schema,
    schema: &apache_avro::Schema,
    id: i64,
) -> Option<apache_avro::Schema> {
    use apache_avro::Schema;
    let resolved = apache_avro::schema::ResolvedSchema::try_from(root).ok()?;
    let names = resolved.get_names();
    let fields = record_fields(schema, names)?;
    let field = fields.iter().find(|field| field_id(field) == Some(id))?;
    let mut schema = &field.schema;
    if let Schema::Union(union) = schema {
        schema = union
            .variants()
            .iter()
            .find(|variant| !matches!(variant, Schema::Null))?;
    }
    if let Schema::Ref { name } = schema {
        schema = names.get(name)?;
    }
    Some(schema.clone())
}

/// An Iceberg map of `id`s to values, as Avro holds one: an array of
/// records of a key and a value (`map_schema` the array's schema, within
/// `root`).
pub(crate) fn iceberg_map(
    root: &apache_avro::Schema,
    map_schema: &apache_avro::Schema,
    entries: Vec<(i32, Avro)>,
) -> Result<Avro, String> {
    use apache_avro::Schema;
    let Schema::Array(array) = map_schema else {
        return Err("An Iceberg map is not an Avro array.".to_string());
    };
    let resolved = apache_avro::schema::ResolvedSchema::try_from(root)
        .map_err(|err| format!("An Avro schema cannot be read: {err}"))?;
    let fields = record_fields(&array.items, resolved.get_names())
        .filter(|fields| fields.len() == 2)
        .ok_or_else(|| "An Iceberg map's entries are not key and value records.".to_string())?;
    Ok(Avro::Array(
        entries
            .into_iter()
            .map(|(key, value)| {
                Avro::Record(vec![
                    (fields[0].name.clone(), Avro::Int(key)),
                    (fields[1].name.clone(), value),
                ])
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{Codec, Schema, Writer, types::Record};

    #[test]
    fn manifest_lists_and_manifests_name_their_files() {
        // Iceberg's shapes, as far as they are read (others' fields kept).
        let list = Schema::parse_str(
            r#"{"type": "record", "name": "manifest_file", "fields": [
                {"name": "manifest_path", "type": "string"},
                {"name": "manifest_length", "type": "long"},
                {"name": "content", "type": "int"}]}"#,
        )
        .unwrap();
        let mut writer =
            Writer::with_codec(&list, Vec::new(), Codec::Deflate(Default::default())).unwrap();
        for path in ["s3://t/metadata/m1.avro", "s3://t/metadata/m2.avro"] {
            let mut record = Record::new(&list).unwrap();
            record.put("manifest_path", path);
            record.put("manifest_length", 10_i64);
            record.put("content", 0);
            writer.append(record).unwrap();
        }
        let bytes = writer.into_inner().unwrap();
        assert_eq!(
            manifest_list(&bytes).unwrap(),
            ["s3://t/metadata/m1.avro", "s3://t/metadata/m2.avro"]
        );
        let manifest = Schema::parse_str(
            r#"{"type": "record", "name": "manifest_entry", "fields": [
                {"name": "status", "type": "int"},
                {"name": "snapshot_id", "type": ["null", "long"]},
                {"name": "data_file", "type": {"type": "record", "name": "r2", "fields": [
                    {"name": "content", "type": "int"},
                    {"name": "file_path", "type": "string"},
                    {"name": "record_count", "type": "long"}]}}]}"#,
        )
        .unwrap();
        let mut writer = Writer::with_codec(&manifest, Vec::new(), Codec::Snappy).unwrap();
        for (status, path) in [(1, "s3://t/data/a.parquet"), (2, "s3://t/data/b.parquet")] {
            let mut file = Record::new(manifest_field(&manifest)).unwrap();
            file.put("content", 0);
            file.put("file_path", path);
            file.put("record_count", 3_i64);
            let mut record = Record::new(&manifest).unwrap();
            record.put("status", status);
            record.put("snapshot_id", Avro::Union(1, Box::new(Avro::Long(7))));
            record.put("data_file", file);
            writer.append(record).unwrap();
        }
        let bytes = writer.into_inner().unwrap();
        // The deleted entry's file too.
        assert_eq!(
            manifest_files(&bytes).unwrap(),
            ["s3://t/data/a.parquet", "s3://t/data/b.parquet"]
        );
        assert!(manifest_list(b"not avro").is_err());
        assert!(manifest_files(&manifest_list_bytes_without_path()).is_err());
    }

    /// The `data_file` record's schema of `manifest`.
    fn manifest_field(manifest: &Schema) -> &Schema {
        let Schema::Record(record) = manifest else {
            unreachable!()
        };
        &record.fields[2].schema
    }

    fn manifest_list_bytes_without_path() -> Vec<u8> {
        let schema = Schema::parse_str(
            r#"{"type": "record", "name": "e", "fields": [{"name": "status", "type": "int"}]}"#,
        )
        .unwrap();
        let mut writer = Writer::new(&schema, Vec::new()).unwrap();
        let mut record = Record::new(&schema).unwrap();
        record.put("status", 1);
        writer.append(record).unwrap();
        writer.into_inner().unwrap()
    }
}
