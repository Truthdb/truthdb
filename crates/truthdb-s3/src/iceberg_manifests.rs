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
