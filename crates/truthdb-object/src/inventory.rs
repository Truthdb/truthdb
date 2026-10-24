//! A bucket's S3 Inventory configurations (kept as bucket configurations
//! of kind `BUCKET_INVENTORY`, by id): what an inventory report lists and
//! where it goes. Kept as JSON here, where the object jobs read them; the
//! S3 layer takes and renders them as XML.

use serde_json::{Value, json};

/// The format an inventory list is written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryFormat {
    Csv,
    Parquet,
}

/// How often an inventory list is made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventoryFrequency {
    Daily,
    Weekly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryConfig {
    pub id: String,
    pub enabled: bool,
    /// Only the objects whose key starts with this.
    pub prefix: Option<String>,
    /// The destination bucket (its name), the account named for it, and
    /// the prefix of the list's keys there.
    pub destination: String,
    pub destination_account: Option<String>,
    pub destination_prefix: Option<String>,
    pub format: InventoryFormat,
    /// Whether the lists are to be encrypted with S3's keys (`SSE-S3`).
    pub sse_s3: bool,
    /// Every version (`All`), or only the current ones.
    pub all_versions: bool,
    pub optional_fields: Vec<String>,
    pub frequency: InventoryFrequency,
}

impl InventoryConfig {
    pub fn encode(&self) -> Vec<u8> {
        json!({
            "id": self.id,
            "enabled": self.enabled,
            "prefix": self.prefix,
            "destination": self.destination,
            "destination_account": self.destination_account,
            "destination_prefix": self.destination_prefix,
            "format": match self.format {
                InventoryFormat::Csv => "CSV",
                InventoryFormat::Parquet => "Parquet",
            },
            "sse_s3": self.sse_s3,
            "all_versions": self.all_versions,
            "optional_fields": self.optional_fields,
            "frequency": match self.frequency {
                InventoryFrequency::Daily => "Daily",
                InventoryFrequency::Weekly => "Weekly",
            },
        })
        .to_string()
        .into_bytes()
    }

    pub fn decode(document: &[u8]) -> Option<InventoryConfig> {
        let value: Value = serde_json::from_slice(document).ok()?;
        let text = |name: &str| value[name].as_str().map(str::to_string);
        Some(InventoryConfig {
            id: text("id")?,
            enabled: value["enabled"].as_bool()?,
            prefix: text("prefix"),
            destination: text("destination")?,
            destination_account: text("destination_account"),
            destination_prefix: text("destination_prefix"),
            format: match value["format"].as_str()? {
                "CSV" => InventoryFormat::Csv,
                "Parquet" => InventoryFormat::Parquet,
                _ => return None,
            },
            sse_s3: value["sse_s3"].as_bool().unwrap_or(false),
            all_versions: value["all_versions"].as_bool()?,
            optional_fields: value["optional_fields"]
                .as_array()?
                .iter()
                .filter_map(|field| field.as_str().map(str::to_string))
                .collect(),
            frequency: match value["frequency"].as_str()? {
                "Daily" => InventoryFrequency::Daily,
                "Weekly" => InventoryFrequency::Weekly,
                _ => return None,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configuration_reads_back_as_kept() {
        let config = InventoryConfig {
            id: "report1".into(),
            enabled: true,
            prefix: Some("docs/".into()),
            destination: "inventories".into(),
            destination_account: Some("111122223333".into()),
            destination_prefix: None,
            format: InventoryFormat::Parquet,
            sse_s3: true,
            all_versions: false,
            optional_fields: vec!["Size".into(), "ETag".into()],
            frequency: InventoryFrequency::Weekly,
        };
        assert_eq!(InventoryConfig::decode(&config.encode()), Some(config));
        assert_eq!(InventoryConfig::decode(b"{}"), None);
    }
}
