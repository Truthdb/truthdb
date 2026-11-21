//! A bucket's S3 Inventory configurations (kept as bucket configurations
//! of kind `BUCKET_INVENTORY`, by id): what an inventory report lists and
//! where it goes. Kept as JSON here, where the object jobs read them; the
//! S3 layer takes and renders them as XML. Each configuration's report
//! runs are tracked by when the last one started.

use serde_json::{Value, json};
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::corrupt;
use crate::{INVENTORY_STATE_TREE, ObjFile, ObjTxn};

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

/// A configuration's run state key: its bucket, 0, its id (bucket names
/// hold no byte below `-`, so a bucket's keys lie between `bucket\0` and
/// `bucket\1`).
fn state_key(bucket: &str, id: &str) -> Vec<u8> {
    [bucket.as_bytes(), &[0], id.as_bytes()].concat()
}

impl ObjTxn<'_, '_> {
    /// Tracks configuration `id`'s runs from now on, none run yet; one
    /// already tracked (a configuration replaced) keeps its last run.
    pub fn track_inventory(&mut self, bucket: &str, id: &str) -> Result<(), StorageError> {
        let tree = self.trees.inventory_state();
        let key = state_key(bucket, id);
        if tree.get(self.ctx, &key)?.is_none() {
            tree.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &key,
                &0u64.to_be_bytes(),
            )?;
        }
        Ok(())
    }

    /// Stops tracking configuration `id` (deleted).
    pub fn forget_inventory(&mut self, bucket: &str, id: &str) -> Result<(), StorageError> {
        self.trees.inventory_state().delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &state_key(bucket, id),
        )?;
        Ok(())
    }

    /// Stops tracking every configuration of a bucket being deleted.
    pub(crate) fn forget_inventories(&mut self, bucket: &str) -> Result<(), StorageError> {
        let tree = self.trees.inventory_state();
        let lower = [bucket.as_bytes(), &[0]].concat();
        let upper = [bucket.as_bytes(), &[1]].concat();
        for (key, _) in tree.scan_range(self.ctx, Some(&lower), Some(&upper))? {
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        Ok(())
    }

    /// Records that a run of configuration `id` started at `started_ms`
    /// ended; false (nothing recorded) when it is no longer tracked.
    pub fn inventory_ran(
        &mut self,
        bucket: &str,
        id: &str,
        started_ms: u64,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.inventory_state();
        let key = state_key(bucket, id);
        if tree.get(self.ctx, &key)?.is_none() {
            return Ok(false);
        }
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, &key)?;
        tree.insert_unique(self.ctx, mode, &key, &started_ms.to_be_bytes())?;
        Ok(true)
    }
}

impl ObjFile<'_> {
    /// Every tracked configuration: its bucket, its id, and when its last
    /// run started (0: none has).
    pub fn inventory_runs(&mut self) -> Result<Vec<(String, String, u64)>, StorageError> {
        let Some(states) = self.existing_tree(INVENTORY_STATE_TREE)? else {
            return Ok(Vec::new());
        };
        let mut runs = Vec::new();
        for (key, row) in states.scan(&mut self.core.page_ctx(0))? {
            let split = key
                .iter()
                .position(|&byte| byte == 0)
                .ok_or_else(|| corrupt("inventory state"))?;
            let text = |bytes: &[u8]| {
                String::from_utf8(bytes.to_vec()).map_err(|_| corrupt("inventory state"))
            };
            let last: [u8; 8] = row
                .as_slice()
                .try_into()
                .map_err(|_| corrupt("inventory state"))?;
            runs.push((
                text(&key[..split])?,
                text(&key[split + 1..])?,
                u64::from_be_bytes(last),
            ));
        }
        Ok(runs)
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
