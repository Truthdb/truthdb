//! Imports from another S3 service (`s3 create import`): where each import
//! copies from (its endpoint, region, keys, bucket and prefix) and to, and
//! how far it has come. An import walks the source's listing in key order;
//! its place is the last source key done, recorded in the same transaction
//! as the object that key became (or as the record of its failure), so a
//! restart goes on after it, neither copying an object twice nor missing
//! one. The secret lives in the database, which whole-database encryption
//! covers.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::{Reader, Writer};
use crate::{IMPORT_FAILURES_TREE, IMPORTS_TREE, NEXT_IMPORT_KEY, ObjFile, ObjTxn, next_counter};

/// Where an import copies from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportSource {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret: String,
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportState {
    Running,
    /// Every object of the source was looked at.
    Done,
    /// The source refused the import itself (its keys, its bucket).
    Failed,
}

impl ImportState {
    pub fn as_str(self) -> &'static str {
        match self {
            ImportState::Running => "RUNNING",
            ImportState::Done => "DONE",
            ImportState::Failed => "FAILED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Import {
    pub id: u64,
    pub source: ImportSource,
    /// The bucket and key prefix it copies into.
    pub bucket: String,
    pub prefix: String,
    pub state: ImportState,
    /// The last source key done (empty: none yet).
    pub after: String,
    pub copied: u64,
    /// Objects already there with the same size and ETag.
    pub skipped: u64,
    pub failed: u64,
    pub bytes: u64,
    pub created_ms: u64,
    pub updated_ms: u64,
    /// The last error met (the import may have gone on since).
    pub error: String,
}

/// An object an import could not copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportFailure {
    pub key: String,
    pub error: String,
}

const FORMAT: u8 = 1;

impl Import {
    fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(FORMAT);
        for text in [
            &self.source.endpoint,
            &self.source.region,
            &self.source.access_key,
            &self.source.secret,
            &self.source.bucket,
            &self.source.prefix,
            &self.bucket,
            &self.prefix,
        ] {
            out.str(text);
        }
        out.u8(match self.state {
            ImportState::Running => 0,
            ImportState::Done => 1,
            ImportState::Failed => 2,
        });
        out.str(&self.after);
        for number in [
            self.copied,
            self.skipped,
            self.failed,
            self.bytes,
            self.created_ms,
            self.updated_ms,
        ] {
            out.u64(number);
        }
        out.str(&self.error);
        out.0
    }

    fn decode(id: u64, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "import");
        if input.u8()? != FORMAT {
            return Err(input.corrupt());
        }
        let source = ImportSource {
            endpoint: input.str()?,
            region: input.str()?,
            access_key: input.str()?,
            secret: input.str()?,
            bucket: input.str()?,
            prefix: input.str()?,
        };
        let (bucket, prefix) = (input.str()?, input.str()?);
        let state = match input.u8()? {
            0 => ImportState::Running,
            1 => ImportState::Done,
            2 => ImportState::Failed,
            _ => return Err(input.corrupt()),
        };
        let import = Import {
            id,
            source,
            bucket,
            prefix,
            state,
            after: input.str()?,
            copied: input.u64()?,
            skipped: input.u64()?,
            failed: input.u64()?,
            bytes: input.u64()?,
            created_ms: input.u64()?,
            updated_ms: input.u64()?,
            error: input.str()?,
        };
        if !input.rest().is_empty() {
            return Err(input.corrupt());
        }
        Ok(import)
    }
}

/// `(import, 0, source key)`.
fn failure_key(id: u64, key: &str) -> Vec<u8> {
    [&id.to_be_bytes()[..], &[0], key.as_bytes()].concat()
}

impl ObjTxn<'_, '_> {
    /// Records a new import, running; returns its id.
    pub fn create_import(
        &mut self,
        source: ImportSource,
        bucket: &str,
        prefix: &str,
        now_ms: u64,
    ) -> Result<u64, StorageError> {
        let id = next_counter(self, NEXT_IMPORT_KEY)?;
        let import = Import {
            id,
            source,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            state: ImportState::Running,
            after: String::new(),
            copied: 0,
            skipped: 0,
            failed: 0,
            bytes: 0,
            created_ms: now_ms,
            updated_ms: now_ms,
            error: String::new(),
        };
        self.trees.imports().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &id.to_be_bytes(),
            &import.encode(),
        )?;
        Ok(id)
    }

    /// Import `id`, if there is one.
    pub fn import(&mut self, id: u64) -> Result<Option<Import>, StorageError> {
        self.trees
            .imports()
            .get(self.ctx, &id.to_be_bytes())?
            .map(|row| Import::decode(id, &row))
            .transpose()
    }

    /// Records where an import stands (in the transaction of what it did).
    pub fn update_import(&mut self, import: &Import) -> Result<(), StorageError> {
        self.trees.imports().update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &import.id.to_be_bytes(),
            &import.encode(),
        )?;
        Ok(())
    }

    /// Records an object an import could not copy.
    pub fn record_import_failure(
        &mut self,
        id: u64,
        key: &str,
        error: &str,
    ) -> Result<(), StorageError> {
        let tree = self.trees.import_failures();
        let mode = &mut OpMode::Txn(self.txn);
        let row_key = failure_key(id, key);
        tree.delete(self.ctx, mode, &row_key)?;
        tree.insert_unique(self.ctx, mode, &row_key, error.as_bytes())?;
        Ok(())
    }

    /// Forgets an import and its failures (a running one stops); whether
    /// there was one.
    pub fn drop_import(&mut self, id: u64) -> Result<bool, StorageError> {
        let mode = &mut OpMode::Txn(self.txn);
        let failures = self.trees.import_failures();
        for (key, _) in failures.scan_range(
            self.ctx,
            Some(&failure_key(id, "")),
            Some(&[&id.to_be_bytes()[..], &[1]].concat()),
        )? {
            failures.delete(self.ctx, mode, &key)?;
        }
        Ok(self
            .trees
            .imports()
            .delete(self.ctx, mode, &id.to_be_bytes())?
            .is_some())
    }
}

impl ObjFile<'_> {
    /// Every import, oldest first.
    pub fn imports(&mut self) -> Result<Vec<Import>, StorageError> {
        let Some(tree) = self.existing_tree(IMPORTS_TREE)? else {
            return Ok(Vec::new());
        };
        tree.scan(&mut self.core.page_ctx(0))?
            .into_iter()
            .map(|(key, row)| {
                let id = u64::from_be_bytes(
                    key.as_slice()
                        .try_into()
                        .map_err(|_| StorageError::InvalidFile("corrupt import key".into()))?,
                );
                Import::decode(id, &row)
            })
            .collect()
    }

    /// Import `id`, if there is one.
    pub fn import(&mut self, id: u64) -> Result<Option<Import>, StorageError> {
        let Some(tree) = self.existing_tree(IMPORTS_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), &id.to_be_bytes())?
            .map(|row| Import::decode(id, &row))
            .transpose()
    }

    /// The objects import `id` could not copy, in key order.
    pub fn import_failures(&mut self, id: u64) -> Result<Vec<ImportFailure>, StorageError> {
        let Some(tree) = self.existing_tree(IMPORT_FAILURES_TREE)? else {
            return Ok(Vec::new());
        };
        tree.scan_range(
            &mut self.core.page_ctx(0),
            Some(&failure_key(id, "")),
            Some(&[&id.to_be_bytes()[..], &[1]].concat()),
        )?
        .into_iter()
        .map(|(key, error)| {
            let corrupt = || StorageError::InvalidFile("corrupt import failure".into());
            Ok(ImportFailure {
                key: String::from_utf8(key.get(9..).ok_or_else(corrupt)?.to_vec())
                    .map_err(|_| corrupt())?,
                error: String::from_utf8(error).map_err(|_| corrupt())?,
            })
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_import_round_trips_and_corruption_is_refused() {
        let import = Import {
            id: 7,
            source: ImportSource {
                endpoint: "https://s3.example".into(),
                region: "eu-west-1".into(),
                access_key: "AKID".into(),
                secret: "secret".into(),
                bucket: "src".into(),
                prefix: "in/".into(),
            },
            bucket: "dst".into(),
            prefix: "out/".into(),
            state: ImportState::Failed,
            after: "in/k".into(),
            copied: 1,
            skipped: 2,
            failed: 3,
            bytes: 4,
            created_ms: 5,
            updated_ms: 6,
            error: "403".into(),
        };
        let bytes = import.encode();
        assert_eq!(Import::decode(7, &bytes).unwrap(), import);
        for cut in 0..bytes.len() {
            assert!(Import::decode(7, &bytes[..cut]).is_err(), "cut at {cut}");
        }
        assert!(Import::decode(7, &[bytes.clone(), vec![0]].concat()).is_err());
    }
}
