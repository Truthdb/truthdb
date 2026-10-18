//! The S3 API's access keys: an access key id and its secret. The secret is
//! stored as given, because request signing (SigV4) needs it back, not a
//! hash of it.
//!
//! So the secret is in plaintext wherever the database's pages and log go:
//! the data file, the WAL, every standby the WAL ships to, and full and log
//! backups (all copied verbatim). A restore to a point before
//! `s3 drop access key` brings the dropped key back as valid.

use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::{CREDENTIALS_TREE, ObjFile, ObjTxn};

fn invalid(message: String) -> StorageError {
    StorageError::InvalidConfig(message)
}

/// Longest access key id or secret accepted.
const MAX_LEN: usize = 128;

fn check(what: &str, value: &str) -> Result<(), StorageError> {
    if value.is_empty() || value.len() > MAX_LEN || !value.is_ascii() {
        return Err(invalid(format!(
            "an access key {what} is 1..={MAX_LEN} ASCII characters"
        )));
    }
    Ok(())
}

impl ObjTxn<'_, '_> {
    pub fn create_access_key(&mut self, id: &str, secret: &str) -> Result<(), StorageError> {
        check("id", id)?;
        check("secret", secret)?;
        let credentials = self.trees.credentials();
        let inserted = credentials.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            id.as_bytes(),
            secret.as_bytes(),
        )?;
        if inserted != TreeInsert::Inserted {
            return Err(invalid(format!("access key {id} already exists")));
        }
        Ok(())
    }

    pub fn drop_access_key(&mut self, id: &str) -> Result<(), StorageError> {
        let credentials = self.trees.credentials();
        credentials
            .delete(self.ctx, &mut OpMode::Txn(self.txn), id.as_bytes())?
            .ok_or_else(|| invalid(format!("no access key {id}")))?;
        Ok(())
    }
}

impl ObjFile<'_> {
    /// The secret of access key `id`, if it exists.
    pub fn access_key_secret(&mut self, id: &str) -> Result<Option<String>, StorageError> {
        let Some(credentials) = self.existing_tree(CREDENTIALS_TREE)? else {
            return Ok(None);
        };
        let mut ctx = self.core.page_ctx(0);
        let secret = credentials.get(&mut ctx, id.as_bytes())?;
        secret
            .map(|bytes| {
                String::from_utf8(bytes)
                    .map_err(|_| StorageError::InvalidFile(format!("corrupt access key {id}")))
            })
            .transpose()
    }

    /// Every access key id, in order.
    pub fn access_key_ids(&mut self) -> Result<Vec<String>, StorageError> {
        let Some(credentials) = self.existing_tree(CREDENTIALS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut ctx = self.core.page_ctx(0);
        credentials
            .scan(&mut ctx)?
            .into_iter()
            .map(|(key, _)| {
                String::from_utf8(key)
                    .map_err(|_| StorageError::InvalidFile("corrupt access key id".to_string()))
            })
            .collect()
    }
}
