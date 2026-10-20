//! Documents the engine keeps as given: a bucket's configurations (its
//! policy, and later its lifecycle, notification and other settings) and the
//! policies of an account's users. A document is stored in chunks small
//! enough for a tree cell, under a key prefix that ends in a 0 byte, each
//! chunk keyed by the prefix and its number.

use truthdb_core::relstore::btree::{BTree, KeyRow};
use truthdb_core::relstore::ctx::{OpMode, RelCtx};
use truthdb_core::storage::StorageError;

use crate::{BUCKET_CONFIG_TREE, ObjFile, ObjTxn, POLICIES_TREE};

/// Bytes per chunk.
const CHUNK: usize = 1500;

/// A bucket configuration: the bucket's policy.
pub const BUCKET_POLICY: u8 = 1;

fn chunk_key(prefix: &[u8], chunk: u16) -> Vec<u8> {
    [prefix, &chunk.to_be_bytes()].concat()
}

/// A bucket configuration's key prefix: bucket, 0, kind, id, 0.
fn config_prefix(bucket: &str, kind: u8, id: &str) -> Vec<u8> {
    [bucket.as_bytes(), &[0, kind], id.as_bytes(), &[0]].concat()
}

/// A user policy's key prefix: account, 0, user, 0, policy name, 0.
fn user_policy_prefix(account: &str, user: &str, name: &str) -> Vec<u8> {
    [
        account.as_bytes(),
        &[0],
        user.as_bytes(),
        &[0],
        name.as_bytes(),
        &[0],
    ]
    .concat()
}

/// The chunks under `prefix`, in order.
fn chunks(ctx: &mut RelCtx<'_>, tree: &BTree, prefix: &[u8]) -> Result<Vec<KeyRow>, StorageError> {
    tree.scan_range(
        ctx,
        Some(&chunk_key(prefix, 0)),
        Some(&chunk_key(prefix, u16::MAX)),
    )
}

fn read(
    ctx: &mut RelCtx<'_>,
    tree: &BTree,
    prefix: &[u8],
) -> Result<Option<Vec<u8>>, StorageError> {
    let chunks = chunks(ctx, tree, prefix)?;
    Ok((!chunks.is_empty()).then(|| chunks.into_iter().flat_map(|(_, chunk)| chunk).collect()))
}

impl ObjTxn<'_, '_> {
    /// Replaces the document under `prefix` with `document`.
    fn put_document(
        &mut self,
        tree: BTree,
        prefix: &[u8],
        document: &[u8],
    ) -> Result<(), StorageError> {
        self.delete_document(&tree, prefix)?;
        for (index, chunk) in document.chunks(CHUNK).enumerate() {
            tree.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &chunk_key(prefix, index as u16),
                chunk,
            )?;
        }
        Ok(())
    }

    /// Deletes the document under `prefix`; whether there was one.
    fn delete_document(&mut self, tree: &BTree, prefix: &[u8]) -> Result<bool, StorageError> {
        let chunks = chunks(self.ctx, tree, prefix)?;
        for (key, _) in &chunks {
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
        }
        Ok(!chunks.is_empty())
    }

    /// Sets bucket configuration `kind` (`id` names one of several of a
    /// kind; empty otherwise). The caller checks the bucket exists.
    pub fn put_bucket_config(
        &mut self,
        bucket: &str,
        kind: u8,
        id: &str,
        document: &[u8],
    ) -> Result<(), StorageError> {
        let tree = self.trees.bucket_config();
        self.put_document(tree, &config_prefix(bucket, kind, id), document)
    }

    /// Deletes bucket configuration `kind`; whether there was one.
    pub fn delete_bucket_config(
        &mut self,
        bucket: &str,
        kind: u8,
        id: &str,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.bucket_config();
        self.delete_document(&tree, &config_prefix(bucket, kind, id))
    }

    /// Deletes every configuration of a bucket being deleted.
    pub(crate) fn delete_bucket_configs(&mut self, bucket: &str) -> Result<(), StorageError> {
        let tree = self.trees.bucket_config();
        // Bucket names hold no byte below `-`: every key of the bucket's
        // configurations lies between these.
        let lower = [bucket.as_bytes(), &[0]].concat();
        let upper = [bucket.as_bytes(), &[1]].concat();
        for (key, _) in tree.scan_range(self.ctx, Some(&lower), Some(&upper))? {
            tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        Ok(())
    }

    /// Sets user `user`'s policy `name` (the caller checks the user exists).
    pub fn put_user_policy(
        &mut self,
        account: &str,
        user: &str,
        name: &str,
        document: &[u8],
    ) -> Result<(), StorageError> {
        // IAM's policy name rule (it keeps the key's 0 separators apart).
        if !(1..=128).contains(&name.len())
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"+=,.@_-".contains(&b))
        {
            return Err(StorageError::InvalidConfig(format!(
                "a policy name is 1 to 128 letters, digits or +=,.@_- characters, not '{name}'"
            )));
        }
        let tree = self.trees.policies();
        self.put_document(tree, &user_policy_prefix(account, user, name), document)
    }

    /// Deletes user `user`'s policy `name`; whether there was one.
    pub fn delete_user_policy(
        &mut self,
        account: &str,
        user: &str,
        name: &str,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.policies();
        self.delete_document(&tree, &user_policy_prefix(account, user, name))
    }

    /// Whether user `user` has any policy.
    pub(crate) fn user_has_policies(
        &mut self,
        account: &str,
        user: &str,
    ) -> Result<bool, StorageError> {
        let lower = user_policy_prefix(account, user, "");
        let lower = &lower[..lower.len() - 1];
        let upper = [lower, &[0xFF]].concat();
        Ok(!self
            .trees
            .policies()
            .scan_range_limit(self.ctx, Some(lower), Some(&upper), 1)?
            .is_empty())
    }
}

impl ObjFile<'_> {
    /// Bucket configuration `kind`, if set.
    pub fn bucket_config(
        &mut self,
        bucket: &str,
        kind: u8,
        id: &str,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(tree) = self.existing_tree(BUCKET_CONFIG_TREE)? else {
            return Ok(None);
        };
        read(
            &mut self.core.page_ctx(0),
            &tree,
            &config_prefix(bucket, kind, id),
        )
    }

    /// User `user`'s policies: `(name, document)`, by name.
    pub fn user_policies(
        &mut self,
        account: &str,
        user: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let Some(tree) = self.existing_tree(POLICIES_TREE)? else {
            return Ok(Vec::new());
        };
        let start = user_policy_prefix(account, user, "");
        let start = &start[..start.len() - 1];
        let end = [start, &[0xFF]].concat();
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        for (key, chunk) in tree.scan_range(&mut self.core.page_ctx(0), Some(start), Some(&end))? {
            // The key: the start, the name, 0, the chunk number.
            let name = key
                .get(start.len()..key.len().saturating_sub(3))
                .and_then(|name| String::from_utf8(name.to_vec()).ok())
                .ok_or_else(|| StorageError::InvalidFile("corrupt user policy key".to_string()))?;
            match out.last_mut() {
                Some((last, document)) if *last == name => document.extend_from_slice(&chunk),
                _ => out.push((name, chunk)),
            }
        }
        Ok(out)
    }
}
