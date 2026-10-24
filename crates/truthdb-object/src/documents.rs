//! Documents the engine keeps as given: a bucket's configurations (its
//! policy, and later its lifecycle, notification and other settings) and the
//! policies of an account's users. A document is stored in chunks small
//! enough for a tree cell, under a key prefix that ends in a 0 byte, each
//! chunk keyed by the prefix and its number.

use truthdb_core::relstore::btree::{BTree, KeyRow};
use truthdb_core::relstore::ctx::{OpMode, RelCtx};
use truthdb_core::storage::StorageError;

use crate::accounts::AccountError;
use crate::{BUCKET_CONFIG_TREE, ObjFile, ObjTxn, POLICIES_TREE};

/// Bytes per chunk.
const CHUNK: usize = 1500;

/// A bucket configuration: the bucket's policy.
pub const BUCKET_POLICY: u8 = 1;
/// A bucket configuration: its Block Public Access settings (one byte, see
/// `truthdb-s3`'s `public_access`). An account's own settings are kept as
/// this kind under the empty bucket name, with the account id as the id.
pub const PUBLIC_ACCESS_BLOCK: u8 = 2;
/// A bucket configuration: its default encryption settings (one byte, see
/// `truthdb-s3`'s `sse`).
pub const BUCKET_ENCRYPTION: u8 = 3;
/// A bucket configuration: its Object Lock default retention (see
/// `truthdb-s3`'s `object_lock`; whether Object Lock is enabled is the
/// bucket's own).
pub const OBJECT_LOCK_DEFAULT: u8 = 4;
/// A bucket configuration: its lifecycle rules (see `lifecycle`).
pub const BUCKET_LIFECYCLE: u8 = 5;
/// A bucket configuration: its event notifications (see `notifications`).
pub const BUCKET_NOTIFICATION: u8 = 6;
/// Bucket configuration: its CORS configuration (the rendered XML).
pub const BUCKET_CORS: u8 = 7;
/// Bucket configuration: its website configuration (the rendered XML).
pub const BUCKET_WEBSITE: u8 = 8;
/// Bucket configuration: its server access logging (the rendered XML).
pub const BUCKET_LOGGING: u8 = 9;
/// Bucket configurations: its request metrics configurations, by id (the
/// rendered XML).
pub const BUCKET_METRICS: u8 = 11;

fn chunk_key(prefix: &[u8], chunk: u16) -> Vec<u8> {
    [prefix, &chunk.to_be_bytes()].concat()
}

/// A bucket configuration's key prefix: bucket, 0, kind, id, 0.
fn config_prefix(bucket: &str, kind: u8, id: &str) -> Vec<u8> {
    [bucket.as_bytes(), &[0, kind], id.as_bytes(), &[0]].concat()
}

/// The ids among keys of a bucket's configurations of one kind (`bucket,
/// 0, kind, id, 0, chunk`), in order.
fn config_ids(keys: impl Iterator<Item = Vec<u8>>, bucket: &str) -> Vec<String> {
    let skip = bucket.len() + 2;
    let mut ids: Vec<String> = Vec::new();
    for key in keys {
        let Some(rest) = key.get(skip..) else {
            continue;
        };
        let id = rest.split(|byte| *byte == 0).next().unwrap_or_default();
        let id = String::from_utf8_lossy(id).into_owned();
        if ids.last() != Some(&id) {
            ids.push(id);
        }
    }
    ids
}

/// The range of a bucket's configuration keys of `kind`.
fn config_kind_range(bucket: &str, kind: u8) -> (Vec<u8>, Vec<u8>) {
    (
        [bucket.as_bytes(), &[0, kind]].concat(),
        [bucket.as_bytes(), &[0, kind + 1]].concat(),
    )
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

/// A managed policy's key prefix: account, 1, policy name, 0 (after every
/// user policy of the account, whose keys go on with a 0).
fn managed_prefix(account: &str, name: &str) -> Vec<u8> {
    [account.as_bytes(), &[1], name.as_bytes(), &[0]].concat()
}

/// A managed policy's attachment to a user: account, 2, user, 0, policy name.
fn attachment_key(account: &str, user: &str, name: &str) -> Vec<u8> {
    [
        account.as_bytes(),
        &[2],
        user.as_bytes(),
        &[0],
        name.as_bytes(),
    ]
    .concat()
}

/// IAM's policy name rule (it keeps the keys' 0 separators apart).
fn valid_policy_name(name: &str) -> bool {
    (1..=128).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+=,.@_-".contains(&b))
}

/// Whether the names of a key are well-formed: a name with any other byte
/// (a 0 above all) would scan or delete another policy's rows.
fn valid_names(user: Option<&str>, policy: &str) -> bool {
    user.is_none_or(crate::accounts::valid_user_name) && valid_policy_name(policy)
}

fn check_policy_name(name: &str) -> Result<(), StorageError> {
    if !valid_policy_name(name) {
        return Err(StorageError::InvalidConfig(format!(
            "a policy name is 1 to 128 letters, digits or +=,.@_- characters, not '{name}'"
        )));
    }
    Ok(())
}

/// A managed policy of an account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedPolicy {
    pub name: String,
    /// Its unique id (`ANPA…`), as IAM names it.
    pub policy_id: String,
    pub created_ms: u64,
    pub document: Vec<u8>,
}

/// A managed policy's stored form: creation time, id length, id, document.
fn encode_managed(policy_id: &str, created_ms: u64, document: &[u8]) -> Vec<u8> {
    [
        &created_ms.to_le_bytes()[..],
        &[policy_id.len() as u8],
        policy_id.as_bytes(),
        document,
    ]
    .concat()
}

fn decode_managed(name: String, stored: &[u8]) -> Result<ManagedPolicy, StorageError> {
    let corrupt = || StorageError::InvalidFile("corrupt managed policy".to_string());
    let id_len = *stored.get(8).ok_or_else(corrupt)? as usize;
    let id = stored.get(9..9 + id_len).ok_or_else(corrupt)?;
    Ok(ManagedPolicy {
        name,
        policy_id: String::from_utf8(id.to_vec()).map_err(|_| corrupt())?,
        created_ms: crate::codec::u64_le(&stored[..8])?,
        document: stored[9 + id_len..].to_vec(),
    })
}

/// The names of `account`'s managed policies (from their first chunks).
fn managed_names(
    ctx: &mut RelCtx<'_>,
    tree: &BTree,
    account: &str,
) -> Result<Vec<String>, StorageError> {
    let lower = [account.as_bytes(), &[1]].concat();
    let upper = [account.as_bytes(), &[1, 0xFF]].concat();
    Ok(tree
        .scan_range(ctx, Some(&lower), Some(&upper))?
        .into_iter()
        .filter(|(key, _)| key.ends_with(&[0, 0, 0]))
        .filter_map(|(key, _)| String::from_utf8(key[lower.len()..key.len() - 3].to_vec()).ok())
        .collect())
}

/// The chunks under `prefix`, in order.
pub(crate) fn chunks(
    ctx: &mut RelCtx<'_>,
    tree: &BTree,
    prefix: &[u8],
) -> Result<Vec<KeyRow>, StorageError> {
    tree.scan_range(
        ctx,
        Some(&chunk_key(prefix, 0)),
        Some(&chunk_key(prefix, u16::MAX)),
    )
}

pub(crate) fn read(
    ctx: &mut RelCtx<'_>,
    tree: &BTree,
    prefix: &[u8],
) -> Result<Option<Vec<u8>>, StorageError> {
    let chunks = chunks(ctx, tree, prefix)?;
    Ok((!chunks.is_empty()).then(|| chunks.into_iter().flat_map(|(_, chunk)| chunk).collect()))
}

impl ObjTxn<'_, '_> {
    /// Replaces the document under `prefix` with `document`.
    pub(crate) fn put_document(
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
    pub(crate) fn delete_document(
        &mut self,
        tree: &BTree,
        prefix: &[u8],
    ) -> Result<bool, StorageError> {
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

    /// The ids of the bucket's configurations of `kind`, in order.
    pub fn bucket_config_ids(
        &mut self,
        bucket: &str,
        kind: u8,
    ) -> Result<Vec<String>, StorageError> {
        let tree = self.trees.bucket_config();
        let (lower, upper) = config_kind_range(bucket, kind);
        let keys = tree
            .scan_range(self.ctx, Some(&lower), Some(&upper))?
            .into_iter()
            .map(|(key, _)| key);
        Ok(config_ids(keys, bucket))
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
        check_policy_name(name)?;
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
        if !valid_names(Some(user), name) {
            return Ok(false);
        }
        let tree = self.trees.policies();
        self.delete_document(&tree, &user_policy_prefix(account, user, name))
    }

    /// Whether user `user` has any policy of its own or attached.
    pub(crate) fn user_has_policies(
        &mut self,
        account: &str,
        user: &str,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.policies();
        let lower = user_policy_prefix(account, user, "");
        let lower = &lower[..lower.len() - 1];
        let upper = [lower, &[0xFF]].concat();
        let attached = attachment_key(account, user, "");
        let attached_upper = [&attached[..], &[0xFF]].concat();
        Ok(!tree
            .scan_range_limit(self.ctx, Some(lower), Some(&upper), 1)?
            .is_empty()
            || !tree
                .scan_range_limit(self.ctx, Some(&attached), Some(&attached_upper), 1)?
                .is_empty())
    }

    /// Whether `account` has any managed policy.
    pub(crate) fn account_has_managed_policies(
        &mut self,
        account: &str,
    ) -> Result<bool, StorageError> {
        let lower = [account.as_bytes(), &[1]].concat();
        let upper = [account.as_bytes(), &[1, 0xFF]].concat();
        Ok(!self
            .trees
            .policies()
            .scan_range_limit(self.ctx, Some(&lower), Some(&upper), 1)?
            .is_empty())
    }

    /// Creates managed policy `name` of `account`, with `policy_id` (a fresh
    /// id the caller drew).
    pub fn create_managed_policy(
        &mut self,
        account: &str,
        name: &str,
        policy_id: &str,
        document: &[u8],
        now_ms: u64,
    ) -> Result<Result<(), AccountError>, StorageError> {
        check_policy_name(name)?;
        if !self.account_exists(account)? {
            return Ok(Err(AccountError::NoSuchAccount));
        }
        let tree = self.trees.policies();
        // Names compare without case, as in IAM.
        if managed_names(self.ctx, &tree, account)?
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(name))
        {
            return Ok(Err(AccountError::PolicyExists));
        }
        let prefix = managed_prefix(account, name);
        self.put_document(tree, &prefix, &encode_managed(policy_id, now_ms, document))?;
        Ok(Ok(()))
    }

    /// Deletes managed policy `name`, refused while attached to a user.
    pub fn delete_managed_policy(
        &mut self,
        account: &str,
        name: &str,
    ) -> Result<Result<(), AccountError>, StorageError> {
        if !valid_names(None, name) {
            return Ok(Err(AccountError::NoSuchPolicy));
        }
        let tree = self.trees.policies();
        let prefix = managed_prefix(account, name);
        if chunks(self.ctx, &tree, &prefix)?.is_empty() {
            return Ok(Err(AccountError::NoSuchPolicy));
        }
        let lower = [account.as_bytes(), &[2]].concat();
        let upper = [account.as_bytes(), &[2, 0xFF]].concat();
        for (key, _) in tree.scan_range(self.ctx, Some(&lower), Some(&upper))? {
            if key.split(|b| *b == 0).nth(1) == Some(name.as_bytes()) {
                return Ok(Err(AccountError::NotEmpty("attachments")));
            }
        }
        self.delete_document(&tree, &prefix)?;
        Ok(Ok(()))
    }

    /// Attaches managed policy `name` to user `user` (both of `account`).
    pub fn attach_user_policy(
        &mut self,
        account: &str,
        user: &str,
        name: &str,
    ) -> Result<Result<(), AccountError>, StorageError> {
        if !self.user_exists(account, user)? {
            return Ok(Err(AccountError::NoSuchUser));
        }
        if !valid_names(Some(user), name) {
            return Ok(Err(AccountError::NoSuchPolicy));
        }
        let tree = self.trees.policies();
        if chunks(self.ctx, &tree, &managed_prefix(account, name))?.is_empty() {
            return Ok(Err(AccountError::NoSuchPolicy));
        }
        let key = attachment_key(account, user, name);
        if tree.get(self.ctx, &key)?.is_none() {
            tree.insert_unique(self.ctx, &mut OpMode::Txn(self.txn), &key, &[])?;
        }
        Ok(Ok(()))
    }

    /// Detaches managed policy `name` from user `user`; whether it was.
    pub fn detach_user_policy(
        &mut self,
        account: &str,
        user: &str,
        name: &str,
    ) -> Result<bool, StorageError> {
        if !valid_names(Some(user), name) {
            return Ok(false);
        }
        Ok(self
            .trees
            .policies()
            .delete(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &attachment_key(account, user, name),
            )?
            .is_some())
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

    /// The ids of the bucket's configurations of `kind`, in order.
    pub fn bucket_config_ids(
        &mut self,
        bucket: &str,
        kind: u8,
    ) -> Result<Vec<String>, StorageError> {
        let Some(tree) = self.existing_tree(BUCKET_CONFIG_TREE)? else {
            return Ok(Vec::new());
        };
        let (lower, upper) = config_kind_range(bucket, kind);
        let keys = tree
            .scan_range(&mut self.core.page_ctx(0), Some(&lower), Some(&upper))?
            .into_iter()
            .map(|(key, _)| key);
        Ok(config_ids(keys, bucket))
    }

    /// Managed policy `name` of `account`, if it exists.
    pub fn managed_policy(
        &mut self,
        account: &str,
        name: &str,
    ) -> Result<Option<ManagedPolicy>, StorageError> {
        let Some(tree) = self.existing_tree(POLICIES_TREE)? else {
            return Ok(None);
        };
        if !valid_names(None, name) {
            return Ok(None);
        }
        read(
            &mut self.core.page_ctx(0),
            &tree,
            &managed_prefix(account, name),
        )?
        .map(|stored| decode_managed(name.to_string(), &stored))
        .transpose()
    }

    /// The managed policies of `account`, by name, with how many users each
    /// is attached to.
    pub fn managed_policies(
        &mut self,
        account: &str,
    ) -> Result<Vec<(ManagedPolicy, u32)>, StorageError> {
        let Some(tree) = self.existing_tree(POLICIES_TREE)? else {
            return Ok(Vec::new());
        };
        let mut ctx = self.core.page_ctx(0);
        let corrupt = || StorageError::InvalidFile("corrupt managed policy key".to_string());
        let lower = [account.as_bytes(), &[1]].concat();
        let upper = [account.as_bytes(), &[1, 0xFF]].concat();
        let mut stored: Vec<(String, Vec<u8>)> = Vec::new();
        for (key, chunk) in tree.scan_range(&mut ctx, Some(&lower), Some(&upper))? {
            // The key: the account, 1, the name, 0, the chunk number.
            let name = key
                .get(lower.len()..key.len().saturating_sub(3))
                .and_then(|name| String::from_utf8(name.to_vec()).ok())
                .ok_or_else(corrupt)?;
            match stored.last_mut() {
                Some((last, document)) if *last == name => document.extend_from_slice(&chunk),
                _ => stored.push((name, chunk)),
            }
        }
        let lower = [account.as_bytes(), &[2]].concat();
        let upper = [account.as_bytes(), &[2, 0xFF]].concat();
        let attachments = tree.scan_range(&mut ctx, Some(&lower), Some(&upper))?;
        stored
            .into_iter()
            .map(|(name, document)| {
                let count = attachments
                    .iter()
                    .filter(|(key, _)| key.split(|b| *b == 0).nth(1) == Some(name.as_bytes()))
                    .count() as u32;
                Ok((decode_managed(name, &document)?, count))
            })
            .collect()
    }

    /// The managed policies attached to user `user`, by name.
    pub fn attached_policies(
        &mut self,
        account: &str,
        user: &str,
    ) -> Result<Vec<String>, StorageError> {
        let Some(tree) = self.existing_tree(POLICIES_TREE)? else {
            return Ok(Vec::new());
        };
        if !crate::accounts::valid_user_name(user) {
            return Ok(Vec::new());
        }
        let lower = attachment_key(account, user, "");
        let upper = [&lower[..], &[0xFF]].concat();
        tree.scan_range(&mut self.core.page_ctx(0), Some(&lower), Some(&upper))?
            .into_iter()
            .map(|(key, _)| {
                String::from_utf8(key[lower.len()..].to_vec())
                    .map_err(|_| StorageError::InvalidFile("corrupt policy attachment".to_string()))
            })
            .collect()
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
        if !crate::accounts::valid_user_name(user) {
            return Ok(Vec::new());
        }
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
