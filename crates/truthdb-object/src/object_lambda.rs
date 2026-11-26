//! S3 Object Lambda access points: an account's named endpoints (unique
//! per account in a region; this server is one region) that transform what
//! a supporting access point serves. Each is kept, in the access points'
//! tree, under its account and name, with its configuration (the XML its
//! S3 Control requests give, as the S3 layer renders it: the supporting
//! access point, the transformations and the features), its alias
//! (generated, unchangeable; indexed, as data requests may name it in
//! place of a bucket), its creation time, and its policy, if put.

use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::{Reader, Writer};
use crate::{ACCESS_POINTS_TREE, ObjFile, ObjTxn};

const FORMAT: u8 = 1;
/// The kinds of row: an Object Lambda access point, its alias, its policy
/// (an access point's are P, A and Q).
const POINT: u8 = b'L';
const ALIAS: u8 = b'M';
const POLICY: u8 = b'N';

/// An Object Lambda access point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLambdaPoint {
    pub account: String,
    pub name: String,
    /// Its configuration, as the S3 layer renders it.
    pub configuration: String,
    pub alias: String,
    pub created_ms: u64,
}

fn point_key(account: &str, name: &str) -> Vec<u8> {
    let mut key = vec![POINT];
    key.extend_from_slice(account.as_bytes());
    key.push(0);
    key.extend_from_slice(name.as_bytes());
    key
}

fn policy_key(account: &str, name: &str) -> Vec<u8> {
    let mut key = point_key(account, name);
    key[0] = POLICY;
    key
}

fn alias_key(alias: &str) -> Vec<u8> {
    let mut key = vec![ALIAS];
    key.extend_from_slice(alias.as_bytes());
    key
}

impl ObjectLambdaPoint {
    fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(FORMAT);
        out.str(&self.configuration);
        out.str(&self.alias);
        out.u64(self.created_ms);
        out.0
    }

    fn decode(account: &str, name: &str, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "Object Lambda access point");
        if input.u8()? != FORMAT {
            return Err(input.corrupt());
        }
        let configuration = input.str()?;
        let alias = input.str()?;
        let created_ms = input.u64()?;
        if !input.rest().is_empty() {
            return Err(input.corrupt());
        }
        Ok(ObjectLambdaPoint {
            account: account.to_string(),
            name: name.to_string(),
            configuration,
            alias,
            created_ms,
        })
    }
}

/// The account and name a point key holds.
fn split_key(key: &[u8]) -> Result<(String, String), StorageError> {
    let corrupt =
        || StorageError::InvalidFile("corrupt Object Lambda access point key".to_string());
    let rest = key.get(1..).ok_or_else(corrupt)?;
    let at = rest
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(corrupt)?;
    let account = String::from_utf8(rest[..at].to_vec()).map_err(|_| corrupt())?;
    let name = String::from_utf8(rest[at + 1..].to_vec()).map_err(|_| corrupt())?;
    Ok((account, name))
}

impl ObjTxn<'_, '_> {
    /// Records Object Lambda access point `point` (and its alias); false if
    /// its account has one of its name, or its alias is taken.
    pub fn create_object_lambda_point(
        &mut self,
        point: &ObjectLambdaPoint,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.tree(ACCESS_POINTS_TREE);
        if tree.get(self.ctx, &alias_key(&point.alias))?.is_some() {
            return Ok(false);
        }
        let inserted = tree.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &point_key(&point.account, &point.name),
            &point.encode(),
        )?;
        if inserted != TreeInsert::Inserted {
            return Ok(false);
        }
        let mut target = point.account.as_bytes().to_vec();
        target.push(0);
        target.extend_from_slice(point.name.as_bytes());
        tree.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &alias_key(&point.alias),
            &target,
        )?;
        Ok(true)
    }

    /// Replaces the configuration of `account`'s Object Lambda access point
    /// `name`; whether it is there.
    pub fn set_object_lambda_configuration(
        &mut self,
        account: &str,
        name: &str,
        configuration: &str,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.tree(ACCESS_POINTS_TREE);
        let key = point_key(account, name);
        let Some(row) = tree.get(self.ctx, &key)? else {
            return Ok(false);
        };
        let mut point = ObjectLambdaPoint::decode(account, name, &row)?;
        point.configuration = configuration.to_string();
        tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        tree.insert_unique(self.ctx, &mut OpMode::Txn(self.txn), &key, &point.encode())?;
        Ok(true)
    }

    /// Removes `account`'s Object Lambda access point `name`, its alias and
    /// its policy; whether it was there.
    pub fn delete_object_lambda_point(
        &mut self,
        account: &str,
        name: &str,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.tree(ACCESS_POINTS_TREE);
        let Some(row) = tree.delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &point_key(account, name),
        )?
        else {
            return Ok(false);
        };
        let point = ObjectLambdaPoint::decode(account, name, &row)?;
        tree.delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &alias_key(&point.alias),
        )?;
        tree.delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &policy_key(account, name),
        )?;
        Ok(true)
    }

    /// Puts policy `policy` (none: deletes it) of `account`'s Object Lambda
    /// access point `name`; whether it is there.
    pub fn set_object_lambda_policy(
        &mut self,
        account: &str,
        name: &str,
        policy: Option<&str>,
    ) -> Result<bool, StorageError> {
        let tree = self.trees.tree(ACCESS_POINTS_TREE);
        if tree.get(self.ctx, &point_key(account, name))?.is_none() {
            return Ok(false);
        }
        let key = policy_key(account, name);
        tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        if let Some(policy) = policy {
            tree.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &key,
                policy.as_bytes(),
            )?;
        }
        Ok(true)
    }
}

impl ObjFile<'_> {
    /// `account`'s Object Lambda access point `name`, if there is one.
    pub fn object_lambda_point(
        &mut self,
        account: &str,
        name: &str,
    ) -> Result<Option<ObjectLambdaPoint>, StorageError> {
        let Some(tree) = self.existing_tree(ACCESS_POINTS_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), &point_key(account, name))?
            .map(|row| ObjectLambdaPoint::decode(account, name, &row))
            .transpose()
    }

    /// The Object Lambda access point of alias `alias`, if there is one.
    pub fn object_lambda_point_by_alias(
        &mut self,
        alias: &str,
    ) -> Result<Option<ObjectLambdaPoint>, StorageError> {
        let Some(tree) = self.existing_tree(ACCESS_POINTS_TREE)? else {
            return Ok(None);
        };
        let Some(target) = tree.get(&mut self.core.page_ctx(0), &alias_key(alias))? else {
            return Ok(None);
        };
        let mut key = vec![POINT];
        key.extend_from_slice(&target);
        let (account, name) = split_key(&key)?;
        self.object_lambda_point(&account, &name)
    }

    /// The policy of `account`'s Object Lambda access point `name`, if one
    /// is put.
    pub fn object_lambda_policy(
        &mut self,
        account: &str,
        name: &str,
    ) -> Result<Option<String>, StorageError> {
        let Some(tree) = self.existing_tree(ACCESS_POINTS_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), &policy_key(account, name))?
            .map(|row| {
                String::from_utf8(row).map_err(|_| {
                    StorageError::InvalidFile(
                        "corrupt Object Lambda access point policy".to_string(),
                    )
                })
            })
            .transpose()
    }

    /// `account`'s Object Lambda access points after name `after` (none:
    /// from the first), by name, at most `limit`.
    pub fn object_lambda_points(
        &mut self,
        account: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectLambdaPoint>, StorageError> {
        let Some(tree) = self.existing_tree(ACCESS_POINTS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut lower = point_key(account, after.unwrap_or(""));
        if after.is_some() {
            // Past `after` itself.
            lower.push(0);
        }
        let mut upper = vec![POINT];
        upper.extend_from_slice(account.as_bytes());
        upper.push(1);
        tree.scan_range_limit(
            &mut self.core.page_ctx(0),
            Some(&lower),
            Some(&upper),
            limit,
        )?
        .into_iter()
        .filter(|(key, _)| key.as_slice() < upper.as_slice())
        .map(|(key, row)| {
            let (account, name) = split_key(&key)?;
            ObjectLambdaPoint::decode(&account, &name, &row)
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_object_lambda_point_reads_back_and_refuses_damage() {
        let point = ObjectLambdaPoint {
            account: "111111111111".into(),
            name: "shaper".into(),
            configuration: "<Configuration/>".into(),
            alias: "shaper-abc--ol-s3".into(),
            created_ms: 7,
        };
        let bytes = point.encode();
        assert_eq!(
            ObjectLambdaPoint::decode("111111111111", "shaper", &bytes).unwrap(),
            point
        );
        for len in 0..bytes.len() {
            assert!(
                ObjectLambdaPoint::decode("a", "b", &bytes[..len]).is_err(),
                "{len}"
            );
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(ObjectLambdaPoint::decode("a", "b", &longer).is_err());
        assert_eq!(
            split_key(&point_key("111111111111", "shaper")).unwrap(),
            ("111111111111".to_string(), "shaper".to_string())
        );
    }
}
