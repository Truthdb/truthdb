//! S3 access points: named network endpoints of a bucket, an account's
//! own (unique per account in a region; this server is one region). Each
//! is kept under its account and name, with its bucket (and that bucket's
//! owner), its network origin's VPC (none: Internet), its Block Public
//! Access settings (fixed at creation), its alias (generated, unchangeable;
//! indexed, as data requests may name it in place of a bucket), its
//! creation time, and its policy, if put.

use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::{Reader, Writer};
use crate::{ACCESS_POINTS_TREE, ObjFile, ObjTxn};

const FORMAT: u8 = 1;
/// The kinds of row: an access point, its alias, its policy.
const POINT: u8 = b'P';
const ALIAS: u8 = b'A';
const POLICY: u8 = b'Q';

/// An access point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessPoint {
    pub account: String,
    pub name: String,
    pub bucket: String,
    /// The account that owns the bucket.
    pub bucket_account: String,
    /// The VPC its network origin is (none: the Internet).
    pub vpc: Option<String>,
    /// BlockPublicAcls, IgnorePublicAcls, BlockPublicPolicy,
    /// RestrictPublicBuckets.
    pub block_public: [bool; 4],
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

impl AccessPoint {
    fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(FORMAT);
        out.str(&self.bucket);
        out.str(&self.bucket_account);
        match &self.vpc {
            None => out.u8(0),
            Some(vpc) => {
                out.u8(1);
                out.str(vpc);
            }
        }
        out.u8(self
            .block_public
            .iter()
            .enumerate()
            .fold(0, |bits, (at, on)| bits | (u8::from(*on) << at)));
        out.str(&self.alias);
        out.u64(self.created_ms);
        out.0
    }

    fn decode(account: &str, name: &str, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "access point");
        if input.u8()? != FORMAT {
            return Err(input.corrupt());
        }
        let bucket = input.str()?;
        let bucket_account = input.str()?;
        let vpc = if input.present()? {
            Some(input.str()?)
        } else {
            None
        };
        let bits = input.u8()?;
        if bits >> 4 != 0 {
            return Err(input.corrupt());
        }
        let block_public = [0, 1, 2, 3].map(|at| bits & (1 << at) != 0);
        let alias = input.str()?;
        let created_ms = input.u64()?;
        if !input.rest().is_empty() {
            return Err(input.corrupt());
        }
        Ok(AccessPoint {
            account: account.to_string(),
            name: name.to_string(),
            bucket,
            bucket_account,
            vpc,
            block_public,
            alias,
            created_ms,
        })
    }
}

/// The account and name a point or policy key holds.
fn split_key(key: &[u8]) -> Result<(String, String), StorageError> {
    let corrupt = || StorageError::InvalidFile("corrupt access point key".to_string());
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
    /// Records access point `point` (and its alias); false if its account
    /// has one of its name, or its alias is taken.
    pub fn create_access_point(&mut self, point: &AccessPoint) -> Result<bool, StorageError> {
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

    /// Removes `account`'s access point `name`, its alias and its policy;
    /// whether it was there.
    pub fn delete_access_point(&mut self, account: &str, name: &str) -> Result<bool, StorageError> {
        let tree = self.trees.tree(ACCESS_POINTS_TREE);
        let Some(row) = tree.delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &point_key(account, name),
        )?
        else {
            return Ok(false);
        };
        let point = AccessPoint::decode(account, name, &row)?;
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

    /// How many access points `account` has.
    pub fn access_point_count(&mut self, account: &str) -> Result<usize, StorageError> {
        let lower = point_key(account, "");
        let mut upper = vec![POINT];
        upper.extend_from_slice(account.as_bytes());
        upper.push(1);
        Ok(self
            .trees
            .tree(ACCESS_POINTS_TREE)
            .scan_range(self.ctx, Some(&lower), Some(&upper))?
            .len())
    }

    /// Puts (or, with none, deletes) the policy of `account`'s access point
    /// `name`; false if there is no such access point.
    pub fn set_access_point_policy(
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
    /// `account`'s access point `name`, if there is one.
    pub fn access_point(
        &mut self,
        account: &str,
        name: &str,
    ) -> Result<Option<AccessPoint>, StorageError> {
        let Some(tree) = self.existing_tree(ACCESS_POINTS_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), &point_key(account, name))?
            .map(|row| AccessPoint::decode(account, name, &row))
            .transpose()
    }

    /// The access point of alias `alias`, if there is one.
    pub fn access_point_by_alias(
        &mut self,
        alias: &str,
    ) -> Result<Option<AccessPoint>, StorageError> {
        let Some(tree) = self.existing_tree(ACCESS_POINTS_TREE)? else {
            return Ok(None);
        };
        let Some(target) = tree.get(&mut self.core.page_ctx(0), &alias_key(alias))? else {
            return Ok(None);
        };
        let mut key = vec![POINT];
        key.extend_from_slice(&target);
        let (account, name) = split_key(&key)?;
        self.access_point(&account, &name)
    }

    /// The policy of `account`'s access point `name`, if one is put.
    pub fn access_point_policy(
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
                    StorageError::InvalidFile("corrupt access point policy".to_string())
                })
            })
            .transpose()
    }

    /// `account`'s access points after name `after` (none: from the first),
    /// by name, at most `limit`.
    pub fn access_points(
        &mut self,
        account: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AccessPoint>, StorageError> {
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
            AccessPoint::decode(&account, &name, &row)
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_access_point_reads_back_and_refuses_damage() {
        for vpc in [None, Some("vpc-1a2b".to_string())] {
            let point = AccessPoint {
                account: "111111111111".into(),
                name: "reader".into(),
                bucket: "data".into(),
                bucket_account: "222222222222".into(),
                vpc,
                block_public: [true, false, true, false],
                alias: "reader-abc-s3alias".into(),
                created_ms: 7,
            };
            let bytes = point.encode();
            assert_eq!(
                AccessPoint::decode("111111111111", "reader", &bytes).unwrap(),
                point
            );
            for len in 0..bytes.len() {
                assert!(
                    AccessPoint::decode("a", "b", &bytes[..len]).is_err(),
                    "{len}"
                );
            }
            let mut longer = bytes.clone();
            longer.push(0);
            assert!(AccessPoint::decode("a", "b", &longer).is_err());
        }
        assert_eq!(
            split_key(&point_key("111111111111", "reader")).unwrap(),
            ("111111111111".to_string(), "reader".to_string())
        );
    }
}
