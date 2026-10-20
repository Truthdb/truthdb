//! S3 accounts and their users. An account owns buckets and access keys; an
//! access key belongs to the account itself (its root) or to one of its
//! users. Accounts are named by 12-digit ids, as in AWS.
//!
//! The account [`DEFAULT_ACCOUNT`] always exists, without a row: it owns what
//! was created before there were accounts (access keys and buckets whose rows
//! name no owner), and it cannot be dropped.

use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::buckets::BucketRow;
use crate::credentials::KeyRow;
use crate::{ACCOUNTS_TREE, ObjFile, ObjTxn, USERS_TREE, codec};

/// The account that owns everything created before accounts existed.
pub const DEFAULT_ACCOUNT: &str = "000000000000";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountInfo {
    pub id: String,
    pub name: String,
    pub created_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserInfo {
    pub account: String,
    pub name: String,
    /// The user's unique id (`AIDA…`), as IAM names it.
    pub user_id: String,
    pub created_ms: u64,
}

/// Why an account or user operation was refused (the storage itself worked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountError {
    NoSuchAccount,
    AccountExists,
    /// The default account cannot be dropped.
    DefaultAccount,
    NoSuchUser,
    UserExists,
    /// The account or user still has these.
    NotEmpty(&'static str),
    InvalidName(&'static str),
}

/// Whether `id` is an account id: 12 ASCII digits.
pub fn valid_account_id(id: &str) -> bool {
    id.len() == 12 && id.bytes().all(|b| b.is_ascii_digit())
}

/// IAM's name rules, for account and user names: 1 to 64 letters, digits and
/// `+=,.@_-`.
fn check_name(name: &str) -> Result<(), AccountError> {
    if !(1..=64).contains(&name.len())
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+=,.@_-".contains(&b))
    {
        return Err(AccountError::InvalidName(
            "must be 1 to 64 letters, digits or +=,.@_- characters",
        ));
    }
    Ok(())
}

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("corrupt {what}"))
}

/// An account row: creation time, then the name.
fn encode_account(name: &str, created_ms: u64) -> Vec<u8> {
    [&created_ms.to_le_bytes(), name.as_bytes()].concat()
}

fn decode_account(id: &[u8], row: &[u8]) -> Result<AccountInfo, StorageError> {
    if row.len() < 8 {
        return Err(corrupt("account row"));
    }
    Ok(AccountInfo {
        id: String::from_utf8(id.to_vec()).map_err(|_| corrupt("account id"))?,
        name: String::from_utf8(row[8..].to_vec()).map_err(|_| corrupt("account name"))?,
        created_ms: codec::u64_le(&row[..8])?,
    })
}

fn default_account() -> AccountInfo {
    AccountInfo {
        id: DEFAULT_ACCOUNT.to_string(),
        name: "default".to_string(),
        created_ms: 0,
    }
}

/// A user's key: the account, a 0 byte (account ids have none), the name.
fn user_key(account: &str, name: &str) -> Vec<u8> {
    [account.as_bytes(), &[0], name.as_bytes()].concat()
}

/// The first user key of `account` and the first key past them.
fn user_range(account: &str) -> (Vec<u8>, Vec<u8>) {
    (
        [account.as_bytes(), &[0]].concat(),
        [account.as_bytes(), &[1]].concat(),
    )
}

/// A user row: creation time, then the user id.
fn decode_user(key: &[u8], row: &[u8]) -> Result<UserInfo, StorageError> {
    let (account, name) = key
        .split_at_checked(12)
        .filter(|(_, rest)| rest.first() == Some(&0))
        .ok_or_else(|| corrupt("user key"))?;
    if row.len() < 8 {
        return Err(corrupt("user row"));
    }
    Ok(UserInfo {
        account: String::from_utf8(account.to_vec()).map_err(|_| corrupt("user key"))?,
        name: String::from_utf8(name[1..].to_vec()).map_err(|_| corrupt("user name"))?,
        user_id: String::from_utf8(row[8..].to_vec()).map_err(|_| corrupt("user id"))?,
        created_ms: codec::u64_le(&row[..8])?,
    })
}

impl ObjTxn<'_, '_> {
    /// Creates account `id` (a fresh 12-digit id the caller drew) named
    /// `name`.
    pub fn create_account(
        &mut self,
        id: &str,
        name: &str,
        now_ms: u64,
    ) -> Result<Result<(), AccountError>, StorageError> {
        if let Err(err) = check_name(name) {
            return Ok(Err(err));
        }
        if !valid_account_id(id) {
            return Ok(Err(AccountError::InvalidName("an account id is 12 digits")));
        }
        if id == DEFAULT_ACCOUNT {
            return Ok(Err(AccountError::AccountExists));
        }
        let inserted = self.trees.accounts().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            id.as_bytes(),
            &encode_account(name, now_ms),
        )?;
        Ok(match inserted {
            TreeInsert::Inserted => Ok(()),
            TreeInsert::DuplicateKey => Err(AccountError::AccountExists),
        })
    }

    /// Drops an account that has no users, access keys or buckets left (the
    /// caller checks the other engines' buckets).
    pub fn drop_account(&mut self, id: &str) -> Result<Result<(), AccountError>, StorageError> {
        if id == DEFAULT_ACCOUNT {
            return Ok(Err(AccountError::DefaultAccount));
        }
        if self
            .trees
            .accounts()
            .get(self.ctx, id.as_bytes())?
            .is_none()
        {
            return Ok(Err(AccountError::NoSuchAccount));
        }
        let (lower, upper) = user_range(id);
        if !self
            .trees
            .users()
            .scan_range_limit(self.ctx, Some(&lower), Some(&upper), 1)?
            .is_empty()
        {
            return Ok(Err(AccountError::NotEmpty("users")));
        }
        for (_, row) in self.trees.credentials().scan(self.ctx)? {
            if KeyRow::decode(&row)?.account == id {
                return Ok(Err(AccountError::NotEmpty("access keys")));
            }
        }
        for (_, row) in self.trees.buckets().scan(self.ctx)? {
            if BucketRow::decode(&row)?.owner == id {
                return Ok(Err(AccountError::NotEmpty("buckets")));
            }
        }
        self.trees
            .accounts()
            .delete(self.ctx, &mut OpMode::Txn(self.txn), id.as_bytes())?;
        Ok(Ok(()))
    }

    /// Whether account `id` exists (the default account always does).
    pub(crate) fn account_exists(&mut self, id: &str) -> Result<bool, StorageError> {
        Ok(id == DEFAULT_ACCOUNT
            || self
                .trees
                .accounts()
                .get(self.ctx, id.as_bytes())?
                .is_some())
    }

    /// Whether user `name` of `account` exists.
    pub(crate) fn user_exists(&mut self, account: &str, name: &str) -> Result<bool, StorageError> {
        Ok(self
            .trees
            .users()
            .get(self.ctx, &user_key(account, name))?
            .is_some())
    }

    /// Creates user `name` in `account`, with `user_id` (a fresh id the caller
    /// drew).
    pub fn create_user(
        &mut self,
        account: &str,
        name: &str,
        user_id: &str,
        now_ms: u64,
    ) -> Result<Result<(), AccountError>, StorageError> {
        if let Err(err) = check_name(name) {
            return Ok(Err(err));
        }
        if !self.account_exists(account)? {
            return Ok(Err(AccountError::NoSuchAccount));
        }
        let inserted = self.trees.users().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &user_key(account, name),
            &[&now_ms.to_le_bytes(), user_id.as_bytes()].concat(),
        )?;
        Ok(match inserted {
            TreeInsert::Inserted => Ok(()),
            TreeInsert::DuplicateKey => Err(AccountError::UserExists),
        })
    }

    /// Drops a user that has no access keys left.
    pub fn drop_user(
        &mut self,
        account: &str,
        name: &str,
    ) -> Result<Result<(), AccountError>, StorageError> {
        if !self.user_exists(account, name)? {
            return Ok(Err(AccountError::NoSuchUser));
        }
        for (_, row) in self.trees.credentials().scan(self.ctx)? {
            let key = KeyRow::decode(&row)?;
            if key.account == account && key.user.as_deref() == Some(name) {
                return Ok(Err(AccountError::NotEmpty("access keys")));
            }
        }
        self.trees.users().delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &user_key(account, name),
        )?;
        Ok(Ok(()))
    }
}

impl ObjFile<'_> {
    /// Account `id`, if it exists.
    pub fn account(&mut self, id: &str) -> Result<Option<AccountInfo>, StorageError> {
        if id == DEFAULT_ACCOUNT {
            return Ok(Some(default_account()));
        }
        let Some(accounts) = self.existing_tree(ACCOUNTS_TREE)? else {
            return Ok(None);
        };
        let mut ctx = self.core.page_ctx(0);
        accounts
            .get(&mut ctx, id.as_bytes())?
            .map(|row| decode_account(id.as_bytes(), &row))
            .transpose()
    }

    /// Every account, by id (the default account first).
    pub fn accounts(&mut self) -> Result<Vec<AccountInfo>, StorageError> {
        let mut out = vec![default_account()];
        if let Some(accounts) = self.existing_tree(ACCOUNTS_TREE)? {
            let mut ctx = self.core.page_ctx(0);
            for (id, row) in accounts.scan(&mut ctx)? {
                out.push(decode_account(&id, &row)?);
            }
        }
        Ok(out)
    }

    /// The users of `account`, by name.
    pub fn users(&mut self, account: &str) -> Result<Vec<UserInfo>, StorageError> {
        let Some(users) = self.existing_tree(USERS_TREE)? else {
            return Ok(Vec::new());
        };
        let (lower, upper) = user_range(account);
        let mut ctx = self.core.page_ctx(0);
        users
            .scan_range(&mut ctx, Some(&lower), Some(&upper))?
            .into_iter()
            .map(|(key, row)| decode_user(&key, &row))
            .collect()
    }
}
