//! The S3 API's access keys: an access key id, its owner (an account, or one
//! of the account's users) and its secret. The secret is stored as given,
//! because request signing (SigV4) needs it back, not a hash of it.
//!
//! So the secret is in plaintext wherever the database's pages and log go:
//! the data file, the WAL, every standby the WAL ships to, and full and log
//! backups (all copied verbatim). A restore to a point before
//! `s3 drop access key` brings the dropped key back as valid.

use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::accounts::{AccountError, DEFAULT_ACCOUNT};
use crate::{CREDENTIALS_TREE, ObjFile, ObjTxn, codec};

fn invalid(message: String) -> StorageError {
    StorageError::InvalidConfig(message)
}

/// Longest access key id or secret accepted.
const MAX_LEN: usize = 128;

fn check(what: &str, value: &str) -> Result<(), StorageError> {
    // Printable: a stored row tells the old format (a bare secret) from the
    // current one by its first byte.
    if value.is_empty() || value.len() > MAX_LEN || !value.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(invalid(format!(
            "an access key {what} is 1..={MAX_LEN} printable ASCII characters"
        )));
    }
    Ok(())
}

/// An access key, without its secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessKey {
    pub id: String,
    /// The account the key acts for.
    pub account: String,
    /// The account's user the key belongs to; `None` for the account's root.
    pub user: Option<String>,
    pub active: bool,
    pub created_ms: u64,
}

/// A credential row's first byte in the current format. A row written before
/// keys had owners is the bare secret, printable ASCII: it is a root key of
/// [`DEFAULT_ACCOUNT`].
const KEY_ROW_V2: u8 = 1;

/// A stored credential row: `[1][account 12][active u8][created u64]
/// [user length u8][user][secret]`.
pub(crate) struct KeyRow {
    pub account: String,
    pub user: Option<String>,
    pub active: bool,
    pub created_ms: u64,
    pub secret: String,
}

impl KeyRow {
    fn encode(&self) -> Vec<u8> {
        let user = self.user.as_deref().unwrap_or("");
        let mut out = vec![KEY_ROW_V2];
        out.extend_from_slice(self.account.as_bytes());
        out.push(u8::from(self.active));
        out.extend_from_slice(&self.created_ms.to_le_bytes());
        out.push(user.len() as u8);
        out.extend_from_slice(user.as_bytes());
        out.extend_from_slice(self.secret.as_bytes());
        out
    }

    pub fn decode(row: &[u8]) -> Result<Self, StorageError> {
        let corrupt = || StorageError::InvalidFile("corrupt access key row".to_string());
        let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).map_err(|_| corrupt());
        if row.first() != Some(&KEY_ROW_V2) {
            return Ok(KeyRow {
                account: DEFAULT_ACCOUNT.to_string(),
                user: None,
                active: true,
                created_ms: 0,
                secret: text(row)?,
            });
        }
        let user_len = *row.get(22).ok_or_else(corrupt)? as usize;
        if row.len() < 23 + user_len {
            return Err(corrupt());
        }
        Ok(KeyRow {
            account: text(&row[1..13])?,
            active: row[13] != 0,
            created_ms: codec::u64_le(&row[14..22])?,
            user: (user_len > 0)
                .then(|| text(&row[23..23 + user_len]))
                .transpose()?,
            secret: text(&row[23 + user_len..])?,
        })
    }

    fn key(self, id: String) -> AccessKey {
        AccessKey {
            id,
            account: self.account,
            user: self.user,
            active: self.active,
            created_ms: self.created_ms,
        }
    }
}

impl ObjTxn<'_, '_> {
    /// Creates access key `id` with `secret` for `account`'s root, or for its
    /// user `user`.
    pub fn create_access_key(
        &mut self,
        id: &str,
        secret: &str,
        account: &str,
        user: Option<&str>,
        now_ms: u64,
    ) -> Result<Result<(), AccountError>, StorageError> {
        check("id", id)?;
        check("secret", secret)?;
        if !self.account_exists(account)? {
            return Ok(Err(AccountError::NoSuchAccount));
        }
        if let Some(user) = user
            && !self.user_exists(account, user)?
        {
            return Ok(Err(AccountError::NoSuchUser));
        }
        let row = KeyRow {
            account: account.to_string(),
            user: user.map(str::to_string),
            active: true,
            created_ms: now_ms,
            secret: secret.to_string(),
        };
        let credentials = self.trees.credentials();
        let inserted = credentials.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            id.as_bytes(),
            &row.encode(),
        )?;
        if inserted != TreeInsert::Inserted {
            return Err(invalid(format!("access key {id} already exists")));
        }
        Ok(Ok(()))
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
    /// Access key `id` and its secret, if it exists.
    pub fn access_key(&mut self, id: &str) -> Result<Option<(AccessKey, String)>, StorageError> {
        let Some(credentials) = self.existing_tree(CREDENTIALS_TREE)? else {
            return Ok(None);
        };
        let mut ctx = self.core.page_ctx(0);
        credentials
            .get(&mut ctx, id.as_bytes())?
            .map(|row| {
                let mut row = KeyRow::decode(&row)?;
                let secret = std::mem::take(&mut row.secret);
                Ok((row.key(id.to_string()), secret))
            })
            .transpose()
    }

    /// Every access key, by id.
    pub fn access_keys(&mut self) -> Result<Vec<AccessKey>, StorageError> {
        let Some(credentials) = self.existing_tree(CREDENTIALS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut ctx = self.core.page_ctx(0);
        credentials
            .scan(&mut ctx)?
            .into_iter()
            .map(|(id, row)| {
                let id = String::from_utf8(id)
                    .map_err(|_| StorageError::InvalidFile("corrupt access key id".to_string()))?;
                Ok(KeyRow::decode(&row)?.key(id))
            })
            .collect()
    }
}
