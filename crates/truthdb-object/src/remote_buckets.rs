//! Buckets on another S3 service that replication may send to (`s3 create
//! remote bucket`): each by its name, with the account of this server it is
//! recorded for (only that account's buckets replicate to it, with its
//! keys), and the endpoint, region and keys to reach it. A replication destination names a bucket of this server first,
//! then one of these (bucket names are global in S3: one name, one place).
//! The secret lives in the database, which whole-database encryption covers.

use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::{Reader, Writer};
use crate::{ObjFile, ObjTxn, REMOTE_BUCKETS_TREE};

const FORMAT: u8 = 1;

/// A bucket on another S3 service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteBucket {
    pub name: String,
    /// The account of this server whose buckets may replicate to it.
    pub account: String,
    /// `http(s)://host[:port]`.
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret: String,
    /// The PEM certificates to trust for it (none: the system's).
    pub ca_pem: Option<Vec<u8>>,
    pub created_ms: u64,
}

impl RemoteBucket {
    fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(FORMAT);
        out.str(&self.account);
        out.str(&self.endpoint);
        out.str(&self.region);
        out.str(&self.access_key);
        out.str(&self.secret);
        match &self.ca_pem {
            None => out.u8(0),
            Some(pem) => {
                out.u8(1);
                out.u32(pem.len() as u32);
                out.0.extend_from_slice(pem);
            }
        }
        out.u64(self.created_ms);
        out.0
    }

    fn decode(name: &str, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader::new(bytes, "remote bucket");
        if input.u8()? != FORMAT {
            return Err(input.corrupt());
        }
        let account = input.str()?;
        let endpoint = input.str()?;
        let region = input.str()?;
        let access_key = input.str()?;
        let secret = input.str()?;
        let ca_pem = if input.present()? {
            let len = input.count()?;
            let rest = input.rest();
            let pem = rest.get(..len).ok_or_else(|| input.corrupt())?.to_vec();
            input = Reader::new(&rest[len..], "remote bucket");
            Some(pem)
        } else {
            None
        };
        let created_ms = input.u64()?;
        if !input.rest().is_empty() {
            return Err(input.corrupt());
        }
        Ok(RemoteBucket {
            name: name.to_string(),
            account,
            endpoint,
            region,
            access_key,
            secret,
            ca_pem,
            created_ms,
        })
    }
}

impl ObjTxn<'_, '_> {
    /// Records a remote bucket; false if one of that name is recorded. The
    /// caller checks no bucket of this server has the name.
    pub fn create_remote_bucket(&mut self, remote: &RemoteBucket) -> Result<bool, StorageError> {
        let inserted = self.trees.remote_buckets().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            remote.name.as_bytes(),
            &remote.encode(),
        )?;
        Ok(inserted == TreeInsert::Inserted)
    }

    /// Whether a remote bucket is recorded for account `account`.
    pub(crate) fn has_remote_buckets(&mut self, account: &str) -> Result<bool, StorageError> {
        for (key, row) in self.trees.remote_buckets().scan(self.ctx)? {
            let name = String::from_utf8(key)
                .map_err(|_| StorageError::InvalidFile("corrupt remote bucket".to_string()))?;
            if RemoteBucket::decode(&name, &row)?.account == account {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Forgets remote bucket `name`; whether it was recorded.
    pub fn drop_remote_bucket(&mut self, name: &str) -> Result<bool, StorageError> {
        Ok(self
            .trees
            .remote_buckets()
            .delete(self.ctx, &mut OpMode::Txn(self.txn), name.as_bytes())?
            .is_some())
    }
}

impl ObjFile<'_> {
    /// Remote bucket `name`, if recorded.
    pub fn remote_bucket(&mut self, name: &str) -> Result<Option<RemoteBucket>, StorageError> {
        let Some(tree) = self.existing_tree(REMOTE_BUCKETS_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), name.as_bytes())?
            .map(|row| RemoteBucket::decode(name, &row))
            .transpose()
    }

    /// Every remote bucket, by name.
    pub fn remote_buckets(&mut self) -> Result<Vec<RemoteBucket>, StorageError> {
        let Some(tree) = self.existing_tree(REMOTE_BUCKETS_TREE)? else {
            return Ok(Vec::new());
        };
        tree.scan(&mut self.core.page_ctx(0))?
            .into_iter()
            .map(|(key, row)| {
                let name = String::from_utf8(key)
                    .map_err(|_| StorageError::InvalidFile("corrupt remote bucket".to_string()))?;
                RemoteBucket::decode(&name, &row)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_remote_bucket_reads_back_and_refuses_damage() {
        for ca_pem in [None, Some(b"-----BEGIN CERTIFICATE-----".to_vec())] {
            let remote = RemoteBucket {
                name: "far".into(),
                account: "111111111111".into(),
                endpoint: "https://s3.example:9000".into(),
                region: "eu-west-1".into(),
                access_key: "AKIA".into(),
                secret: "s3cr3t".into(),
                ca_pem,
                created_ms: 7,
            };
            let bytes = remote.encode();
            assert_eq!(RemoteBucket::decode("far", &bytes).unwrap(), remote);
            for len in 0..bytes.len() {
                assert!(RemoteBucket::decode("far", &bytes[..len]).is_err(), "{len}");
            }
            let mut longer = bytes.clone();
            longer.push(0);
            assert!(RemoteBucket::decode("far", &longer).is_err());
        }
    }
}
