//! Where a version's own state is kept: its attributes (the attrs tree),
//! its Object Lock settings (the locks tree) and its restore (the restores
//! tree, and the restore timers and ready trees ordering restores by time).
//! The rows are keyed by the version's bucket, key and number (its row key
//! in the objects tree), so a version number is the same version's only in
//! its own bucket and key: a replica keeps its source's number.
//!
//! Files written before were keyed by the number alone. A migration walks
//! the keys in order (`rekey_step`) and moves their versions' rows to the
//! new keys; until it ends, a key's versions are keyed by the number unless
//! the walk has passed it (`keyed`), so every row of a version is in one
//! form. The new keys begin with a bucket name's first byte (a letter or a
//! digit), and the old with a 0 byte (the uploads' rows in the same trees
//! with a 1 byte): the two forms cannot meet. A timer key is the time, then
//! the restore row's key.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::objects::{FLAG_DELETE_MARKER, VersionRow, key_prefix, version_from_key, version_key};
use crate::{ObjFile, ObjTxn};

/// META key of the migration's position: the keys before it are keyed.
const CURSOR_KEY: &[u8] = b"rekey_cursor";
/// META key set once every key is keyed (or, in a file the engine starts
/// in, when it starts: there is nothing to move).
pub(crate) const DONE_KEY: &[u8] = b"rekey_done";

/// The key of a version's state rows (its attributes' chunks add their
/// number): its row key once `keyed`, its number before.
pub(crate) fn state_key(keyed: bool, bucket: &str, key: &str, version: u64) -> Vec<u8> {
    if keyed {
        version_key(bucket, key, version)
    } else {
        version.to_be_bytes().to_vec()
    }
}

/// A timer's key: its time, then its restore row's key.
pub(crate) fn timer_key(time_ms: u64, state: &[u8]) -> Vec<u8> {
    [&time_ms.to_be_bytes()[..], state].concat()
}

/// A timer key's time and restore row key.
pub(crate) fn decode_timer_key(key: &[u8]) -> Result<(u64, &[u8]), StorageError> {
    if key.len() < 16 {
        return Err(StorageError::InvalidFile("corrupt timer key".to_string()));
    }
    let (time, state) = key.split_at(8);
    Ok((u64::from_be_bytes(time.try_into().unwrap()), state))
}

/// The version number a state key names.
pub(crate) fn state_version(state: &[u8]) -> Result<u64, StorageError> {
    match state.len() {
        8 => Ok(u64::from_be_bytes(state.try_into().unwrap())),
        _ => version_from_key(state),
    }
}

fn after_key(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    out.extend_from_slice(&[0xFF; 8]);
    out.push(0);
    out
}

impl ObjTxn<'_, '_> {
    /// Whether the state of `bucket`'s `key`'s versions is keyed by them.
    pub(crate) fn keyed(&mut self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let meta = self.trees.meta();
        if meta.get(self.ctx, DONE_KEY)?.is_some() {
            return Ok(true);
        }
        Ok(meta
            .get(self.ctx, CURSOR_KEY)?
            .is_some_and(|cursor| key_prefix(bucket, key) < cursor))
    }

    /// The key of a version's state rows, in the form its key has now.
    pub(crate) fn version_state_key(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let keyed = self.keyed(bucket, key)?;
        Ok(state_key(keyed, bucket, key, version))
    }

    /// One step of the migration: moves the state of the versions of the
    /// keys from its position on, about `rows` version rows of them, and
    /// moves past them. Returns whether keys remain.
    pub fn rekey_step(&mut self, rows: usize) -> Result<bool, StorageError> {
        let meta = self.trees.meta();
        if meta.get(self.ctx, DONE_KEY)?.is_some() {
            return Ok(false);
        }
        let start = meta.get(self.ctx, CURSOR_KEY)?.unwrap_or_default();
        let limit = rows.max(1);
        let objects = self.trees.objects();
        let found = objects.scan_range_limit(self.ctx, Some(&start), None, limit)?;
        // The step ends after the last key it reached, and moves each key
        // before it whole, however many versions it has.
        let end = (found.len() >= limit)
            .then(|| {
                found
                    .last()
                    .map(|(row_key, _)| after_key(&row_key[..row_key.len() - 8]))
            })
            .flatten();
        let mut prefixes: Vec<Vec<u8>> = Vec::new();
        for (row_key, _) in &found {
            let prefix = row_key[..row_key.len() - 8].to_vec();
            if prefixes.last() != Some(&prefix) {
                prefixes.push(prefix);
            }
        }
        for prefix in &prefixes {
            let mut upper = prefix.clone();
            upper.extend_from_slice(&[0xFF; 8]);
            for (row_key, row) in objects.scan_range(self.ctx, Some(prefix), Some(&upper))? {
                if VersionRow::decode(&row)?.flags & FLAG_DELETE_MARKER == 0 {
                    let version = version_from_key(&row_key)?;
                    self.move_state(&version.to_be_bytes(), &row_key)?;
                }
            }
        }
        let mode = &mut OpMode::Txn(self.txn);
        match end {
            Some(end) => {
                meta.delete(self.ctx, mode, CURSOR_KEY)?;
                meta.insert_unique(self.ctx, mode, CURSOR_KEY, &end)?;
                Ok(true)
            }
            None => {
                meta.delete(self.ctx, mode, CURSOR_KEY)?;
                meta.insert_unique(self.ctx, mode, DONE_KEY, &[1])?;
                Ok(false)
            }
        }
    }

    /// Moves one version's attributes, lock and restore (with its timers)
    /// from the rows under `from` to rows under `to`.
    fn move_state(&mut self, from: &[u8], to: &[u8]) -> Result<(), StorageError> {
        let mode = &mut OpMode::Txn(self.txn);
        let attrs = self.trees.attrs();
        let lower = [from, &[0, 0]].concat();
        let upper = [from, &[0xFF, 0xFF]].concat();
        for (chunk_key, chunk) in attrs.scan_range(self.ctx, Some(&lower), Some(&upper))? {
            attrs.delete(self.ctx, mode, &chunk_key)?;
            let moved = [to, &chunk_key[from.len()..]].concat();
            attrs.insert_unique(self.ctx, mode, &moved, &chunk)?;
        }
        let locks = self.trees.locks();
        if let Some(lock) = locks.delete(self.ctx, mode, from)? {
            locks.insert_unique(self.ctx, mode, to, &lock)?;
        }
        let restores = self.trees.restores();
        if let Some(row) = restores.delete(self.ctx, mode, from)? {
            let restore = crate::restores::restore_times(&row)?;
            restores.insert_unique(self.ctx, mode, to, &row)?;
            for (tree, time) in [
                (self.trees.restore_timers(), restore.expiry_ms),
                (self.trees.restore_ready(), restore.ready_ms),
            ] {
                if tree
                    .delete(self.ctx, mode, &timer_key(time, from))?
                    .is_some()
                {
                    tree.insert_unique(self.ctx, mode, &timer_key(time, to), &[])?;
                }
            }
        }
        Ok(())
    }

    /// Forgets that the migration ran, as in a file written before it: the
    /// keys are keyed by number again from the start. For tests that write
    /// such a file; the trees must hold no version state yet.
    #[cfg(test)]
    pub(crate) fn unkey_versions(&mut self) -> Result<(), StorageError> {
        let meta = self.trees.meta();
        for key in [CURSOR_KEY, DONE_KEY] {
            meta.delete(self.ctx, &mut OpMode::Txn(self.txn), key)?;
        }
        Ok(())
    }
}

impl ObjFile<'_> {
    /// Whether the state of `bucket`'s `key`'s versions is keyed by them.
    pub(crate) fn keyed(&mut self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let Some(root) = self.eng.root else {
            return Ok(true);
        };
        let meta = crate::meta_tree(root);
        let mut ctx = self.core.page_ctx(0);
        if meta.get(&mut ctx, DONE_KEY)?.is_some() {
            return Ok(true);
        }
        Ok(meta
            .get(&mut ctx, CURSOR_KEY)?
            .is_some_and(|cursor| key_prefix(bucket, key) < cursor))
    }

    /// The key of a version's state rows, in the form its key has now.
    pub(crate) fn version_state_key(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let keyed = self.keyed(bucket, key)?;
        Ok(state_key(keyed, bucket, key, version))
    }

    /// One step of the migration, in a transaction of its own; returns
    /// whether keys remain.
    pub fn rekey_versions(&mut self, rows: usize) -> Result<bool, StorageError> {
        // A file the engine never wrote has nothing to move, and is not
        // made to hold the engine by moving it.
        if self.eng.root.is_none() || self.versions_keyed()? {
            return Ok(false);
        }
        self.transaction(|tx| tx.rekey_step(rows))
    }

    /// Whether every version's state is keyed by it (the migration is done).
    pub fn versions_keyed(&mut self) -> Result<bool, StorageError> {
        self.core.ensure_rel_usable()?;
        let Some(root) = self.eng.root else {
            return Ok(false);
        };
        let meta = crate::meta_tree(root);
        let mut ctx = self.core.page_ctx(0);
        Ok(meta.get(&mut ctx, DONE_KEY)?.is_some())
    }
}
