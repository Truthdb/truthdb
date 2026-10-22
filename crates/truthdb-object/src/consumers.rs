//! The change journal's consumers: each reads the journal in order and keeps
//! its place (a cursor, the last row it has applied) in the consumers tree.
//! The journal is not trimmed past the slowest consumer's cursor, but only
//! for so long: a row older than the hold cap goes whatever the consumers,
//! and a consumer still before it is moved past it, the rows it missed
//! counted. The full-text indexer is the consumer `fulltext`.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::{CONSUMERS_TREE, FT_CURSOR_KEY, ObjFile, ObjTxn, codec};

/// The full-text indexer's consumer name.
pub const FULLTEXT_CONSUMER: &str = "fulltext";

/// A journal consumer as kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Consumer {
    pub name: String,
    /// The last journal row the consumer has applied.
    pub cursor: u64,
    /// Journal rows trimmed before the consumer read them.
    pub dropped: u64,
}

/// `[cursor u64][dropped u64]`.
fn encode(cursor: u64, dropped: u64) -> [u8; 16] {
    let mut row = [0u8; 16];
    row[..8].copy_from_slice(&cursor.to_le_bytes());
    row[8..].copy_from_slice(&dropped.to_le_bytes());
    row
}

fn decode(name: &[u8], row: &[u8]) -> Result<Consumer, StorageError> {
    let corrupt = || StorageError::InvalidFile("corrupt journal consumer".to_string());
    if row.len() != 16 {
        return Err(corrupt());
    }
    Ok(Consumer {
        name: String::from_utf8(name.to_vec()).map_err(|_| corrupt())?,
        cursor: u64::from_le_bytes(row[..8].try_into().unwrap()),
        dropped: u64::from_le_bytes(row[8..].try_into().unwrap()),
    })
}

impl ObjTxn<'_, '_> {
    /// Every consumer: the kept ones, and the full-text indexer of a file
    /// written before consumers were (its cursor then in the directory tree,
    /// and a consumer while any bucket has full-text state).
    pub(crate) fn consumers(&mut self) -> Result<Vec<Consumer>, StorageError> {
        let mut consumers = self
            .trees
            .consumers()
            .scan(self.ctx)?
            .iter()
            .map(|(name, row)| decode(name, row))
            .collect::<Result<Vec<_>, _>>()?;
        if !consumers.iter().any(|c| c.name == FULLTEXT_CONSUMER)
            && !self
                .trees
                .ft_buckets()
                .scan_range_limit(self.ctx, None, None, 1)?
                .is_empty()
        {
            let cursor = self.trees.meta().get(self.ctx, FT_CURSOR_KEY)?;
            consumers.push(Consumer {
                name: FULLTEXT_CONSUMER.to_string(),
                cursor: cursor
                    .map(|row| codec::u64_le(&row))
                    .transpose()?
                    .unwrap_or(0),
                dropped: 0,
            });
        }
        Ok(consumers)
    }

    /// Keeps `consumer` as it is now (its cursor and dropped count), in
    /// place of the full-text indexer's cursor in the directory tree.
    pub(crate) fn put_consumer(&mut self, consumer: &Consumer) -> Result<(), StorageError> {
        let tree = self.trees.consumers();
        let row = encode(consumer.cursor, consumer.dropped);
        let mode = &mut OpMode::Txn(self.txn);
        if tree
            .update(self.ctx, mode, consumer.name.as_bytes(), &row)?
            .is_none()
        {
            tree.insert_unique(self.ctx, mode, consumer.name.as_bytes(), &row)?;
        }
        if consumer.name == FULLTEXT_CONSUMER {
            self.trees.meta().delete(self.ctx, mode, FT_CURSOR_KEY)?;
        }
        Ok(())
    }

    /// Moves consumer `name`'s cursor forward to `sequence` (registering
    /// it). A cursor never moves back: a consumer that applied rows a trim
    /// has since moved it past keeps the trim's place.
    pub(crate) fn advance_consumer(
        &mut self,
        name: &str,
        sequence: u64,
    ) -> Result<(), StorageError> {
        let kept = self.consumers()?.into_iter().find(|c| c.name == name);
        self.put_consumer(&Consumer {
            name: name.to_string(),
            cursor: kept.as_ref().map_or(sequence, |c| c.cursor.max(sequence)),
            dropped: kept.map_or(0, |c| c.dropped),
        })
    }

    /// Forgets consumer `name`: the journal is no longer held for it.
    pub(crate) fn remove_consumer(&mut self, name: &str) -> Result<(), StorageError> {
        let mode = &mut OpMode::Txn(self.txn);
        self.trees
            .consumers()
            .delete(self.ctx, mode, name.as_bytes())?;
        if name == FULLTEXT_CONSUMER {
            self.trees.meta().delete(self.ctx, mode, FT_CURSOR_KEY)?;
        }
        Ok(())
    }
}

impl ObjFile<'_> {
    /// Every consumer (see [`ObjTxn::consumers`]), read outside a
    /// transaction.
    pub fn journal_consumers(&mut self) -> Result<Vec<Consumer>, StorageError> {
        let mut consumers = match self.existing_tree(CONSUMERS_TREE)? {
            Some(tree) => tree
                .scan(&mut self.core.page_ctx(0))?
                .iter()
                .map(|(name, row)| decode(name, row))
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };
        if !consumers.iter().any(|c| c.name == FULLTEXT_CONSUMER)
            && !self.fulltext_buckets()?.is_empty()
        {
            consumers.push(Consumer {
                name: FULLTEXT_CONSUMER.to_string(),
                cursor: self.legacy_fulltext_cursor()?,
                dropped: 0,
            });
        }
        Ok(consumers)
    }

    /// The full-text indexer's cursor as a file written before consumers
    /// kept it (0 when it has none).
    fn legacy_fulltext_cursor(&mut self) -> Result<u64, StorageError> {
        let Some(meta) = self.eng.root.map(crate::meta_tree) else {
            return Ok(0);
        };
        self.core.ensure_rel_usable()?;
        Ok(meta
            .get(&mut self.core.page_ctx(0), FT_CURSOR_KEY)?
            .map(|row| codec::u64_le(&row))
            .transpose()?
            .unwrap_or(0))
    }
}
