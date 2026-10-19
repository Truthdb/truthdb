//! The index's layout in two B+ trees, and the writes that keep it.
//!
//! An index holds any number of scopes (an object bucket, say), each its
//! own collection of documents with its own statistics. A scope is up to
//! 255 bytes, written length-first at the start of every key, so a scope's
//! rows sort together and any bytes can name one.
//!
//! - **Postings:** `scope, term, 0, doc (8 bytes BE)` -> term frequency and
//!   document length (u32 LE each). The 0 cannot occur in a term, so a
//!   term's postings sort together and before longer terms that share its
//!   prefix.
//! - **Documents:** `scope, doc (8 bytes BE), chunk (2 bytes BE)` -> the
//!   document's terms with their frequencies, chunked under the tree's cell
//!   cap; chunk 0 starts with the document's length. Unindexing reads them
//!   back to find the postings to delete.
//! - **Statistics:** `scope` alone, in the documents tree -> the scope's
//!   document count and total length (u64 LE each), for BM25.

use truthdb_core::relstore::btree::{BTree, TREE_MAX_CELL, TreeInsert};
use truthdb_core::relstore::ctx::{OpMode, RelCtx, TxnLink};
use truthdb_core::storage::StorageError;

use crate::tokenize::{DocTerms, MAX_TERM_BYTES};

/// The two trees an index lives in.
pub struct Index {
    pub postings: BTree,
    pub docs: BTree,
}

/// A scope's document count and total length in terms.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub docs: u64,
    pub total_len: u64,
}

/// One document's entry under a term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Posting {
    pub doc: u64,
    pub tf: u32,
    pub doc_len: u32,
}

fn scope_prefix(scope: &[u8]) -> Result<Vec<u8>, StorageError> {
    let len = u8::try_from(scope.len()).map_err(|_| {
        StorageError::InvalidConfig(format!(
            "a full-text scope of {} bytes is longer than 255",
            scope.len()
        ))
    })?;
    let mut key = Vec::with_capacity(1 + scope.len() + 80);
    key.push(len);
    key.extend_from_slice(scope);
    Ok(key)
}

fn posting_key(scope: &[u8], term: &str, doc: u64) -> Result<Vec<u8>, StorageError> {
    let mut key = scope_prefix(scope)?;
    key.extend_from_slice(term.as_bytes());
    key.push(0);
    key.extend_from_slice(&doc.to_be_bytes());
    Ok(key)
}

fn doc_key(scope: &[u8], doc: u64, chunk: u16) -> Result<Vec<u8>, StorageError> {
    let mut key = scope_prefix(scope)?;
    key.extend_from_slice(&doc.to_be_bytes());
    key.extend_from_slice(&chunk.to_be_bytes());
    Ok(key)
}

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("full-text index: {what}"))
}

fn u32_at(bytes: &[u8], at: usize) -> Result<u32, StorageError> {
    bytes
        .get(at..at + 4)
        .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")))
        .ok_or_else(|| corrupt("a row is cut short"))
}

fn u64_at(bytes: &[u8], at: usize) -> Result<u64, StorageError> {
    bytes
        .get(at..at + 8)
        .map(|b| u64::from_le_bytes(b.try_into().expect("8 bytes")))
        .ok_or_else(|| corrupt("a row is cut short"))
}

/// The terms of a document row chunk: (term, frequency) entries, each a
/// length byte, the term and the frequency (u32 LE).
fn decode_terms(mut bytes: &[u8]) -> Result<Vec<(String, u32)>, StorageError> {
    let mut out = Vec::new();
    while let Some((&len, rest)) = bytes.split_first() {
        let len = usize::from(len);
        let term = rest
            .get(..len)
            .and_then(|term| std::str::from_utf8(term).ok())
            .ok_or_else(|| corrupt("a document row holds a bad term"))?;
        out.push((term.to_string(), u32_at(rest, len)?));
        bytes = &rest[len + 4..];
    }
    Ok(out)
}

impl Index {
    /// The scope's statistics (zero when it holds no documents).
    pub fn stats(&self, ctx: &mut RelCtx<'_>, scope: &[u8]) -> Result<Stats, StorageError> {
        Ok(match self.docs.get(ctx, &scope_prefix(scope)?)? {
            Some(row) => Stats {
                docs: u64_at(&row, 0)?,
                total_len: u64_at(&row, 8)?,
            },
            None => Stats::default(),
        })
    }

    fn add_stats(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        scope: &[u8],
        docs: i64,
        len: i64,
    ) -> Result<(), StorageError> {
        let key = scope_prefix(scope)?;
        let old = self.stats(ctx, scope)?;
        let new = Stats {
            docs: old.docs.wrapping_add_signed(docs),
            total_len: old.total_len.wrapping_add_signed(len),
        };
        let mut row = new.docs.to_le_bytes().to_vec();
        row.extend_from_slice(&new.total_len.to_le_bytes());
        let mode = &mut OpMode::Txn(txn);
        if new == Stats::default() {
            self.docs.delete(ctx, mode, &key)?;
        } else if old == Stats::default() {
            self.docs.insert_unique(ctx, mode, &key, &row)?;
        } else {
            self.docs.update(ctx, mode, &key, &row)?;
        }
        Ok(())
    }

    /// Indexes `doc` in `scope` with `terms`, replacing what was indexed for
    /// it before. Runs inside the caller's transaction.
    pub fn index_document(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        scope: &[u8],
        doc: u64,
        terms: &DocTerms,
    ) -> Result<(), StorageError> {
        // A term is stored behind a length byte and ends at a 0 in posting keys.
        if let Some(bad) = terms
            .counts
            .keys()
            .find(|term| term.is_empty() || term.len() > MAX_TERM_BYTES || term.contains('\0'))
        {
            return Err(StorageError::InvalidConfig(format!(
                "{bad:?} cannot be indexed as a full-text term"
            )));
        }
        self.unindex_document(ctx, txn, scope, doc)?;
        let posting = |tf: u32| {
            let mut row = tf.to_le_bytes().to_vec();
            row.extend_from_slice(&terms.len.to_le_bytes());
            row
        };
        // Document rows: chunk 0 opens with the length; each chunk is filled
        // up to the cell cap.
        let cap = TREE_MAX_CELL - 2 - doc_key(scope, doc, 0)?.len();
        let mut chunks = vec![terms.len.to_le_bytes().to_vec()];
        for (term, &tf) in &terms.counts {
            let inserted = self.postings.insert_unique(
                ctx,
                &mut OpMode::Txn(txn),
                &posting_key(scope, term, doc)?,
                &posting(tf),
            )?;
            if inserted != TreeInsert::Inserted {
                return Err(corrupt("a posting outlived its document"));
            }
            let entry_len = 1 + term.len() + 4;
            if chunks.last().expect("a chunk").len() + entry_len > cap {
                chunks.push(Vec::new());
            }
            let chunk = chunks.last_mut().expect("a chunk");
            chunk.push(term.len() as u8);
            chunk.extend_from_slice(term.as_bytes());
            chunk.extend_from_slice(&tf.to_le_bytes());
        }
        for (index, chunk) in chunks.iter().enumerate() {
            let index = u16::try_from(index).map_err(|_| {
                StorageError::InvalidConfig("a document has too many distinct terms".to_string())
            })?;
            self.docs.insert_unique(
                ctx,
                &mut OpMode::Txn(txn),
                &doc_key(scope, doc, index)?,
                chunk,
            )?;
        }
        self.add_stats(ctx, txn, scope, 1, i64::from(terms.len))
    }

    /// Removes `doc` from `scope`. Returns whether it was indexed. Runs inside
    /// the caller's transaction.
    pub fn unindex_document(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        scope: &[u8],
        doc: u64,
    ) -> Result<bool, StorageError> {
        let lower = doc_key(scope, doc, 0)?;
        let upper = doc_key(scope, doc, u16::MAX)?;
        let rows = self.docs.scan_range(ctx, Some(&lower), Some(&upper))?;
        let Some((_, first)) = rows.first() else {
            return Ok(false);
        };
        let len = u32_at(first, 0)?;
        for (index, (key, row)) in rows.iter().enumerate() {
            let entries = if index == 0 { &row[4..] } else { &row[..] };
            for (term, _) in decode_terms(entries)? {
                if self
                    .postings
                    .delete(ctx, &mut OpMode::Txn(txn), &posting_key(scope, &term, doc)?)?
                    .is_none()
                {
                    return Err(corrupt("a document's posting is missing"));
                }
            }
            self.docs.delete(ctx, &mut OpMode::Txn(txn), key)?;
        }
        self.add_stats(ctx, txn, scope, -1, -i64::from(len))?;
        Ok(true)
    }

    /// Reads up to `limit` postings of `term` in `scope` (of every term that
    /// starts with it, when `prefix`), in (term, doc) order, after the
    /// posting `after` names (from the start when `None`). Returns them with
    /// their terms, and where to resume: `None` once nothing is left.
    #[allow(clippy::type_complexity)]
    pub fn read_postings(
        &self,
        ctx: &mut RelCtx<'_>,
        scope: &[u8],
        term: &str,
        prefix: bool,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<(String, Posting)>, Option<Vec<u8>>), StorageError> {
        let mut base = scope_prefix(scope)?;
        let skip = base.len();
        base.extend_from_slice(term.as_bytes());
        let mut upper = base.clone();
        if !prefix {
            upper.push(0);
        }
        // No term holds 0xFF (they are UTF-8), and no doc id sorts past it.
        upper.extend_from_slice(&[0xFF; 9]);
        let lower = match after {
            // Just past the last key read.
            Some(after) => [after, &[0]].concat(),
            None => base,
        };
        let rows = self
            .postings
            .scan_range_limit(ctx, Some(&lower), Some(&upper), limit)?;
        let mut out = Vec::with_capacity(rows.len());
        for (key, row) in &rows {
            if key.len() < skip + 9 {
                return Err(corrupt("a posting key is cut short"));
            }
            let (term, doc) = key[skip..].split_at(key.len() - skip - 9);
            let doc = &doc[1..];
            let term = std::str::from_utf8(term).map_err(|_| corrupt("a posting's term"))?;
            out.push((
                term.to_string(),
                Posting {
                    doc: u64::from_be_bytes(doc.try_into().expect("8 bytes")),
                    tf: u32_at(row, 0)?,
                    doc_len: u32_at(row, 4)?,
                },
            ));
        }
        let resume = (rows.len() == limit)
            .then(|| rows.last().map(|(key, _)| key.clone()))
            .flatten();
        Ok((out, resume))
    }
}
