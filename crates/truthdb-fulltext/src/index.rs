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
//! - **Documents:** `scope, doc (8 bytes BE), chunk (2 bytes BE)`: chunks
//!   1 on hold the document's terms with their frequencies, chunked under
//!   the tree's cell cap (removing a document reads them back to find its
//!   postings); chunk 0, written last, marks the document complete and holds
//!   its length and chunk count. A large document is written and removed a
//!   transaction at a time, so a document may exist in part.
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

/// How far [`Index::add_terms`] has written a document: past the term
/// `after`, with `next_chunk` the document row to write next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    pub after: Option<String>,
    pub next_chunk: u16,
}

impl Default for Progress {
    fn default() -> Self {
        Progress {
            after: None,
            next_chunk: 1,
        }
    }
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

    /// Indexes `doc` in `scope` with `terms` in one go, replacing what was
    /// indexed for it before (a small document, or a test: a large one is
    /// written by [`Self::add_terms`] a transaction at a time).
    pub fn index_document(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        scope: &[u8],
        doc: u64,
        terms: &DocTerms,
    ) -> Result<(), StorageError> {
        self.remove_document(ctx, txn, scope, doc, u64::MAX)?;
        let mut progress = Progress::default();
        self.add_terms(ctx, txn, scope, doc, terms, &mut progress, u64::MAX)?;
        self.finish_document(ctx, txn, scope, doc, terms.len, progress.next_chunk)
    }

    /// Removes `doc` from `scope` in one go. Returns whether anything of it
    /// was indexed.
    pub fn unindex_document(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        scope: &[u8],
        doc: u64,
    ) -> Result<bool, StorageError> {
        let lower = doc_key(scope, doc, 0)?;
        let upper = doc_key(scope, doc, u16::MAX)?;
        let held = !self
            .docs
            .scan_range_limit(ctx, Some(&lower), Some(&upper), 1)?
            .is_empty();
        self.remove_document(ctx, txn, scope, doc, u64::MAX)?;
        Ok(held)
    }

    /// Writes `terms` of `doc` from `progress` on, a posting each and their
    /// list in document rows, until done or until this call has written more
    /// than `max_log_bytes` of log; returns whether every term is written.
    /// The document is not complete (counted or ranked as a document) until
    /// [`Self::finish_document`]; a document written in part is removed by
    /// [`Self::remove_document`]. Each call runs inside the caller's
    /// transaction: a document too large for one is written by several.
    #[allow(clippy::too_many_arguments)]
    pub fn add_terms(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        scope: &[u8],
        doc: u64,
        terms: &DocTerms,
        progress: &mut Progress,
        max_log_bytes: u64,
    ) -> Result<bool, StorageError> {
        use std::ops::Bound;
        // A term is stored behind a length byte and ends at a 0 in posting
        // keys: refuse the document before writing any of it.
        if let Some(bad) = terms
            .counts
            .keys()
            .find(|term| term.is_empty() || term.len() > MAX_TERM_BYTES || term.contains('\0'))
        {
            return Err(StorageError::InvalidConfig(format!(
                "{bad:?} cannot be indexed as a full-text term"
            )));
        }
        let start = ctx.io.wal.tail();
        let mut posting = terms.len.to_le_bytes().to_vec();
        posting.splice(0..0, [0u8; 4]);
        let cap = TREE_MAX_CELL - 2 - doc_key(scope, doc, 0)?.len();
        let lower = match &progress.after {
            Some(after) => Bound::Excluded(after.clone()),
            None => Bound::Unbounded,
        };
        let mut chunk = Vec::new();
        let mut done = true;
        for (index, (term, &tf)) in terms.counts.range((lower, Bound::Unbounded)).enumerate() {
            if index % 16 == 0 && ctx.io.wal.tail() - start > max_log_bytes {
                done = false;
                break;
            }
            let entry_len = 1 + term.len() + 4;
            if chunk.len() + entry_len > cap {
                self.write_chunk(ctx, txn, scope, doc, progress, &mut chunk)?;
            }
            posting[..4].copy_from_slice(&tf.to_le_bytes());
            let inserted = self.postings.insert_unique(
                ctx,
                &mut OpMode::Txn(txn),
                &posting_key(scope, term, doc)?,
                &posting,
            )?;
            if inserted != TreeInsert::Inserted {
                return Err(corrupt("a posting outlived its document"));
            }
            chunk.push(term.len() as u8);
            chunk.extend_from_slice(term.as_bytes());
            chunk.extend_from_slice(&tf.to_le_bytes());
            progress.after = Some(term.clone());
        }
        if !chunk.is_empty() {
            self.write_chunk(ctx, txn, scope, doc, progress, &mut chunk)?;
        }
        Ok(done)
    }

    /// Writes one document row of terms and empties `chunk`.
    fn write_chunk(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        scope: &[u8],
        doc: u64,
        progress: &mut Progress,
        chunk: &mut Vec<u8>,
    ) -> Result<(), StorageError> {
        if progress.next_chunk == u16::MAX {
            return Err(StorageError::InvalidConfig(
                "a document has too many distinct terms".to_string(),
            ));
        }
        self.docs.insert_unique(
            ctx,
            &mut OpMode::Txn(txn),
            &doc_key(scope, doc, progress.next_chunk)?,
            chunk,
        )?;
        progress.next_chunk += 1;
        chunk.clear();
        Ok(())
    }

    /// Completes `doc`, whose terms are all written: from now on it counts
    /// as a document of `scope`, `len` terms long, in `chunks` rows.
    pub fn finish_document(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        scope: &[u8],
        doc: u64,
        len: u32,
        chunks: u16,
    ) -> Result<(), StorageError> {
        let mut header = len.to_le_bytes().to_vec();
        header.extend_from_slice(&chunks.to_le_bytes());
        let inserted = self.docs.insert_unique(
            ctx,
            &mut OpMode::Txn(txn),
            &doc_key(scope, doc, 0)?,
            &header,
        )?;
        if inserted != TreeInsert::Inserted {
            return Err(corrupt("a document completed twice"));
        }
        self.add_stats(ctx, txn, scope, 1, i64::from(len))
    }

    /// Removes `doc` from `scope`, complete or written in part: first its
    /// completion (so it no longer counts), then its rows and postings until
    /// this call has written more than `max_log_bytes` of log. Returns
    /// whether nothing of it is left. Runs inside the caller's transaction.
    pub fn remove_document(
        &self,
        ctx: &mut RelCtx<'_>,
        txn: &mut TxnLink,
        scope: &[u8],
        doc: u64,
        max_log_bytes: u64,
    ) -> Result<bool, StorageError> {
        let start = ctx.io.wal.tail();
        let header_key = doc_key(scope, doc, 0)?;
        if let Some(header) = self.docs.delete(ctx, &mut OpMode::Txn(txn), &header_key)? {
            self.add_stats(ctx, txn, scope, -1, -i64::from(u32_at(&header, 0)?))?;
        }
        let rows = self.docs.scan_range(
            ctx,
            Some(&doc_key(scope, doc, 1)?),
            Some(&doc_key(scope, doc, u16::MAX)?),
        )?;
        for (key, row) in &rows {
            if ctx.io.wal.tail() - start > max_log_bytes {
                return Ok(false);
            }
            for (term, _) in decode_terms(row)? {
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
        Ok(true)
    }

    /// Up to `limit` of the documents indexed in `scope`, lowest first (to
    /// unindex a whole scope a batch at a time).
    pub fn documents(
        &self,
        ctx: &mut RelCtx<'_>,
        scope: &[u8],
        limit: usize,
    ) -> Result<Vec<u64>, StorageError> {
        let lower = doc_key(scope, 0, 0)?;
        let upper = doc_key(scope, u64::MAX, u16::MAX)?;
        let mut docs = Vec::new();
        let mut after = lower;
        while docs.len() < limit {
            let rows = self
                .docs
                .scan_range_limit(ctx, Some(&after), Some(&upper), 1)?;
            let Some((key, _)) = rows.first() else {
                break;
            };
            let at = key.len() - 10;
            let doc = u64::from_be_bytes(key[at..at + 8].try_into().expect("8 bytes"));
            docs.push(doc);
            // Past every chunk of this document.
            match doc.checked_add(1) {
                Some(next) => after = doc_key(scope, next, 0)?,
                None => break,
            }
        }
        Ok(docs)
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
