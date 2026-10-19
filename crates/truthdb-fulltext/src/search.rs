//! Answering a search condition: its postings are read in slices (so the
//! caller can drop the storage lock between them), then the matching
//! documents are ranked by BM25.
//!
//! The slices are read at different times, so a search sees each term as it
//! was when its slice was read, not one snapshot of the index.

use std::collections::{BTreeMap, HashMap};

use truthdb_core::relstore::ctx::RelCtx;
use truthdb_core::storage::StorageError;

use crate::index::{Index, Posting, Stats};
use crate::query::{Node, Query, QueryError};

/// The most terms one prefix may expand to.
pub const MAX_PREFIX_TERMS: usize = 1024;
/// The most postings one search reads.
pub const MAX_POSTINGS: usize = 1_000_000;

/// BM25's term-frequency saturation.
const K1: f64 = 1.2;
/// BM25's document-length normalization.
const B: f64 = 0.75;

#[derive(Debug)]
pub enum SearchError {
    Query(QueryError),
    /// A prefix matches more than [`MAX_PREFIX_TERMS`] terms.
    TooManyTerms(String),
    /// The condition matches more than [`MAX_POSTINGS`] postings.
    TooManyPostings,
    Storage(StorageError),
}

impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchError::Query(err) => err.fmt(f),
            SearchError::TooManyTerms(prefix) => write!(
                f,
                "The prefix '{prefix}*' matches more than {MAX_PREFIX_TERMS} terms."
            ),
            SearchError::TooManyPostings => write!(
                f,
                "The full-text search condition matches more than {MAX_POSTINGS} term occurrences."
            ),
            SearchError::Storage(err) => err.fmt(f),
        }
    }
}

impl From<StorageError> for SearchError {
    fn from(err: StorageError) -> Self {
        SearchError::Storage(err)
    }
}

/// A matching document and its score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Hit {
    pub doc: u64,
    pub score: f64,
}

/// A search in progress.
pub struct Search {
    query: Query,
    /// Per leaf of the query: each term it matched, with that term's postings.
    found: Vec<BTreeMap<String, Vec<Posting>>>,
    /// The leaf being read, and where in it to resume.
    leaf: usize,
    resume: Option<Vec<u8>>,
    read: usize,
}

impl Search {
    pub fn new(condition: &str) -> Result<Search, SearchError> {
        let query = Query::parse(condition).map_err(SearchError::Query)?;
        Ok(Search {
            found: vec![BTreeMap::new(); query.leaves.len()],
            query,
            leaf: 0,
            resume: None,
            read: 0,
        })
    }

    /// Reads up to `budget` more postings from `scope` of `index`. Returns
    /// whether every posting has been read.
    pub fn step(
        &mut self,
        ctx: &mut RelCtx<'_>,
        index: &Index,
        scope: &[u8],
        budget: usize,
    ) -> Result<bool, SearchError> {
        let mut budget = budget.max(1);
        while budget > 0 && self.leaf < self.query.leaves.len() {
            let leaf = &self.query.leaves[self.leaf];
            let (postings, resume) = index.read_postings(
                ctx,
                scope,
                &leaf.term,
                leaf.prefix,
                self.resume.as_deref(),
                budget,
            )?;
            budget -= postings.len();
            self.read += postings.len();
            if self.read > MAX_POSTINGS {
                return Err(SearchError::TooManyPostings);
            }
            let found = &mut self.found[self.leaf];
            for (term, posting) in postings {
                found.entry(term).or_default().push(posting);
            }
            if found.len() > MAX_PREFIX_TERMS {
                return Err(SearchError::TooManyTerms(leaf.term.clone()));
            }
            self.resume = resume;
            if self.resume.is_none() {
                self.leaf += 1;
            }
        }
        Ok(self.leaf == self.query.leaves.len())
    }

    /// The documents the condition matches, best first (ties by document),
    /// once [`Self::step`] has read everything; `stats` are the scope's.
    pub fn rank(&self, stats: Stats) -> Vec<Hit> {
        let mut hits: Vec<Hit> = self
            .scores(&self.query.root, stats)
            .into_iter()
            .map(|(doc, score)| Hit { doc, score })
            .collect();
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.doc.cmp(&b.doc)));
        hits
    }

    fn scores(&self, node: &Node, stats: Stats) -> HashMap<u64, f64> {
        match node {
            Node::Leaf(leaf) => {
                let mut scores = HashMap::new();
                for postings in self.found[*leaf].values() {
                    let idf = idf(stats.docs, postings.len() as u64);
                    for posting in postings {
                        *scores.entry(posting.doc).or_insert(0.0) += idf * weight(posting, stats);
                    }
                }
                scores
            }
            Node::And(nodes) => {
                let mut all = nodes.iter().map(|node| self.scores(node, stats));
                let mut scores = all.next().expect("AND has operands");
                for next in all {
                    scores.retain(|doc, _| next.contains_key(doc));
                    for (doc, score) in scores.iter_mut() {
                        *score += next[doc];
                    }
                }
                scores
            }
            Node::Or(nodes) => {
                let mut scores = HashMap::new();
                for next in nodes.iter().map(|node| self.scores(node, stats)) {
                    for (doc, score) in next {
                        *scores.entry(doc).or_insert(0.0) += score;
                    }
                }
                scores
            }
            Node::AndNot(keep, drop) => {
                let mut scores = self.scores(keep, stats);
                let drop = self.scores(drop, stats);
                scores.retain(|doc, _| !drop.contains_key(doc));
                scores
            }
        }
    }
}

/// BM25's inverse document frequency of a term in `df` of `docs` documents
/// (the count is raised to `df` when the statistics were read before
/// documents the postings already hold).
fn idf(docs: u64, df: u64) -> f64 {
    let docs = docs.max(df) as f64;
    let df = df as f64;
    (1.0 + (docs - df + 0.5) / (df + 0.5)).ln()
}

/// BM25's term-frequency weight of one posting.
fn weight(posting: &Posting, stats: Stats) -> f64 {
    let avg_len = if stats.docs == 0 || stats.total_len == 0 {
        1.0
    } else {
        stats.total_len as f64 / stats.docs as f64
    };
    let tf = f64::from(posting.tf);
    let len = f64::from(posting.doc_len);
    tf * (K1 + 1.0) / (tf + K1 * (1.0 - B + B * len / avg_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idf_falls_as_a_term_gets_common() {
        assert!(idf(100, 1) > idf(100, 10));
        assert!(idf(100, 10) > idf(100, 90));
        assert!(idf(100, 100) > 0.0);
        // Statistics read before the postings: no negative or undefined idf.
        assert!(idf(0, 5) > 0.0);
    }

    #[test]
    fn weight_saturates_with_frequency_and_falls_with_length() {
        let stats = Stats {
            docs: 10,
            total_len: 1000,
        };
        let w = |tf, doc_len| {
            weight(
                &Posting {
                    doc: 1,
                    tf,
                    doc_len,
                },
                stats,
            )
        };
        assert!(w(2, 100) > w(1, 100));
        assert!(w(2, 100) - w(1, 100) > w(20, 100) - w(19, 100));
        assert!(w(20, 100) < K1 + 1.0);
        assert!(w(1, 50) > w(1, 100));
        assert!(w(1, 100) > w(1, 400));
        // At the average length the weight is tf (k1 + 1) / (tf + k1).
        assert!((w(1, 100) - 1.0).abs() < 1e-12);
    }
}
