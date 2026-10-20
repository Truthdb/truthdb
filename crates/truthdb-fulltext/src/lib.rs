//! Full-text search over B+ trees on the shared log: a Unicode tokenizer,
//! `CONTAINS`-style search conditions, an inverted index any engine can keep
//! in two of its trees, and BM25 ranking.
//!
//! The index is written inside the caller's transactions, so it commits,
//! rolls back and recovers with the data it indexes.

mod index;
mod query;
mod search;
mod tokenize;

#[cfg(test)]
mod tests;

pub use index::{Index, Posting, Progress, Stats};
pub use query::{Leaf, MAX_NESTING, MAX_TERMS, MAX_TOKENS, Query, QueryError};
pub use search::{Hit, MAX_POSTINGS, MAX_PREFIX_TERMS, Search, SearchError};
pub use tokenize::{DocTerms, MAX_TERM_BYTES, terms};
