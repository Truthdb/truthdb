//! Exact nearest-neighbour search: an index's vectors are read in sequence
//! order, a slice per storage-lock hold, and the `top_k` nearest that pass
//! the filter are kept. Between slices other writers run, so a search reads
//! each vector as it was when its slice was read; a vector deleted or put
//! again since is left out of the results, so a query that runs alongside
//! writes to its index can return fewer than `top_k` vectors.

use std::collections::BinaryHeap;

use serde_json::{Map, Value};
use truthdb_core::storage::StorageError;

use crate::metadata::Filter;
use crate::store::{DistanceMetric, IndexInfo, VectorError, check_values};
use crate::{DATA_TREE, FILTER_TREE, VecFile};

/// The most results one query returns.
pub const MAX_TOP_K: usize = 10_000;
/// Float32 values per data row (as stored).
const FLOATS_PER_ROW: usize = 448;

#[derive(Debug)]
pub enum QueryError {
    Invalid(String),
    Storage(StorageError),
}

impl From<StorageError> for QueryError {
    fn from(err: StorageError) -> Self {
        QueryError::Storage(err)
    }
}

/// A result: a vector's key and distance, and its metadata when asked for.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub key: String,
    pub distance: f32,
    pub metadata: Option<Map<String, Value>>,
}

/// A candidate kept: the heap's greatest is the farthest.
#[derive(Debug, Clone, Copy)]
struct Candidate {
    /// In f64: no finite float32 vectors overflow or underflow it.
    distance: f64,
    seq: u64,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.seq.cmp(&other.seq))
    }
}

/// A query in progress.
pub struct Query {
    index: IndexInfo,
    vector: Vec<f32>,
    norm: f64,
    top_k: usize,
    filter: Option<Filter>,
    nearest: BinaryHeap<Candidate>,
    /// The next sequence number to read.
    next_seq: u64,
}

/// A vector's length, in f64: squaring a float32 overflows or underflows
/// float32 itself for values past about 1e19 or under 1e-23.
fn norm(values: &[f32]) -> f64 {
    values
        .iter()
        .map(|v| f64::from(*v) * f64::from(*v))
        .sum::<f64>()
        .sqrt()
}

impl Query {
    /// A query for the `top_k` vectors of `index` nearest to `vector` that
    /// pass `filter`.
    pub fn new(
        index: IndexInfo,
        vector: Vec<f32>,
        top_k: usize,
        filter: Option<&Value>,
    ) -> Result<Query, QueryError> {
        check_values(&index, &vector).map_err(|err| match err {
            VectorError::Invalid(message) => QueryError::Invalid(message),
            other => QueryError::Invalid(format!("{other:?}")),
        })?;
        if !(1..=MAX_TOP_K).contains(&top_k) {
            return Err(QueryError::Invalid(format!(
                "topK must be 1 to {MAX_TOP_K}."
            )));
        }
        let filter = filter
            .map(Filter::parse)
            .transpose()
            .map_err(|err| QueryError::Invalid(err.0))?;
        Ok(Query {
            norm: norm(&vector),
            index,
            vector,
            top_k,
            filter,
            nearest: BinaryHeap::new(),
            next_seq: 0,
        })
    }

    fn distance(&self, values: &[f32]) -> f64 {
        let pairs = self
            .vector
            .iter()
            .zip(values)
            .map(|(a, b)| (f64::from(*a), f64::from(*b)));
        match self.index.metric {
            DistanceMetric::Euclidean => pairs.map(|(a, b)| (a - b) * (a - b)).sum::<f64>().sqrt(),
            DistanceMetric::Cosine => {
                let dot: f64 = pairs.map(|(a, b)| a * b).sum();
                1.0 - dot / (self.norm * norm(values))
            }
        }
    }

    /// Reads up to `budget` more vectors (one storage-lock hold). Returns
    /// whether the index is read through.
    pub fn step(&mut self, file: &mut VecFile<'_>, budget: usize) -> Result<bool, QueryError> {
        let (Some(data), filter_tree) = (
            file.existing_tree(DATA_TREE)?,
            file.existing_tree(FILTER_TREE)?,
        ) else {
            return Ok(true);
        };
        let id = self.index.id;
        let chunks = usize::from(self.index.dimension).div_ceil(FLOATS_PER_ROW);
        let mut lower = id.to_be_bytes().to_vec();
        lower.extend_from_slice(&self.next_seq.to_be_bytes());
        let mut upper = id.to_be_bytes().to_vec();
        upper.extend_from_slice(&[0xFF; 9]);
        let rows = data.scan_range_limit(
            &mut file.page_ctx(),
            Some(&lower),
            Some(&upper),
            budget * chunks,
        )?;
        let finished = rows.len() < budget * chunks;
        // Rows come grouped by sequence number, a vector's chunks in order.
        let mut at = 0;
        while at < rows.len() {
            let seq = u64::from_be_bytes(rows[at].0[8..16].try_into().expect("8 bytes"));
            let mut values = Vec::with_capacity(usize::from(self.index.dimension));
            while at < rows.len() && rows[at].0[8..16] == seq.to_be_bytes() {
                values.extend(
                    rows[at]
                        .1
                        .chunks_exact(4)
                        .map(|b| f32::from_le_bytes(b.try_into().expect("4 bytes"))),
                );
                at += 1;
            }
            if values.len() < usize::from(self.index.dimension) {
                // Cut short by the slice: read it whole next time.
                if !finished {
                    self.next_seq = seq;
                    return Ok(false);
                }
                continue;
            }
            self.next_seq = seq + 1;
            if let (Some(filter), Some(tree)) = (&self.filter, &filter_tree) {
                let mut from = id.to_be_bytes().to_vec();
                from.extend_from_slice(&seq.to_be_bytes());
                let mut to = from.clone();
                from.push(0);
                to.push(u8::MAX);
                let json: Vec<u8> = tree
                    .scan_range(&mut file.page_ctx(), Some(&from), Some(&to))?
                    .into_iter()
                    .flat_map(|(_, row)| row)
                    .collect();
                let passes = match serde_json::from_slice(&json) {
                    Ok(Value::Object(map)) => filter.holds(&map),
                    _ => false,
                };
                if !passes {
                    continue;
                }
            }
            let candidate = Candidate {
                distance: self.distance(&values),
                seq,
            };
            if self.nearest.len() < self.top_k {
                self.nearest.push(candidate);
            } else if self.nearest.peek().is_some_and(|worst| candidate < *worst) {
                self.nearest.pop();
                self.nearest.push(candidate);
            }
        }
        Ok(finished)
    }

    /// The results, nearest first, once [`Self::step`] has read the index
    /// through: each named by its key, left out when the vector was deleted
    /// or put again since (either removes its sequence row).
    pub fn finish(
        self,
        file: &mut VecFile<'_>,
        with_metadata: bool,
    ) -> Result<Vec<Hit>, StorageError> {
        let mut hits = Vec::new();
        for candidate in self.nearest.into_sorted_vec() {
            let Some(key) = file.seq_key_name(self.index.id, candidate.seq)? else {
                continue;
            };
            let metadata = if with_metadata {
                file.entry(self.index.id, candidate.seq, key.clone(), false, true)?
                    .metadata
            } else {
                None
            };
            hits.push(Hit {
                key,
                // A euclidean distance past float32's range reads as infinite.
                distance: candidate.distance as f32,
                metadata,
            });
        }
        Ok(hits)
    }
}
