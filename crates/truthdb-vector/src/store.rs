//! Vector buckets, vector indexes and vectors in their trees.
//!
//! - **Buckets:** name -> creation time.
//! - **Indexes:** (bucket, 0, name) -> id, dimension, metric, creation time
//!   and the non-filterable metadata keys. Deleting an index removes this
//!   row at once (its name is free again) and queues its id for purging.
//! - **Vectors:** a key maps to a sequence number, fresh on every put; under
//!   it go the float32 values, the filterable metadata and the whole
//!   metadata (each in chunks under the tree's cell cap), and the key.

use serde_json::{Map, Value};
use truthdb_core::relstore::btree::{BTree, TreeInsert};
use truthdb_core::relstore::ctx::{OpMode, RelCtx};
use truthdb_core::storage::StorageError;

use crate::metadata::Metadata;
use crate::names::{
    MAX_DIMENSION, MAX_METADATA_KEY_NAME, MAX_NON_FILTERABLE_KEYS, valid_key, valid_name,
};
use crate::{
    BUCKETS_TREE, INDEXES_TREE, KEYS_TREE, NEXT_INDEX_KEY, NEXT_SEQ_KEY, PURGE_TREE, SEQ_TREE,
    VecFile, VecTxn,
};

/// Float32 values per data row.
const FLOATS_PER_ROW: usize = 448;
/// Metadata JSON bytes per row.
const JSON_PER_ROW: usize = 1536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    Euclidean,
    Cosine,
}

impl DistanceMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            DistanceMetric::Euclidean => "euclidean",
            DistanceMetric::Cosine => "cosine",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "euclidean" => Some(DistanceMetric::Euclidean),
            "cosine" => Some(DistanceMetric::Cosine),
            _ => None,
        }
    }
}

/// Why a vector operation was refused (the storage itself worked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorError {
    NoSuchBucket,
    NoSuchIndex,
    BucketExists,
    IndexExists,
    BucketNotEmpty,
    /// A request value is invalid; the message says which and why.
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketInfo {
    pub name: String,
    pub created_ms: u64,
}

/// What a new index is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewIndex {
    pub name: String,
    pub dimension: u16,
    pub metric: DistanceMetric,
    pub non_filterable: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    pub bucket: String,
    pub name: String,
    /// Unique for the life of the database: an index deleted and created
    /// again under its name has a new id.
    pub id: u64,
    pub dimension: u16,
    pub metric: DistanceMetric,
    pub non_filterable: Vec<String>,
    pub created_ms: u64,
}

/// A vector to put.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorInput {
    pub key: String,
    pub data: Vec<f32>,
    pub metadata: Option<Value>,
}

/// A vector read back.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorEntry {
    pub key: String,
    pub data: Option<Vec<f32>>,
    pub metadata: Option<Map<String, Value>>,
}

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("corrupt vector engine {what}"))
}

fn index_key(bucket: &str, name: &str) -> Vec<u8> {
    let mut out = bucket.as_bytes().to_vec();
    out.push(0);
    out.extend_from_slice(name.as_bytes());
    out
}

fn encode_index(index: &IndexInfo) -> Vec<u8> {
    let mut out = index.id.to_le_bytes().to_vec();
    out.extend_from_slice(&index.dimension.to_le_bytes());
    out.push(match index.metric {
        DistanceMetric::Euclidean => 0,
        DistanceMetric::Cosine => 1,
    });
    out.extend_from_slice(&index.created_ms.to_le_bytes());
    out.push(index.non_filterable.len() as u8);
    for key in &index.non_filterable {
        out.push(key.len() as u8);
        out.extend_from_slice(key.as_bytes());
    }
    out
}

fn decode_index(bucket: &str, name: &str, row: &[u8]) -> Result<IndexInfo, StorageError> {
    let bad = || corrupt("index row");
    if row.len() < 20 {
        return Err(bad());
    }
    let metric = match row[10] {
        0 => DistanceMetric::Euclidean,
        1 => DistanceMetric::Cosine,
        _ => return Err(bad()),
    };
    let mut non_filterable = Vec::new();
    let mut rest = &row[20..];
    for _ in 0..row[19] {
        let (&len, tail) = rest.split_first().ok_or_else(bad)?;
        let key = tail.get(..usize::from(len)).ok_or_else(bad)?;
        non_filterable.push(String::from_utf8(key.to_vec()).map_err(|_| bad())?);
        rest = &tail[usize::from(len)..];
    }
    Ok(IndexInfo {
        bucket: bucket.to_string(),
        name: name.to_string(),
        id: u64::from_le_bytes(row[..8].try_into().expect("8 bytes")),
        dimension: u16::from_le_bytes([row[8], row[9]]),
        metric,
        non_filterable,
        created_ms: u64::from_le_bytes(row[11..19].try_into().expect("8 bytes")),
    })
}

/// `index, key`.
fn vector_key(index: u64, key: &str) -> Vec<u8> {
    let mut out = index.to_be_bytes().to_vec();
    out.extend_from_slice(key.as_bytes());
    out
}

/// `index, sequence`.
fn seq_key(index: u64, seq: u64) -> Vec<u8> {
    let mut out = index.to_be_bytes().to_vec();
    out.extend_from_slice(&seq.to_be_bytes());
    out
}

/// `index, sequence, chunk`.
fn chunk_key(index: u64, seq: u64, chunk: u8) -> Vec<u8> {
    let mut out = seq_key(index, seq);
    out.push(chunk);
    out
}

/// Reads the chunks of `(index, seq)` in `tree`, concatenated.
fn read_chunks(
    ctx: &mut RelCtx<'_>,
    tree: &BTree,
    index: u64,
    seq: u64,
) -> Result<Vec<u8>, StorageError> {
    Ok(tree
        .scan_range(
            ctx,
            Some(&chunk_key(index, seq, 0)),
            Some(&chunk_key(index, seq, u8::MAX)),
        )?
        .into_iter()
        .flat_map(|(_, row)| row)
        .collect())
}

fn floats(bytes: &[u8]) -> Result<Vec<f32>, StorageError> {
    if !bytes.len().is_multiple_of(4) {
        return Err(corrupt("vector data"));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|b| f32::from_le_bytes(b.try_into().expect("4 bytes")))
        .collect())
}

fn json_object(bytes: &[u8]) -> Result<Map<String, Value>, StorageError> {
    match serde_json::from_slice(bytes) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(corrupt("vector metadata")),
    }
}

/// Checks a vector against its index: the key, the dimension, finite
/// values (and, for cosine, not all zero), the metadata.
fn check_vector(index: &IndexInfo, vector: &VectorInput) -> Result<Metadata, VectorError> {
    if !valid_key(&vector.key) {
        return Err(VectorError::Invalid(format!(
            "A vector key must be 1 to {} bytes.",
            crate::names::MAX_KEY_BYTES
        )));
    }
    check_values(index, &vector.data)?;
    Metadata::new(vector.metadata.as_ref(), &index.non_filterable)
        .map_err(|err| VectorError::Invalid(err.0))
}

/// Checks vector values against an index: its dimension, finite, and for
/// cosine not all zero.
pub(crate) fn check_values(index: &IndexInfo, data: &[f32]) -> Result<(), VectorError> {
    if data.len() != usize::from(index.dimension) {
        return Err(VectorError::Invalid(format!(
            "A vector has {} dimensions; the index has {}.",
            data.len(),
            index.dimension
        )));
    }
    if data.iter().any(|value| !value.is_finite()) {
        return Err(VectorError::Invalid(
            "Vector values must be finite.".to_string(),
        ));
    }
    if index.metric == DistanceMetric::Cosine && data.iter().all(|value| *value == 0.0) {
        return Err(VectorError::Invalid(
            "A cosine index cannot hold an all-zero vector.".to_string(),
        ));
    }
    Ok(())
}

impl VecTxn<'_, '_> {
    pub fn create_bucket(
        &mut self,
        name: &str,
        now_ms: u64,
    ) -> Result<Result<(), VectorError>, StorageError> {
        if !valid_name(name) {
            return Ok(Err(VectorError::Invalid(format!(
                "Invalid vector bucket name '{name}'."
            ))));
        }
        let inserted = self.trees.buckets().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            name.as_bytes(),
            &now_ms.to_le_bytes(),
        )?;
        Ok(match inserted {
            TreeInsert::Inserted => Ok(()),
            TreeInsert::DuplicateKey => Err(VectorError::BucketExists),
        })
    }

    /// Deletes an empty vector bucket (one with no indexes).
    pub fn delete_bucket(&mut self, name: &str) -> Result<Result<(), VectorError>, StorageError> {
        if self
            .trees
            .buckets()
            .get(self.ctx, name.as_bytes())?
            .is_none()
        {
            return Ok(Err(VectorError::NoSuchBucket));
        }
        let mut upper = name.as_bytes().to_vec();
        upper.push(1);
        if !self
            .trees
            .indexes()
            .scan_range_limit(self.ctx, Some(&index_key(name, "")), Some(&upper), 1)?
            .is_empty()
        {
            return Ok(Err(VectorError::BucketNotEmpty));
        }
        self.trees
            .buckets()
            .delete(self.ctx, &mut OpMode::Txn(self.txn), name.as_bytes())?;
        Ok(Ok(()))
    }

    pub fn create_index(
        &mut self,
        bucket: &str,
        new: &NewIndex,
        now_ms: u64,
    ) -> Result<Result<IndexInfo, VectorError>, StorageError> {
        if self
            .trees
            .buckets()
            .get(self.ctx, bucket.as_bytes())?
            .is_none()
        {
            return Ok(Err(VectorError::NoSuchBucket));
        }
        if !valid_name(&new.name) {
            return Ok(Err(VectorError::Invalid(format!(
                "Invalid vector index name '{}'.",
                new.name
            ))));
        }
        if !(1..=MAX_DIMENSION).contains(&new.dimension) {
            return Ok(Err(VectorError::Invalid(format!(
                "A dimension must be 1 to {MAX_DIMENSION}."
            ))));
        }
        if new.non_filterable.len() > MAX_NON_FILTERABLE_KEYS
            || new
                .non_filterable
                .iter()
                .any(|key| key.is_empty() || key.len() > MAX_METADATA_KEY_NAME)
        {
            return Ok(Err(VectorError::Invalid(format!(
                "An index may name at most {MAX_NON_FILTERABLE_KEYS} non-filterable metadata keys of 1 to {MAX_METADATA_KEY_NAME} bytes."
            ))));
        }
        let key = index_key(bucket, &new.name);
        if self.trees.indexes().get(self.ctx, &key)?.is_some() {
            return Ok(Err(VectorError::IndexExists));
        }
        let index = IndexInfo {
            bucket: bucket.to_string(),
            name: new.name.clone(),
            id: self.next_counter(NEXT_INDEX_KEY)?,
            dimension: new.dimension,
            metric: new.metric,
            non_filterable: new.non_filterable.clone(),
            created_ms: now_ms,
        };
        self.trees.indexes().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key,
            &encode_index(&index),
        )?;
        Ok(Ok(index))
    }

    /// Deletes an index: its name is free at once, and its vectors are
    /// removed in the background ([`Self::purge_step`]).
    pub fn delete_index(
        &mut self,
        bucket: &str,
        name: &str,
    ) -> Result<Result<(), VectorError>, StorageError> {
        let key = index_key(bucket, name);
        let Some(row) = self
            .trees
            .indexes()
            .delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?
        else {
            return Ok(Err(VectorError::NoSuchIndex));
        };
        let index = decode_index(bucket, name, &row)?;
        self.trees.purge().insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &index.id.to_be_bytes(),
            &[],
        )?;
        Ok(Ok(()))
    }

    /// Whether `index` is still the live index of its name.
    fn index_live(&mut self, index: &IndexInfo) -> Result<bool, StorageError> {
        Ok(self
            .trees
            .indexes()
            .get(self.ctx, &index_key(&index.bucket, &index.name))?
            .map(|row| decode_index(&index.bucket, &index.name, &row))
            .transpose()?
            .is_some_and(|live| live.id == index.id))
    }

    /// Removes the vector `key` of `index`, if it is there.
    fn remove_vector(&mut self, index: u64, key: &str) -> Result<bool, StorageError> {
        let Some(row) = self.trees.keys().delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &vector_key(index, key),
        )?
        else {
            return Ok(false);
        };
        let seq = u64::from_le_bytes(row.as_slice().try_into().map_err(|_| corrupt("key row"))?);
        for tree in [
            self.trees.data(),
            self.trees.filter(),
            self.trees.metadata(),
        ] {
            let rows = tree.scan_range(
                self.ctx,
                Some(&chunk_key(index, seq, 0)),
                Some(&chunk_key(index, seq, u8::MAX)),
            )?;
            for (row_key, _) in rows {
                tree.delete(self.ctx, &mut OpMode::Txn(self.txn), &row_key)?;
            }
        }
        self.trees
            .seq()
            .delete(self.ctx, &mut OpMode::Txn(self.txn), &seq_key(index, seq))?;
        Ok(true)
    }

    /// Writes `bytes` under `(index, seq)` in `tree`, `per_row` bytes a row.
    fn write_chunks(
        &mut self,
        tree: &BTree,
        index: u64,
        seq: u64,
        bytes: &[u8],
        per_row: usize,
    ) -> Result<(), StorageError> {
        for (chunk, row) in bytes.chunks(per_row).enumerate() {
            tree.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &chunk_key(index, seq, chunk as u8),
                row,
            )?;
        }
        Ok(())
    }

    /// Puts `vectors[from..]` into `index` (replacing vectors of the same
    /// key), until done or until this transaction has written its log
    /// budget; returns how far it got (the caller puts the rest in a new
    /// transaction). Every vector is checked before any is written.
    pub fn put_vectors(
        &mut self,
        index: &IndexInfo,
        vectors: &[VectorInput],
        from: usize,
    ) -> Result<Result<usize, VectorError>, StorageError> {
        let mut checked = Vec::with_capacity(vectors.len());
        for vector in vectors {
            match check_vector(index, vector) {
                Ok(metadata) => checked.push(metadata),
                Err(err) => return Ok(Err(err)),
            }
        }
        if !self.index_live(index)? {
            return Ok(Err(VectorError::NoSuchIndex));
        }
        let budget = self.log_budget();
        let start = self.log_tail();
        let mut at = from;
        while at < vectors.len() {
            if at > from && self.log_since(start) > budget {
                break;
            }
            let (vector, metadata) = (&vectors[at], &checked[at]);
            self.remove_vector(index.id, &vector.key)?;
            let seq = self.next_counter(NEXT_SEQ_KEY)?;
            self.trees.keys().insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &vector_key(index.id, &vector.key),
                &seq.to_le_bytes(),
            )?;
            let data: Vec<u8> = vector.data.iter().flat_map(|v| v.to_le_bytes()).collect();
            let trees = self.trees;
            self.write_chunks(&trees.data(), index.id, seq, &data, FLOATS_PER_ROW * 4)?;
            let filterable = Value::Object(metadata.filterable.clone()).to_string();
            self.write_chunks(
                &trees.filter(),
                index.id,
                seq,
                filterable.as_bytes(),
                JSON_PER_ROW,
            )?;
            let whole = Value::Object(metadata.whole.clone()).to_string();
            self.write_chunks(
                &trees.metadata(),
                index.id,
                seq,
                whole.as_bytes(),
                JSON_PER_ROW,
            )?;
            trees.seq().insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &seq_key(index.id, seq),
                vector.key.as_bytes(),
            )?;
            at += 1;
        }
        Ok(Ok(at))
    }

    /// Deletes the vectors `keys[from..]` of `index` (keys not there are
    /// fine), until done or until the log budget is spent; returns how far
    /// it got.
    pub fn delete_vectors(
        &mut self,
        index: &IndexInfo,
        keys: &[String],
        from: usize,
    ) -> Result<Result<usize, VectorError>, StorageError> {
        if !self.index_live(index)? {
            return Ok(Err(VectorError::NoSuchIndex));
        }
        let budget = self.log_budget();
        let start = self.log_tail();
        let mut at = from;
        while at < keys.len() {
            if at > from && self.log_since(start) > budget {
                break;
            }
            self.remove_vector(index.id, &keys[at])?;
            at += 1;
        }
        Ok(Ok(at))
    }

    /// Removes a deleted index's vectors within the log budget; returns
    /// whether no deleted index is left to purge.
    pub fn purge_step(&mut self) -> Result<bool, StorageError> {
        let Some((id_key, _)) = self
            .trees
            .purge()
            .scan_range_limit(self.ctx, None, None, 1)?
            .pop()
        else {
            return Ok(true);
        };
        let budget = self.log_budget();
        let start = self.log_tail();
        let mut upper = id_key.clone();
        upper.extend_from_slice(&[0xFF; 16]);
        let trees = self.trees;
        // Everything of the index starts with its id.
        for tree in [
            trees.keys(),
            trees.data(),
            trees.filter(),
            trees.metadata(),
            trees.seq(),
        ] {
            loop {
                let rows = tree.scan_range_limit(self.ctx, Some(&id_key), Some(&upper), 64)?;
                if rows.is_empty() {
                    break;
                }
                for (row_key, _) in &rows {
                    tree.delete(self.ctx, &mut OpMode::Txn(self.txn), row_key)?;
                }
                if self.log_since(start) > budget {
                    return Ok(false);
                }
            }
        }
        trees
            .purge()
            .delete(self.ctx, &mut OpMode::Txn(self.txn), &id_key)?;
        Ok(trees
            .purge()
            .scan_range_limit(self.ctx, None, None, 1)?
            .is_empty())
    }
}

impl VecFile<'_> {
    pub fn bucket(&mut self, name: &str) -> Result<Option<BucketInfo>, StorageError> {
        let Some(tree) = self.existing_tree(BUCKETS_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.page_ctx(), name.as_bytes())?
            .map(|row| {
                Ok(BucketInfo {
                    name: name.to_string(),
                    created_ms: u64::from_le_bytes(
                        row.as_slice()
                            .try_into()
                            .map_err(|_| corrupt("bucket row"))?,
                    ),
                })
            })
            .transpose()
    }

    /// Up to `limit` buckets after `after`, by name.
    pub fn list_buckets(
        &mut self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<BucketInfo>, StorageError> {
        let Some(tree) = self.existing_tree(BUCKETS_TREE)? else {
            return Ok(Vec::new());
        };
        let lower = after.map(|after| [after.as_bytes(), &[0]].concat());
        tree.scan_range_limit(&mut self.page_ctx(), lower.as_deref(), None, limit)?
            .into_iter()
            .map(|(name, row)| {
                Ok(BucketInfo {
                    name: String::from_utf8(name).map_err(|_| corrupt("bucket name"))?,
                    created_ms: u64::from_le_bytes(
                        row.as_slice()
                            .try_into()
                            .map_err(|_| corrupt("bucket row"))?,
                    ),
                })
            })
            .collect()
    }

    pub fn index(&mut self, bucket: &str, name: &str) -> Result<Option<IndexInfo>, StorageError> {
        let Some(tree) = self.existing_tree(INDEXES_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.page_ctx(), &index_key(bucket, name))?
            .map(|row| decode_index(bucket, name, &row))
            .transpose()
    }

    /// Up to `limit` indexes of `bucket` after `after`, by name.
    pub fn list_indexes(
        &mut self,
        bucket: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<IndexInfo>, StorageError> {
        let Some(tree) = self.existing_tree(INDEXES_TREE)? else {
            return Ok(Vec::new());
        };
        let mut lower = index_key(bucket, after.unwrap_or(""));
        if after.is_some() {
            lower.push(0);
        }
        let mut upper = bucket.as_bytes().to_vec();
        upper.push(1);
        let skip = bucket.len() + 1;
        tree.scan_range_limit(&mut self.page_ctx(), Some(&lower), Some(&upper), limit)?
            .into_iter()
            .map(|(key, row)| {
                let name = std::str::from_utf8(&key[skip..]).map_err(|_| corrupt("index name"))?;
                decode_index(bucket, name, &row)
            })
            .collect()
    }

    /// The vectors of `index` among `keys` (missing ones left out), with
    /// their values and metadata as asked.
    pub fn get_vectors(
        &mut self,
        index: &IndexInfo,
        keys: &[String],
        with_data: bool,
        with_metadata: bool,
    ) -> Result<Vec<VectorEntry>, StorageError> {
        let Some(key_tree) = self.existing_tree(KEYS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for key in keys {
            let Some(row) = key_tree.get(&mut self.page_ctx(), &vector_key(index.id, key))? else {
                continue;
            };
            let seq =
                u64::from_le_bytes(row.as_slice().try_into().map_err(|_| corrupt("key row"))?);
            out.push(self.entry(index.id, seq, key.clone(), with_data, with_metadata)?);
        }
        Ok(out)
    }

    /// The vector stored under `(index, seq)`, as asked.
    pub(crate) fn entry(
        &mut self,
        index: u64,
        seq: u64,
        key: String,
        with_data: bool,
        with_metadata: bool,
    ) -> Result<VectorEntry, StorageError> {
        let data = if with_data {
            let tree = self
                .existing_tree(crate::DATA_TREE)?
                .ok_or_else(|| corrupt("data tree"))?;
            Some(floats(&read_chunks(
                &mut self.page_ctx(),
                &tree,
                index,
                seq,
            )?)?)
        } else {
            None
        };
        let metadata = if with_metadata {
            let tree = self
                .existing_tree(crate::META_DATA_TREE)?
                .ok_or_else(|| corrupt("metadata tree"))?;
            Some(json_object(&read_chunks(
                &mut self.page_ctx(),
                &tree,
                index,
                seq,
            )?)?)
        } else {
            None
        };
        Ok(VectorEntry {
            key,
            data,
            metadata,
        })
    }

    /// Up to `limit` vectors of `index` after the key `after`, by key, with
    /// their values and metadata as asked.
    pub fn list_vectors(
        &mut self,
        index: &IndexInfo,
        after: Option<&str>,
        limit: usize,
        with_data: bool,
        with_metadata: bool,
    ) -> Result<Vec<VectorEntry>, StorageError> {
        let Some(tree) = self.existing_tree(KEYS_TREE)? else {
            return Ok(Vec::new());
        };
        let mut lower = vector_key(index.id, after.unwrap_or(""));
        if after.is_some() {
            lower.push(0);
        }
        let upper = vector_key(index.id + 1, "");
        let rows =
            tree.scan_range_limit(&mut self.page_ctx(), Some(&lower), Some(&upper), limit)?;
        let mut out = Vec::new();
        for (row_key, row) in rows {
            if row_key == upper {
                break;
            }
            let key = String::from_utf8(row_key[8..].to_vec()).map_err(|_| corrupt("key"))?;
            let seq =
                u64::from_le_bytes(row.as_slice().try_into().map_err(|_| corrupt("key row"))?);
            out.push(self.entry(index.id, seq, key, with_data, with_metadata)?);
        }
        Ok(out)
    }

    /// Whether a deleted index still has vectors to purge.
    pub fn purge_pending(&mut self) -> Result<bool, StorageError> {
        Ok(match self.existing_tree(PURGE_TREE)? {
            Some(tree) => !tree
                .scan_range_limit(&mut self.page_ctx(), None, None, 1)?
                .is_empty(),
            None => false,
        })
    }

    /// The key stored under `(index, seq)`, when it is still there.
    pub(crate) fn seq_key_name(
        &mut self,
        index: u64,
        seq: u64,
    ) -> Result<Option<String>, StorageError> {
        let Some(tree) = self.existing_tree(SEQ_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.page_ctx(), &seq_key(index, seq))?
            .map(|row| String::from_utf8(row).map_err(|_| corrupt("key")))
            .transpose()
    }
}
