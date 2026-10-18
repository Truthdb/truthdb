//! Listing a bucket's objects in key order (S3 ListObjects / ListObjectsV2):
//! each key's newest version, keys whose newest version is a delete marker
//! left out, keys under a common prefix folded into one entry when a
//! delimiter is given. ListObjectVersions lists every version instead, newest
//! first within a key, delete markers included.

use truthdb_core::storage::StorageError;

use crate::objects::{ObjectInfo, version_from_key};
use crate::{BUCKETS_TREE, OBJECTS_TREE, ObjFile, ObjectError};

/// Rows the scan reads per tree lookup (tiny under test, so resuming across
/// batches is exercised).
const SCAN_BATCH: usize = if cfg!(test) { 3 } else { 1024 };
/// Rows one call reads at most before it hands back a truncated page (deleted
/// keys and older versions are read but not listed, so a page can take any
/// number of rows to fill).
const ROW_BUDGET: usize = if cfg!(test) { 30 } else { 16 * 1024 };

/// A listed object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    pub key: String,
    pub info: ObjectInfo,
    /// The key's newest version.
    pub latest: bool,
}

/// One page of a listing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listing {
    pub objects: Vec<ListedObject>,
    pub common_prefixes: Vec<String>,
    /// More entries follow this page.
    pub truncated: bool,
    /// The last entry of this page: where the next one resumes (see
    /// [`ListAfter`]); `true` when it is a common prefix.
    pub last: Option<(String, bool)>,
    /// The last entry's version, when it is an object in a version listing.
    pub last_version: Option<u64>,
}

/// Where a listing resumes: strictly after a key (a client's start-after or
/// marker, or the last key of a page), past every key under a common prefix
/// (the last entry of a page), or, in a version listing, after one version of
/// a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListAfter<'a> {
    Key(&'a str),
    CommonPrefix(&'a str),
    Version(&'a str, u64),
}

/// One entry of a listing.
enum Entry {
    Object(Box<ListedObject>),
    Prefix(String),
}

/// The bytes just past every key that starts with `prefix` (no key byte is
/// 0xFF in UTF-8, so bumping the last byte cannot carry).
fn past_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    if let Some(last) = out.last_mut() {
        *last += 1;
    }
    out
}

impl ObjFile<'_> {
    /// Lists up to `max_keys` entries of `bucket` whose keys start with
    /// `prefix`, after `after` (a key or common prefix returned as `last`, or
    /// a client's start-after key).
    pub fn list_objects(
        &mut self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<ListAfter<'_>>,
        max_keys: usize,
    ) -> Result<Result<Listing, ObjectError>, StorageError> {
        self.list(bucket, prefix, delimiter, after, max_keys, false)
    }

    /// [`Self::list_objects`] over every version: up to `max_keys` versions
    /// and common prefixes, delete markers included.
    pub fn list_versions(
        &mut self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<ListAfter<'_>>,
        max_keys: usize,
    ) -> Result<Result<Listing, ObjectError>, StorageError> {
        self.list(bucket, prefix, delimiter, after, max_keys, true)
    }

    fn list(
        &mut self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        after: Option<ListAfter<'_>>,
        max_keys: usize,
        versions: bool,
    ) -> Result<Result<Listing, ObjectError>, StorageError> {
        let Some(buckets) = self.existing_tree(BUCKETS_TREE)? else {
            return Ok(Err(ObjectError::NoSuchBucket));
        };
        if buckets
            .get(&mut self.core.page_ctx(0), bucket.as_bytes())?
            .is_none()
        {
            return Ok(Err(ObjectError::NoSuchBucket));
        }
        let mut listing = Listing::default();
        let Some(objects) = self.existing_tree(OBJECTS_TREE)? else {
            return Ok(Ok(listing));
        };
        let delimiter = delimiter.filter(|d| !d.is_empty());
        let mut bucket_prefix = bucket.as_bytes().to_vec();
        bucket_prefix.push(0);
        let upper = past_prefix(&bucket_prefix);
        let full = |key: &str| {
            let mut out = bucket_prefix.clone();
            out.extend_from_slice(key.as_bytes());
            out
        };
        // Where the scan starts: the prefix, or just past `after` (past every
        // version of that key, or past every key under that common prefix).
        let mut lower = full(prefix);
        let resume = match after {
            Some(ListAfter::CommonPrefix(common)) => past_prefix(&full(common)),
            Some(ListAfter::Key(key)) => {
                let mut past_versions = full(key);
                past_versions.push(1);
                past_versions
            }
            Some(ListAfter::Version(key, version)) => {
                let mut past_version = full(key);
                past_version.push(0);
                past_version.extend_from_slice(&(!version).to_be_bytes());
                past_version.push(0);
                past_version
            }
            None => Vec::new(),
        };
        if resume > lower {
            lower = resume;
        }

        let mut entries = 0usize;
        let mut rows_read = 0usize;
        // The key of the row read last: the rows after it that share it are
        // its older versions.
        let mut last_key: Option<Vec<u8>> = match after {
            Some(ListAfter::Version(key, _)) => Some(key.as_bytes().to_vec()),
            _ => None,
        };
        // Where an object listing cut short by the row budget resumes: the
        // last key read, or the common prefix just folded. (A version listing
        // lists every row it reads, so max_keys bounds it.)
        let mut resume_at: Option<(String, bool)> = None;
        'scan: loop {
            if !versions
                && rows_read >= ROW_BUDGET
                && let Some(at) = resume_at.take()
            {
                listing.truncated = true;
                listing.last = Some(at);
                break;
            }
            let rows = objects.scan_range_limit(
                &mut self.core.page_ctx(0),
                Some(&lower),
                Some(&upper),
                SCAN_BATCH,
            )?;
            let exhausted = rows.len() < SCAN_BATCH;
            let mut next_lower = None;
            for (row_key, row) in rows {
                rows_read += 1;
                // The next batch resumes past the last row read, whatever it
                // was (an older version of a key can fill a whole batch).
                next_lower = Some(row_key.clone());
                // `bucket, 0, key, 0, version`
                let key_bytes = &row_key[bucket_prefix.len()..row_key.len() - 9];
                let latest = last_key.as_deref() != Some(key_bytes);
                if !latest && !versions {
                    continue; // an older version of the key just read
                }
                last_key = Some(key_bytes.to_vec());
                let key = std::str::from_utf8(key_bytes)
                    .map_err(|_| StorageError::InvalidFile("corrupt object key".to_string()))?;
                if !key.starts_with(prefix) {
                    break 'scan;
                }
                resume_at = Some((key.to_string(), false));
                // In an object listing the newest version decides: a key whose
                // newest version is a delete marker is left out, and makes no
                // common prefix.
                let info = crate::objects::decode_version(version_from_key(&row_key)?, &row)?;
                if info.delete_marker && !versions {
                    continue;
                }
                let entry = match delimiter
                    .and_then(|d| key[prefix.len()..].find(d).map(|at| (d, at)))
                {
                    Some((d, at)) => {
                        let common = &key[..prefix.len() + at + d.len()];
                        // A common prefix not after the marker was listed
                        // already (a page that ended on it hands it back as
                        // the next marker): skip its keys.
                        if let Some(ListAfter::Key(marker) | ListAfter::Version(marker, _)) = after
                            && common <= marker
                        {
                            lower = past_prefix(&full(common));
                            continue 'scan;
                        }
                        Entry::Prefix(common.to_string())
                    }
                    None => Entry::Object(Box::new(ListedObject {
                        key: key.to_string(),
                        info,
                        latest,
                    })),
                };
                if entries == max_keys {
                    listing.truncated = true;
                    break 'scan;
                }
                entries += 1;
                match entry {
                    Entry::Object(object) => {
                        listing.last = Some((object.key.clone(), false));
                        listing.last_version = versions.then_some(object.info.version);
                        listing.objects.push(*object);
                    }
                    Entry::Prefix(common) => {
                        // Skip every other key under it.
                        let skip = past_prefix(&full(&common));
                        listing.last = Some((common.clone(), true));
                        listing.last_version = None;
                        resume_at = Some((common.clone(), true));
                        listing.common_prefixes.push(common);
                        lower = skip;
                        continue 'scan;
                    }
                }
            }
            if exhausted {
                break;
            }
            // Resume past the last row read (older versions of the same key
            // are skipped by `last_key`).
            let Some(mut resume) = next_lower else { break };
            resume.push(0);
            lower = resume;
        }
        Ok(Ok(listing))
    }
}
