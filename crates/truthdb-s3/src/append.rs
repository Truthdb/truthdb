//! Appends to a directory bucket's objects: a PutObject with
//! `x-amz-write-offset-bytes`, the object's current size, adds its body as
//! one more part of the object (an offset of 0 on a key with no object
//! creates it).

use hyper::HeaderMap;
use truthdb_core::storage::StorageError;
use truthdb_object::{ObjFile, ObjectInfo, PartInfo, StoredChecksum};

use crate::error::S3Error;
use crate::objects::header;

pub(crate) const OFFSET_HEADER: &str = "x-amz-write-offset-bytes";

/// The offset a PutObject appends at, if it is an append.
pub(crate) fn offset(headers: &HeaderMap) -> Result<Option<u64>, S3Error> {
    header(headers, OFFSET_HEADER)
        .map(|value| {
            value.trim().parse().map_err(|_| {
                S3Error::new(
                    400,
                    "InvalidArgument",
                    "x-amz-write-offset-bytes is a number of bytes",
                )
            })
        })
        .transpose()
}

pub(crate) fn invalid_offset() -> S3Error {
    S3Error::new(
        400,
        "InvalidWriteOffset",
        "The write offset value that you specified does not match the current object size.",
    )
}

/// Whether a request carries user metadata.
pub(crate) fn with_metadata(headers: &HeaderMap) -> bool {
    headers
        .keys()
        .any(|name| name.as_str().starts_with("x-amz-meta-"))
}

/// S3's refusal of user metadata on an append to an object that exists.
pub(crate) fn check_metadata(with_metadata: bool, exists: bool) -> Result<(), S3Error> {
    if with_metadata && exists {
        return Err(S3Error::invalid_request(
            "Cannot specify both a write offset value and user-defined object metadata for existing objects",
        ));
    }
    Ok(())
}

/// The appended part's checksum must be of the object's algorithm (the
/// body was read computing it; the object may have been replaced since).
pub(crate) fn check_algorithm(
    current: &ObjectInfo,
    part: Option<&StoredChecksum>,
) -> Result<(), S3Error> {
    match (&current.attrs.checksum, part) {
        (Some(kept), Some(part)) if kept.algorithm != part.algorithm => {
            Err(S3Error::invalid_request(format!(
                "Checksum Type mismatch occurred, expected checksum Type: {}, actual checksum Type: {}",
                kept.algorithm.to_ascii_lowercase(),
                part.algorithm.to_ascii_lowercase()
            )))
        }
        _ => Ok(()),
    }
}

/// The object's checksum once `part` (the appended body's, `len` bytes) is
/// added to `current`'s: the whole object's CRC for a full-object CRC,
/// otherwise composite of its parts'. None when the object has none, or a
/// part of it has none.
pub(crate) fn appended_checksum(
    file: &mut ObjFile<'_>,
    (bucket, key): (&str, &str),
    current: &ObjectInfo,
    part: &StoredChecksum,
    len: u64,
) -> Result<Option<StoredChecksum>, StorageError> {
    // `part` is of the object's algorithm (`check_algorithm`).
    let Some(kept) = &current.attrs.checksum else {
        return Ok(None);
    };
    let as_part = |number: u32, size: u64, checksum: StoredChecksum| PartInfo {
        number,
        body: 0,
        size,
        md5: [0; 16],
        mtime_ms: 0,
        checksum: Some(checksum),
    };
    let mut parts = Vec::new();
    if current.parts == 0 {
        parts.push(as_part(1, current.size, kept.clone()));
    } else {
        let sizes = crate::objects::part_sizes(file, current)?;
        for (index, size) in sizes.into_iter().enumerate() {
            let place = index as u16 + 1;
            let Some(checksum) = file.part_checksum(bucket, key, current.version, place)? else {
                return Ok(None);
            };
            parts.push(as_part(u32::from(place), size, checksum));
        }
    }
    parts.push(as_part(parts.len() as u32 + 1, len, part.clone()));
    let crc = matches!(part.algorithm.as_str(), "CRC32" | "CRC32C" | "CRC64NVME");
    let kind = StoredChecksum {
        algorithm: part.algorithm.clone(),
        full_object: kept.full_object && crc,
        value: String::new(),
    };
    Ok(crate::part_checksums::of_object(Some(&kind), &parts))
}
