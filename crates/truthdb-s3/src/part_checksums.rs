//! A multipart upload's checksum: the algorithm and type its
//! CreateMultipartUpload asks for (`x-amz-checksum-algorithm`,
//! `x-amz-checksum-type`), its parts' checksums, and the object's, made of
//! them on completion. A full-object checksum (`FULL_OBJECT`, CRC32, CRC32C
//! and CRC64NVME only) is the CRC of the whole object, combined from its
//! parts' CRCs; a composite one (`COMPOSITE`, not CRC64NVME) is the checksum
//! of the parts' checksums, with the part count (`-N`).

use base64::Engine as _;
use hyper::HeaderMap;
use ring::digest;
use truthdb_object::{PartInfo, StoredChecksum};

use crate::crc_combine::{CRC32, CRC32C, CRC64NVME, Crc};
use crate::error::S3Error;

/// The algorithms S3 names, and those of them computed here.
const NAMED: [&str; 10] = [
    "CRC32",
    "CRC32C",
    "CRC64NVME",
    "SHA1",
    "SHA256",
    "SHA512",
    "MD5",
    "XXHASH64",
    "XXHASH3",
    "XXHASH128",
];
const COMPUTED: [&str; 5] = ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"];

fn crc(algorithm: &str) -> Option<Crc> {
    match algorithm {
        "CRC32" => Some(CRC32),
        "CRC32C" => Some(CRC32C),
        "CRC64NVME" => Some(CRC64NVME),
        _ => None,
    }
}

/// The checksum a CreateMultipartUpload asks for: its algorithm and whether
/// it is of the whole object (its value is made on completion). None when it
/// names none: the parts' CRC64NVME then make the object's, as S3 gives an
/// upload without one.
pub(crate) fn of_upload(headers: &HeaderMap) -> Result<Option<StoredChecksum>, S3Error> {
    let algorithm = asked(headers, "x-amz-checksum-algorithm")?;
    let kind = crate::objects::header(headers, "x-amz-checksum-type");
    let Some(algorithm) = algorithm else {
        return match kind {
            None => Ok(None),
            Some(_) => Err(S3Error::invalid_request(
                "x-amz-checksum-type needs an x-amz-checksum-algorithm",
            )),
        };
    };
    // A CRC64NVME is of the whole object; the others are composite unless
    // asked otherwise.
    let full_object = match kind.as_deref().map(str::trim) {
        None => algorithm == "CRC64NVME",
        Some("FULL_OBJECT") => true,
        Some("COMPOSITE") => false,
        Some(other) => {
            return Err(S3Error::invalid_request(format!(
                "The checksum type {other} is not valid"
            )));
        }
    };
    if full_object && crc(&algorithm).is_none() {
        return Err(S3Error::invalid_request(format!(
            "The FULL_OBJECT checksum type cannot be used with the {algorithm} checksum algorithm"
        )));
    }
    if !full_object && algorithm == "CRC64NVME" {
        return Err(S3Error::invalid_request(
            "The COMPOSITE checksum type cannot be used with the CRC64NVME checksum algorithm",
        ));
    }
    Ok(Some(StoredChecksum {
        algorithm,
        full_object,
        value: String::new(),
    }))
}

/// The algorithm `name` (`x-amz-checksum-algorithm`'s) asks for, if any: 400
/// for one S3 does not name, 501 for one it names that is not computed here.
pub(crate) fn asked(headers: &HeaderMap, name: &str) -> Result<Option<String>, S3Error> {
    crate::objects::header(headers, name)
        .map(|algorithm| computed(algorithm.trim()))
        .transpose()
}

/// Algorithm `name` (any case), as S3 names it: 400 for one S3 does not
/// name, 501 for one it names that is not computed here.
pub(crate) fn computed(name: &str) -> Result<String, S3Error> {
    let algorithm = name.to_ascii_uppercase();
    if !NAMED.contains(&algorithm.as_str()) {
        return Err(S3Error::invalid_request(format!(
            "Checksum algorithm provided is unsupported: {algorithm}"
        )));
    }
    if !COMPUTED.contains(&algorithm.as_str()) {
        return Err(S3Error::not_implemented(format!(
            "the checksum algorithm {algorithm} is not implemented"
        )));
    }
    Ok(algorithm)
}

/// The algorithm an upload's parts are checksummed with.
pub(crate) fn part_algorithm(upload: Option<&StoredChecksum>) -> &str {
    upload.map_or("CRC64NVME", |checksum| checksum.algorithm.as_str())
}

/// The object's checksum, made of its `parts`' as `upload` asks; none when
/// a part has none of that algorithm (a part from before this build, or one
/// of an upload that asked for none, sent with another algorithm's).
pub(crate) fn of_object(
    upload: Option<&StoredChecksum>,
    parts: &[PartInfo],
) -> Option<StoredChecksum> {
    let algorithm = part_algorithm(upload);
    let full_object = upload.is_none_or(|checksum| checksum.full_object);
    let mut raw = Vec::with_capacity(parts.len());
    for part in parts {
        let checksum = part.checksum.as_ref()?;
        if checksum.algorithm != algorithm {
            return None;
        }
        raw.push(
            base64::engine::general_purpose::STANDARD
                .decode(&checksum.value)
                .ok()?,
        );
    }
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    let value = if full_object {
        let crc = crc(algorithm)?;
        let width = crc.width / 8;
        let mut whole: Option<u64> = None;
        for (bytes, part) in raw.iter().zip(parts) {
            if bytes.len() != width {
                return None;
            }
            let value = bytes
                .iter()
                .fold(0u64, |acc, byte| acc << 8 | u64::from(*byte));
            whole = Some(match whole {
                None => value,
                Some(before) => crc.combine(before, value, part.size),
            });
        }
        b64(&whole?.to_be_bytes()[8 - width..])
    } else {
        let joined = raw.concat();
        let digest = match algorithm {
            "CRC32" => crc32fast::hash(&joined).to_be_bytes().to_vec(),
            "CRC32C" => crc::Crc::<u32>::new(&crc::CRC_32_ISCSI)
                .checksum(&joined)
                .to_be_bytes()
                .to_vec(),
            "SHA1" => digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, &joined)
                .as_ref()
                .to_vec(),
            "SHA256" => digest::digest(&digest::SHA256, &joined).as_ref().to_vec(),
            _ => return None,
        };
        format!("{}-{}", b64(&digest), parts.len())
    };
    Some(StoredChecksum {
        algorithm: algorithm.to_string(),
        full_object,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(number: u32, data: &[u8], algorithm: &str) -> PartInfo {
        let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
        let value = match algorithm {
            "CRC32" => b64(&crc32fast::hash(data).to_be_bytes()),
            "CRC64NVME" => b64(&crc::Crc::<u64>::new(&crc::CRC_64_NVME)
                .checksum(data)
                .to_be_bytes()),
            "SHA256" => b64(digest::digest(&digest::SHA256, data).as_ref()),
            _ => unreachable!("a test algorithm"),
        };
        PartInfo {
            number,
            body: 0,
            size: data.len() as u64,
            md5: [0; 16],
            mtime_ms: 0,
            checksum: Some(StoredChecksum {
                algorithm: algorithm.to_string(),
                full_object: true,
                value,
            }),
        }
    }

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(*name, value.parse().unwrap());
        }
        headers
    }

    #[test]
    fn an_object_checksum_is_made_of_its_parts_as_s3_makes_it() {
        let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
        let (a, b) = (vec![1u8; 6000], vec![2u8; 3000]);
        let whole = [a.clone(), b.clone()].concat();
        // Full object: the whole object's CRC; the default, CRC64NVME.
        let crc32 = of_object(
            of_upload(&headers(&[
                ("x-amz-checksum-algorithm", "CRC32"),
                ("x-amz-checksum-type", "FULL_OBJECT"),
            ]))
            .unwrap()
            .as_ref(),
            &[part(1, &a, "CRC32"), part(2, &b, "CRC32")],
        )
        .unwrap();
        assert_eq!(crc32.value, b64(&crc32fast::hash(&whole).to_be_bytes()));
        assert!(crc32.full_object);
        let default =
            of_object(None, &[part(1, &a, "CRC64NVME"), part(2, &b, "CRC64NVME")]).unwrap();
        assert_eq!(
            (default.algorithm.as_str(), default.value),
            (
                "CRC64NVME",
                b64(&crc::Crc::<u64>::new(&crc::CRC_64_NVME)
                    .checksum(&whole)
                    .to_be_bytes())
            )
        );
        // Composite: the checksum of the parts' checksums, with the count.
        let sha = of_upload(&headers(&[("x-amz-checksum-algorithm", "sha256")])).unwrap();
        let composite = of_object(
            sha.as_ref(),
            &[part(1, &a, "SHA256"), part(2, &b, "SHA256")],
        )
        .unwrap();
        let joined = [
            digest::digest(&digest::SHA256, &a).as_ref().to_vec(),
            digest::digest(&digest::SHA256, &b).as_ref().to_vec(),
        ]
        .concat();
        assert_eq!(
            composite.value,
            format!(
                "{}-2",
                b64(digest::digest(&digest::SHA256, &joined).as_ref())
            )
        );
        assert!(!composite.full_object);
        // A part without one, or of another algorithm: none.
        let mut copied = part(2, &b, "CRC64NVME");
        copied.checksum = None;
        assert_eq!(of_object(None, &[part(1, &a, "CRC64NVME"), copied]), None);
        assert_eq!(of_object(None, &[part(1, &a, "CRC32")]), None);
    }

    #[test]
    fn an_upload_asks_for_what_s3_allows() {
        let code =
            |pairs: &[(&'static str, &str)]| of_upload(&headers(pairs)).err().map(|err| err.status);
        assert_eq!(of_upload(&headers(&[])).unwrap(), None);
        let crc64 = of_upload(&headers(&[("x-amz-checksum-algorithm", "CRC64NVME")]))
            .unwrap()
            .unwrap();
        assert!(crc64.full_object, "CRC64NVME is of the whole object");
        assert!(
            !of_upload(&headers(&[("x-amz-checksum-algorithm", "CRC32")]))
                .unwrap()
                .unwrap()
                .full_object
        );
        assert_eq!(
            code(&[
                ("x-amz-checksum-algorithm", "SHA256"),
                ("x-amz-checksum-type", "FULL_OBJECT")
            ]),
            Some(400)
        );
        assert_eq!(
            code(&[
                ("x-amz-checksum-algorithm", "CRC64NVME"),
                ("x-amz-checksum-type", "COMPOSITE")
            ]),
            Some(400)
        );
        assert_eq!(code(&[("x-amz-checksum-type", "COMPOSITE")]), Some(400));
        assert_eq!(code(&[("x-amz-checksum-algorithm", "BOGUS")]), Some(400));
        assert_eq!(code(&[("x-amz-checksum-algorithm", "XXHASH3")]), Some(501));
        assert_eq!(
            code(&[
                ("x-amz-checksum-algorithm", "CRC32"),
                ("x-amz-checksum-type", "PARTLY")
            ]),
            Some(400)
        );
    }
}
