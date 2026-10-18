//! Row and key encodings of the object engine's trees. Keys are big-endian so
//! they sort numerically; rows are fixed-layout little-endian.

use truthdb_core::storage::StorageError;

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("corrupt object engine {what}"))
}

pub(crate) fn u64_le(bytes: &[u8]) -> Result<u64, StorageError> {
    Ok(u64::from_le_bytes(
        bytes.try_into().map_err(|_| corrupt("counter"))?,
    ))
}

/// Whether a body is still being written or complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyState {
    Staging,
    Sealed,
}

/// A body: its length, segment count, reference count and MD5.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BodyRow {
    pub state: BodyState,
    pub refcount: u32,
    pub len: u64,
    pub segments: u64,
    pub touched_ms: u64,
    pub md5: [u8; 16],
}

impl BodyRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(45);
        out.push(match self.state {
            BodyState::Staging => 0,
            BodyState::Sealed => 1,
        });
        out.extend_from_slice(&self.refcount.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.segments.to_le_bytes());
        out.extend_from_slice(&self.touched_ms.to_le_bytes());
        out.extend_from_slice(&self.md5);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != 45 {
            return Err(corrupt("body row"));
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        Ok(BodyRow {
            state: match bytes[0] {
                0 => BodyState::Staging,
                1 => BodyState::Sealed,
                _ => return Err(corrupt("body state")),
            },
            refcount: u32::from_le_bytes(bytes[1..5].try_into().unwrap()),
            len: u64_at(5),
            segments: u64_at(13),
            touched_ms: u64_at(21),
            md5: bytes[29..45].try_into().unwrap(),
        })
    }
}

/// One segment of a body: its pages and the LSN of the record that wrote
/// them, which every page carries (a read that finds another LSN is reading
/// a reused page).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentRow {
    pub start_page: u64,
    pub pages: u32,
    pub len: u32,
    pub lsn: u64,
}

impl SegmentRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(24);
        out.extend_from_slice(&self.start_page.to_le_bytes());
        out.extend_from_slice(&self.pages.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.lsn.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != 24 {
            return Err(corrupt("segment row"));
        }
        Ok(SegmentRow {
            start_page: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            pages: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            len: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            lsn: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        })
    }
}

pub(crate) fn body_key(body: u64) -> [u8; 8] {
    body.to_be_bytes()
}

pub(crate) fn segment_key(body: u64, index: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&body.to_be_bytes());
    key[8..].copy_from_slice(&index.to_be_bytes());
    key
}

/// `(time, body)` keys of the garbage and staging trees: time-ordered.
pub(crate) fn timed_key(time_ms: u64, body: u64) -> [u8; 16] {
    segment_key(time_ms, body)
}

pub(crate) fn decode_timed_key(key: &[u8]) -> Result<(u64, u64), StorageError> {
    if key.len() != 16 {
        return Err(corrupt("timed key"));
    }
    Ok((
        u64::from_be_bytes(key[..8].try_into().unwrap()),
        u64::from_be_bytes(key[8..].try_into().unwrap()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_round_trip() {
        let body = BodyRow {
            state: BodyState::Sealed,
            refcount: 3,
            len: 1 << 40,
            segments: 7,
            touched_ms: 1_700_000_000_000,
            md5: [9; 16],
        };
        assert_eq!(BodyRow::decode(&body.encode()).unwrap(), body);
        let segment = SegmentRow {
            start_page: 1234,
            pages: 256,
            len: 1_040_384,
            lsn: 99,
        };
        assert_eq!(SegmentRow::decode(&segment.encode()).unwrap(), segment);
        assert_eq!(decode_timed_key(&timed_key(5, 6)).unwrap(), (5, 6));
        assert!(
            timed_key(1, u64::MAX) < timed_key(2, 0),
            "time orders first"
        );
    }
}
