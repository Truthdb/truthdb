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

/// Whether a body is still being written, complete, or complete and made of
/// other bodies (a multipart upload's parts, in order).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyState {
    Staging,
    Sealed,
    Composite,
}

impl BodyState {
    /// Complete: readable and reference-counted.
    pub fn is_sealed(self) -> bool {
        self != BodyState::Staging
    }
}

/// A body: its length, segment count, reference count and MD5, and the zstd
/// level its segments were packed at (0: as written).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BodyRow {
    pub state: BodyState,
    pub refcount: u32,
    pub len: u64,
    pub segments: u64,
    pub touched_ms: u64,
    pub md5: [u8; 16],
    pub pack_level: u8,
}

impl BodyRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(45);
        out.push(match self.state {
            BodyState::Staging => 0,
            BodyState::Sealed => 1,
            BodyState::Composite => 2,
        });
        out.extend_from_slice(&self.refcount.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.segments.to_le_bytes());
        out.extend_from_slice(&self.touched_ms.to_le_bytes());
        out.extend_from_slice(&self.md5);
        // Only a packed body has the level: the others keep the layout from
        // before packing.
        if self.pack_level != 0 {
            out.push(self.pack_level);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != 45 && bytes.len() != 46 {
            return Err(corrupt("body row"));
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        Ok(BodyRow {
            state: match bytes[0] {
                0 => BodyState::Staging,
                1 => BodyState::Sealed,
                2 => BodyState::Composite,
                _ => return Err(corrupt("body state")),
            },
            refcount: u32::from_le_bytes(bytes[1..5].try_into().unwrap()),
            len: u64_at(5),
            segments: u64_at(13),
            touched_ms: u64_at(21),
            md5: bytes[29..45].try_into().unwrap(),
            pack_level: bytes.get(45).copied().unwrap_or(0),
        })
    }
}

/// One segment of a body: its pages, the bytes they hold (`len`: the
/// segment's bytes, or its zstd frame when `zstd`), and the LSN of the
/// record that wrote them, which every page carries (a read that finds
/// another LSN is reading a reused page).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentRow {
    pub start_page: u64,
    pub pages: u32,
    pub len: u32,
    pub lsn: u64,
    pub zstd: bool,
}

impl SegmentRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(24);
        out.extend_from_slice(&self.start_page.to_le_bytes());
        out.extend_from_slice(&self.pages.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.lsn.to_le_bytes());
        // A compressed segment says so in a byte of its own; a plain one
        // keeps the layout from before compression.
        if self.zstd {
            out.push(SEGMENT_ZSTD);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let zstd = match bytes.len() {
            24 => false,
            25 if bytes[24] == SEGMENT_ZSTD => true,
            _ => return Err(corrupt("segment row")),
        };
        Ok(SegmentRow {
            start_page: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            pages: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            len: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            lsn: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            zstd,
        })
    }
}

/// A segment row's format byte: a zstd frame.
const SEGMENT_ZSTD: u8 = 1;

/// One child of a composite body: the child body and where its bytes start
/// in the composite. Stored in the segments tree under the composite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChildRow {
    pub child: u64,
    pub offset: u64,
    pub len: u64,
}

impl ChildRow {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(24);
        out.extend_from_slice(&self.child.to_le_bytes());
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() != 24 {
            return Err(corrupt("child row"));
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        Ok(ChildRow {
            child: u64_at(0),
            offset: u64_at(8),
            len: u64_at(16),
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

/// The stored form of a record with fields of varying length: each value
/// in order, numbers little-endian, strings `[len u32][bytes]`, options a
/// presence byte, lists `[count u32]` then their items.
#[derive(Default)]
pub(crate) struct Writer(pub(crate) Vec<u8>);

impl Writer {
    pub(crate) fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    pub(crate) fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    pub(crate) fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    pub(crate) fn str(&mut self, value: &str) {
        self.u32(value.len() as u32);
        self.0.extend_from_slice(value.as_bytes());
    }
    pub(crate) fn opt_u32(&mut self, value: Option<u32>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u32(value);
            }
            None => self.u8(0),
        }
    }
    pub(crate) fn opt_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
            None => self.u8(0),
        }
    }
}

/// Reads what a [`Writer`] wrote; anything short or out of place is a
/// corrupt `what`.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    what: &'static str,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8], what: &'static str) -> Self {
        Reader { bytes, what }
    }
    pub(crate) fn corrupt(&self) -> StorageError {
        corrupt(self.what)
    }
    /// What is left to read.
    pub(crate) fn rest(&self) -> &'a [u8] {
        self.bytes
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], StorageError> {
        if self.bytes.len() < len {
            return Err(self.corrupt());
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }
    pub(crate) fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn str(&mut self) -> Result<String, StorageError> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| self.corrupt())
    }
    pub(crate) fn present(&mut self) -> Result<bool, StorageError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(self.corrupt()),
        }
    }
    pub(crate) fn opt_u32(&mut self) -> Result<Option<u32>, StorageError> {
        Ok(if self.present()? {
            Some(self.u32()?)
        } else {
            None
        })
    }
    pub(crate) fn opt_u64(&mut self) -> Result<Option<u64>, StorageError> {
        Ok(if self.present()? {
            Some(self.u64()?)
        } else {
            None
        })
    }
    /// A list's length: every item takes at least a byte, so a count past
    /// the bytes left is corrupt, not a huge allocation.
    pub(crate) fn count(&mut self) -> Result<usize, StorageError> {
        let count = self.u32()? as usize;
        if count > self.bytes.len() {
            return Err(self.corrupt());
        }
        Ok(count)
    }
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
            pack_level: 0,
        };
        assert_eq!(body.encode().len(), 45, "the layout from before packing");
        assert_eq!(BodyRow::decode(&body.encode()).unwrap(), body);
        let packed = BodyRow {
            pack_level: 19,
            ..body
        };
        assert_eq!(BodyRow::decode(&packed.encode()).unwrap(), packed);
        let segment = SegmentRow {
            start_page: 1234,
            pages: 256,
            len: 1_040_384,
            lsn: 99,
            zstd: false,
        };
        assert_eq!(
            segment.encode().len(),
            24,
            "the layout from before compression"
        );
        assert_eq!(SegmentRow::decode(&segment.encode()).unwrap(), segment);
        let frame = SegmentRow {
            len: 1_234,
            zstd: true,
            ..segment
        };
        assert_eq!(SegmentRow::decode(&frame.encode()).unwrap(), frame);
        let mut bad = frame.encode();
        bad[24] = 7;
        assert!(SegmentRow::decode(&bad).is_err());
        assert_eq!(decode_timed_key(&timed_key(5, 6)).unwrap(), (5, 6));
        assert!(
            timed_key(1, u64::MAX) < timed_key(2, 0),
            "time orders first"
        );
    }
}
