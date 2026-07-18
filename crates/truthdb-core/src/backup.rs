//! The `TDBBAK1` backup file format: a self-describing stream of typed,
//! xxh64-checksummed blocks.
//!
//! Layout (in order): a [`BlockType::Header`] carrying the parameters needed to
//! recreate the destination file and the recovery LSN bracket, one
//! [`BlockType::AllocMap`] listing every allocated page run (so restore lays the
//! allocation set back before recovery), a sequence of [`BlockType::PageData`]
//! runs of raw page images, one or more [`BlockType::LogChunk`] blocks carrying
//! the WAL bytes of `[redo_start_lsn, backup_end_lsn)` verbatim, and a
//! [`BlockType::Trailer`]. Every block is framed as `type(u32) | payload_len(u64)
//! | payload | xxh64(payload)(u64)`, so a torn or tampered block is detected on
//! read.
//!
//! `backup_end_lsn` is not stored in the header: it is only known after the
//! (fuzzy) page copy finishes, so it is derived on read as
//! `redo_start_lsn + total LogChunk bytes` — the log covers exactly that range.
//!
//! This module owns only the framing and the block codecs; `storage.rs`
//! assembles the actual page and log data.

use std::io::{self, Read, Write};

use xxhash_rust::xxh64::xxh64;

use crate::storage_layout::PAGE_SIZE;

/// The magic bytes at the start of every `TDBBAK1` file.
pub const MAGIC: &[u8; 8] = b"TDBBAK1\0";
/// Format version — bumped on any incompatible framing/header change.
pub const FORMAT_VERSION: u32 = 1;

/// The kind of a framed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum BlockType {
    /// The header (recreation parameters + redo-start LSN). Exactly one, first.
    Header = 1,
    /// The allocation map: the full set of allocated page runs.
    AllocMap = 2,
    /// A run of raw page images (a starting page number followed by page bytes).
    PageData = 3,
    /// A chunk of WAL bytes (a starting LSN followed by the raw entries).
    LogChunk = 4,
    /// The trailer (block count). Exactly one, last.
    Trailer = 5,
}

impl BlockType {
    fn from_u32(value: u32) -> Option<Self> {
        Some(match value {
            1 => BlockType::Header,
            2 => BlockType::AllocMap,
            3 => BlockType::PageData,
            4 => BlockType::LogChunk,
            5 => BlockType::Trailer,
            _ => return None,
        })
    }
}

/// Backup options that travel in the header (subset of `WITH ...`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackupFlags {
    /// `WITH CHECKSUM` (default on): every page was checksum-verified as copied.
    pub checksum: bool,
    /// `WITH COPY_ONLY`: the backup did not disturb the log-backup chain.
    pub copy_only: bool,
}

/// The `TDBBAK1` header: everything a restore needs to recreate the file and
/// seed recovery. Encoded little-endian with an explicit layout so the format
/// is stable across builds.
///
/// The layout sizes reproduce the destination file's regions byte-for-byte (the
/// offsets are fixed by the region order, so only the sizes travel). The roots
/// are the source's checkpoint-consistent superblock state as of
/// `redo_start_lsn`; recovery redoes `[redo_start_lsn, backup_end_lsn)` on top.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupHeader {
    pub format_version: u32,
    pub page_size: u32,
    /// Region sizes, in the file's region order; the offsets are derived.
    pub total_size: u64,
    pub wal_size: u64,
    pub data_size: u64,
    pub metadata_size: u64,
    pub allocator_size: u64,
    pub snapshot_size: u64,
    pub reserved_size: u64,
    pub default_collation: Option<String>,
    /// The recovery redo-start LSN (the source's `wal_head`); restore seeds the
    /// ring head here and redoes forward.
    pub redo_start_lsn: u64,
    /// The catalog B+tree root as an absolute file offset (0 = none). The
    /// restored superblock carries it so recovery starts from the right catalog.
    pub metadata_root: u64,
    /// The source's `last_committed_seq` at `redo_start_lsn`.
    pub last_committed_seq: u64,
    /// The source's database-options byte (RCSI / ALLOW_SNAPSHOT bits).
    pub db_options: u8,
    /// Wall-clock time the backup finished, milliseconds since the Unix epoch.
    pub finished_at_millis: u64,
    pub flags: BackupFlags,
}

impl BackupHeader {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.format_version.to_le_bytes());
        out.extend_from_slice(&self.page_size.to_le_bytes());
        out.extend_from_slice(&self.total_size.to_le_bytes());
        out.extend_from_slice(&self.wal_size.to_le_bytes());
        out.extend_from_slice(&self.data_size.to_le_bytes());
        out.extend_from_slice(&self.metadata_size.to_le_bytes());
        out.extend_from_slice(&self.allocator_size.to_le_bytes());
        out.extend_from_slice(&self.snapshot_size.to_le_bytes());
        out.extend_from_slice(&self.reserved_size.to_le_bytes());
        out.extend_from_slice(&self.redo_start_lsn.to_le_bytes());
        out.extend_from_slice(&self.metadata_root.to_le_bytes());
        out.extend_from_slice(&self.last_committed_seq.to_le_bytes());
        out.extend_from_slice(&self.finished_at_millis.to_le_bytes());
        out.push(self.db_options);
        let flag_bits = (self.flags.checksum as u8) | ((self.flags.copy_only as u8) << 1);
        out.push(flag_bits);
        // Default collation as a length-prefixed UTF-8 string (u32 len, or
        // u32::MAX for None).
        match &self.default_collation {
            Some(collation) => {
                out.extend_from_slice(&(collation.len() as u32).to_le_bytes());
                out.extend_from_slice(collation.as_bytes());
            }
            None => out.extend_from_slice(&u32::MAX.to_le_bytes()),
        }
        out
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor { bytes, pos: 0 };
        let format_version = cursor.u32()?;
        let page_size = cursor.u32()?;
        let total_size = cursor.u64()?;
        let wal_size = cursor.u64()?;
        let data_size = cursor.u64()?;
        let metadata_size = cursor.u64()?;
        let allocator_size = cursor.u64()?;
        let snapshot_size = cursor.u64()?;
        let reserved_size = cursor.u64()?;
        let redo_start_lsn = cursor.u64()?;
        let metadata_root = cursor.u64()?;
        let last_committed_seq = cursor.u64()?;
        let finished_at_millis = cursor.u64()?;
        let db_options = cursor.u8()?;
        let flag_bits = cursor.u8()?;
        let collation_len = cursor.u32()?;
        let default_collation = if collation_len == u32::MAX {
            None
        } else {
            let bytes = cursor.take(collation_len as usize)?;
            Some(
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| corrupt(&format!("collation not UTF-8: {e}")))?,
            )
        };
        Ok(BackupHeader {
            format_version,
            page_size,
            total_size,
            wal_size,
            data_size,
            metadata_size,
            allocator_size,
            snapshot_size,
            reserved_size,
            default_collation,
            redo_start_lsn,
            metadata_root,
            last_committed_seq,
            db_options,
            finished_at_millis,
            flags: BackupFlags {
                checksum: flag_bits & 1 != 0,
                copy_only: flag_bits & 2 != 0,
            },
        })
    }
}

/// What a completed [`crate::storage::Storage::backup_full`] reports back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupSummary {
    pub redo_start_lsn: u64,
    pub backup_end_lsn: u64,
    pub pages_copied: u64,
    pub log_bytes: u64,
    pub finished_at_millis: u64,
}

/// Encodes an allocation map: a `u64` run count, then `(start_page, count)`
/// pairs.
pub fn encode_alloc_map(runs: &[(u64, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + runs.len() * 16);
    out.extend_from_slice(&(runs.len() as u64).to_le_bytes());
    for (start, count) in runs {
        out.extend_from_slice(&start.to_le_bytes());
        out.extend_from_slice(&count.to_le_bytes());
    }
    out
}

/// Decodes an allocation map produced by [`encode_alloc_map`].
pub fn decode_alloc_map(payload: &[u8]) -> io::Result<Vec<(u64, u64)>> {
    let mut cursor = Cursor {
        bytes: payload,
        pos: 0,
    };
    let count = cursor.u64()? as usize;
    let mut runs = Vec::with_capacity(count);
    for _ in 0..count {
        let start = cursor.u64()?;
        let run = cursor.u64()?;
        runs.push((start, run));
    }
    Ok(runs)
}

/// Encodes a page-data run: the starting page number, then `page_bytes`
/// (a whole number of pages).
pub fn encode_page_run(start_page: u64, page_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + page_bytes.len());
    out.extend_from_slice(&start_page.to_le_bytes());
    out.extend_from_slice(page_bytes);
    out
}

/// Decodes a page-data run into `(start_page, page_count, page_bytes)`.
pub fn decode_page_run(payload: &[u8]) -> io::Result<(u64, u64, &[u8])> {
    if payload.len() < 8 {
        return Err(corrupt("page run block too short"));
    }
    let start_page = u64::from_le_bytes(payload[..8].try_into().unwrap());
    let bytes = &payload[8..];
    if !bytes.len().is_multiple_of(PAGE_SIZE) {
        return Err(corrupt("page run is not a whole number of pages"));
    }
    Ok((start_page, (bytes.len() / PAGE_SIZE) as u64, bytes))
}

/// Encodes a log chunk: the starting LSN, then the raw WAL bytes.
pub fn encode_log_chunk(start_lsn: u64, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + bytes.len());
    out.extend_from_slice(&start_lsn.to_le_bytes());
    out.extend_from_slice(bytes);
    out
}

/// Decodes a log chunk into `(start_lsn, raw_bytes)`.
pub fn decode_log_chunk(payload: &[u8]) -> io::Result<(u64, &[u8])> {
    if payload.len() < 8 {
        return Err(corrupt("log chunk block too short"));
    }
    let start_lsn = u64::from_le_bytes(payload[..8].try_into().unwrap());
    Ok((start_lsn, &payload[8..]))
}

/// Writes framed, checksummed `TDBBAK1` blocks to an underlying writer.
pub struct BackupWriter<W: Write> {
    writer: W,
    blocks: u64,
}

impl<W: Write> BackupWriter<W> {
    /// Starts a backup stream: writes the magic, then the header block.
    pub fn new(mut writer: W, header: &BackupHeader) -> io::Result<Self> {
        writer.write_all(MAGIC)?;
        let mut this = BackupWriter { writer, blocks: 0 };
        this.write_block(BlockType::Header, &header.encode())?;
        Ok(this)
    }

    /// Writes one framed block: `type | len | payload | xxh64(payload)`.
    pub fn write_block(&mut self, block_type: BlockType, payload: &[u8]) -> io::Result<()> {
        self.writer.write_all(&(block_type as u32).to_le_bytes())?;
        self.writer
            .write_all(&(payload.len() as u64).to_le_bytes())?;
        self.writer.write_all(payload)?;
        self.writer.write_all(&xxh64(payload, 0).to_le_bytes())?;
        self.blocks += 1;
        Ok(())
    }

    /// Writes the trailer (the block count for a completeness check) and returns
    /// the underlying writer.
    pub fn finish(mut self) -> io::Result<W> {
        // +1: the trailer counts itself, so a reader can verify it saw them all.
        let count = (self.blocks + 1).to_le_bytes();
        self.write_block(BlockType::Trailer, &count)?;
        self.writer.flush()?;
        Ok(self.writer)
    }
}

/// Reads and verifies framed `TDBBAK1` blocks.
pub struct BackupReader<R: Read> {
    reader: R,
    blocks: u64,
    done: bool,
}

impl<R: Read> BackupReader<R> {
    /// Opens a backup stream: checks the magic and reads the header block.
    pub fn new(mut reader: R) -> io::Result<(Self, BackupHeader)> {
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(corrupt("not a TDBBAK1 backup (bad magic)"));
        }
        let mut this = BackupReader {
            reader,
            blocks: 0,
            done: false,
        };
        let (block_type, payload) = this.read_block()?;
        if block_type != BlockType::Header {
            return Err(corrupt("first block is not the header"));
        }
        let header = BackupHeader::decode(&payload)?;
        if header.format_version != FORMAT_VERSION {
            return Err(corrupt(&format!(
                "unsupported backup format version {}",
                header.format_version
            )));
        }
        Ok((this, header))
    }

    /// Reads the next data block, or `None` at the trailer. The trailer's block
    /// count is verified against the number actually read.
    pub fn next_block(&mut self) -> io::Result<Option<(BlockType, Vec<u8>)>> {
        if self.done {
            return Ok(None);
        }
        let (block_type, payload) = self.read_block()?;
        if block_type == BlockType::Trailer {
            self.done = true;
            let expected = u64::from_le_bytes(
                payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| corrupt("malformed trailer"))?,
            );
            if expected != self.blocks {
                return Err(corrupt(&format!(
                    "backup is incomplete: trailer expected {expected} blocks, read {}",
                    self.blocks
                )));
            }
            return Ok(None);
        }
        Ok(Some((block_type, payload)))
    }

    fn read_block(&mut self) -> io::Result<(BlockType, Vec<u8>)> {
        let mut type_bytes = [0u8; 4];
        self.reader.read_exact(&mut type_bytes)?;
        let block_type = BlockType::from_u32(u32::from_le_bytes(type_bytes))
            .ok_or_else(|| corrupt("unknown block type"))?;
        let mut len_bytes = [0u8; 8];
        self.reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len];
        self.reader.read_exact(&mut payload)?;
        let mut checksum_bytes = [0u8; 8];
        self.reader.read_exact(&mut checksum_bytes)?;
        let stored = u64::from_le_bytes(checksum_bytes);
        if xxh64(&payload, 0) != stored {
            return Err(corrupt("block checksum mismatch (corrupt backup)"));
        }
        self.blocks += 1;
        Ok((block_type, payload))
    }
}

fn corrupt(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// A tiny little-endian byte cursor for header decoding.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn take(&mut self, n: usize) -> io::Result<&[u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.bytes.len())
            .ok_or_else(|| corrupt("truncated backup block"))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header() -> BackupHeader {
        BackupHeader {
            format_version: FORMAT_VERSION,
            page_size: 4096,
            total_size: 1 << 30,
            wal_size: 64 * 1024,
            data_size: 512 * 1024,
            metadata_size: 128 * 1024,
            allocator_size: 4096,
            snapshot_size: 8192,
            reserved_size: 16384,
            default_collation: Some("Finnish_Swedish_CI_AS".to_string()),
            redo_start_lsn: 12_345,
            metadata_root: 4096 * 40,
            last_committed_seq: 7,
            db_options: 0b01,
            finished_at_millis: 1_700_000_000_000,
            flags: BackupFlags {
                checksum: true,
                copy_only: false,
            },
        }
    }

    #[test]
    fn header_and_blocks_round_trip() {
        let header = sample_header();
        let mut buf = Vec::new();
        {
            let mut writer = BackupWriter::new(&mut buf, &header).unwrap();
            writer
                .write_block(BlockType::AllocMap, &[1, 2, 3, 4])
                .unwrap();
            writer
                .write_block(BlockType::PageData, &vec![0xab; 4096])
                .unwrap();
            writer
                .write_block(BlockType::LogChunk, b"logbytes")
                .unwrap();
            writer.finish().unwrap();
        }

        let (mut reader, read_header) = BackupReader::new(&buf[..]).unwrap();
        assert_eq!(read_header, header, "header round-trips exactly");
        let mut seen = Vec::new();
        while let Some((block_type, payload)) = reader.next_block().unwrap() {
            seen.push((block_type, payload));
        }
        assert_eq!(
            seen,
            vec![
                (BlockType::AllocMap, vec![1, 2, 3, 4]),
                (BlockType::PageData, vec![0xab; 4096]),
                (BlockType::LogChunk, b"logbytes".to_vec()),
            ]
        );
    }

    #[test]
    fn block_codecs_round_trip() {
        let runs = vec![(0u64, 3u64), (7, 1), (100, 64)];
        assert_eq!(decode_alloc_map(&encode_alloc_map(&runs)).unwrap(), runs);

        let pages = vec![0x5au8; 2 * PAGE_SIZE];
        let encoded_pages = encode_page_run(9, &pages);
        let (start, count, bytes) = decode_page_run(&encoded_pages).unwrap();
        assert_eq!((start, count), (9, 2));
        assert_eq!(bytes, &pages[..]);

        let log = b"raw-wal-entry-bytes";
        let encoded_log = encode_log_chunk(4096, log);
        let (lsn, bytes) = decode_log_chunk(&encoded_log).unwrap();
        assert_eq!(lsn, 4096);
        assert_eq!(bytes, log);
    }

    #[test]
    fn a_partial_page_run_is_rejected() {
        // 8-byte start prefix + 100 bytes: not a whole page.
        let mut payload = 3u64.to_le_bytes().to_vec();
        payload.extend_from_slice(&[0u8; 100]);
        assert!(decode_page_run(&payload).is_err());
    }

    #[test]
    fn a_tampered_block_is_detected() {
        let header = sample_header();
        let mut buf = Vec::new();
        {
            let mut writer = BackupWriter::new(&mut buf, &header).unwrap();
            writer
                .write_block(BlockType::PageData, b"important")
                .unwrap();
            writer.finish().unwrap();
        }
        // Flip a byte inside the PageData payload.
        let flip = buf.windows(9).position(|w| w == b"important").unwrap();
        buf[flip] ^= 0xff;
        let (mut reader, _) = BackupReader::new(&buf[..]).unwrap();
        let err = reader.next_block().unwrap_err();
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "corruption detected"
        );
    }

    #[test]
    fn a_truncated_backup_is_detected_by_the_trailer_count() {
        // A stream missing its trailer (truncated mid-write) fails to read to end.
        let header = sample_header();
        let mut buf = Vec::new();
        {
            let mut writer = BackupWriter::new(&mut buf, &header).unwrap();
            writer.write_block(BlockType::PageData, b"data").unwrap();
            // NOTE: no finish() → no trailer.
        }
        let (mut reader, _) = BackupReader::new(&buf[..]).unwrap();
        assert!(
            reader.next_block().unwrap().is_some(),
            "the data block reads"
        );
        // The next read hits EOF where the trailer should be.
        assert!(reader.next_block().is_err(), "missing trailer is an error");
    }
}
