//! Temp-extent-backed row spool for spill-capable operators (external merge
//! sort, grace-hash) — Stage 8.
//!
//! Rows are serialized with a self-describing, cap-free spill codec
//! ([`encode_spill_row`]) and laid out as a length-prefixed byte stream across
//! one or more 64-page *temporary* extents ([`Storage::allocate_extent`] with
//! `temp = true`). Unlike the on-disk table codec the spill codec has no
//! 3900-byte in-row cap, so arbitrarily wide scratch rows round-trip. Temp
//! extents are excluded from the checkpoint bitmap and reclaimed wholesale on
//! restart, and spill page writes bypass the WAL, so a spool is pure
//! query-scratch: never durable, never recovered. The extents are freed when the
//! [`RowSpool`] is dropped.
//!
//! Concurrency: page I/O currently goes through the single storage lock (one
//! locked call per 4 KiB page). That is correct but serializes with other
//! storage ops; a dedicated spill file handle is a later optimization.

use crate::allocator::EXTENT_PAGES;
use crate::relstore::types::Datum;
use crate::storage::{Storage, StorageError};
use crate::storage_layout::PAGE_SIZE;

/// An append-then-scan spool of rows on temp extents. Write every row, call
/// [`RowSpool::finish_writing`], then read them back in order with
/// [`RowSpool::reader`]. Dropping the spool frees its temp extents.
///
/// Rows use a self-describing spill encoding ([`encode_spill_row`]) with no size
/// cap — spill scratch rows can be arbitrarily wide (e.g. a join's concatenated
/// source row), unlike the 3900-byte in-row table codec.
pub struct RowSpool<'a> {
    storage: &'a Storage,
    /// Start page of each allocated 64-page temp extent, in write order.
    extents: Vec<u64>,
    /// Bytes accumulated but not yet flushed to a full page.
    pending: Vec<u8>,
    /// Number of whole pages already written (the spool's logical page count
    /// minus the not-yet-flushed tail).
    pages_written: u64,
    /// Total payload bytes (length prefixes + encoded rows).
    byte_len: u64,
    rows: u64,
    finished: bool,
}

impl<'a> RowSpool<'a> {
    /// Creates an empty spool.
    pub fn new(storage: &'a Storage) -> Self {
        RowSpool {
            storage,
            extents: Vec::new(),
            pending: Vec::new(),
            pages_written: 0,
            byte_len: 0,
            rows: 0,
            finished: false,
        }
    }

    pub fn row_count(&self) -> u64 {
        self.rows
    }

    /// The absolute data-region page number for logical spool page `index`,
    /// allocating a new temp extent when the current ones are exhausted.
    fn page_for(&mut self, index: u64) -> Result<u64, StorageError> {
        let extent_ix = (index / EXTENT_PAGES) as usize;
        while extent_ix >= self.extents.len() {
            let start = self.storage.allocate_extent(true)?;
            self.extents.push(start);
        }
        Ok(self.extents[extent_ix] + index % EXTENT_PAGES)
    }

    /// Appends one row.
    pub fn write_row(&mut self, row: &[Datum]) -> Result<(), StorageError> {
        debug_assert!(!self.finished);
        let encoded = encode_spill_row(row);
        let len = encoded.len() as u32;
        self.pending.extend_from_slice(&len.to_le_bytes());
        self.pending.extend_from_slice(&encoded);
        self.byte_len += 4 + encoded.len() as u64;
        while self.pending.len() >= PAGE_SIZE {
            let page = self.page_for(self.pages_written)?;
            self.storage
                .spill_write_page(page, &self.pending[..PAGE_SIZE])?;
            self.pending.drain(..PAGE_SIZE);
            self.pages_written += 1;
        }
        self.rows += 1;
        Ok(())
    }

    /// Flushes the final partial page. Must be called before reading.
    pub fn finish_writing(&mut self) -> Result<(), StorageError> {
        if self.finished {
            return Ok(());
        }
        if !self.pending.is_empty() {
            let page = self.page_for(self.pages_written)?;
            let mut padded = vec![0u8; PAGE_SIZE];
            padded[..self.pending.len()].copy_from_slice(&self.pending);
            self.storage.spill_write_page(page, &padded)?;
            self.pending.clear();
            self.pages_written += 1;
        }
        self.finished = true;
        Ok(())
    }

    /// A sequential reader over the spooled rows (in write order).
    pub fn reader(&self) -> RowSpoolReader<'_, 'a> {
        debug_assert!(self.finished);
        RowSpoolReader {
            spool: self,
            buf: Vec::new(),
            buf_pos: 0,
            next_page: 0,
            bytes_read: 0,
        }
    }
}

impl Drop for RowSpool<'_> {
    fn drop(&mut self) {
        for start in &self.extents {
            let _ = self.storage.free_extent(*start);
        }
    }
}

/// Serializes one row for spill: a self-describing, cap-free tag+payload stream
/// (`u16` column count, then per datum a 1-byte tag and its value; variable-
/// length values carry a `u32` length). Unlike the on-disk table codec it has no
/// 3900-byte in-row cap and no `u16` var-section limit, so an arbitrarily wide
/// scratch row (a join's concatenated source row) round-trips.
pub fn encode_spill_row(row: &[Datum]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + row.len() * 8);
    out.extend_from_slice(&(row.len() as u16).to_le_bytes());
    for datum in row {
        match datum {
            Datum::Null => out.push(0),
            Datum::OverflowRef { .. } => {
                unreachable!("overflow reference escaped the storage layer")
            }
            Datum::TinyInt(v) => {
                out.push(1);
                out.push(*v);
            }
            Datum::SmallInt(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Int(v) => {
                out.push(3);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::BigInt(v) => {
                out.push(4);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Bit(b) => {
                out.push(5);
                out.push(*b as u8);
            }
            Datum::Real(v) => {
                out.push(6);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Float(v) => {
                out.push(7);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Decimal(v) => {
                out.push(8);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Date(v) => {
                out.push(9);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Time(v) => {
                out.push(10);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Datum::DateTime2(d, t) => {
                out.push(11);
                out.extend_from_slice(&d.to_le_bytes());
                out.extend_from_slice(&t.to_le_bytes());
            }
            Datum::UniqueIdentifier(g) => {
                out.push(12);
                out.extend_from_slice(g);
            }
            Datum::VarChar(s) => encode_bytes(&mut out, 13, s.as_bytes()),
            Datum::NVarChar(s) => encode_bytes(&mut out, 14, s.as_bytes()),
            Datum::VarBinary(b) => encode_bytes(&mut out, 15, b),
        }
    }
    out
}

fn encode_bytes(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Decodes a row written by [`encode_spill_row`]. A malformed buffer (never
/// produced by the encoder) is a storage error.
pub fn decode_spill_row(bytes: &[u8]) -> Result<Vec<Datum>, StorageError> {
    let mut cur = Cursor { bytes, pos: 0 };
    let ncols = cur.u16()? as usize;
    let mut row = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let tag = cur.u8()?;
        let datum = match tag {
            0 => Datum::Null,
            1 => Datum::TinyInt(cur.u8()?),
            2 => Datum::SmallInt(i16::from_le_bytes(cur.fixed()?)),
            3 => Datum::Int(i32::from_le_bytes(cur.fixed()?)),
            4 => Datum::BigInt(i64::from_le_bytes(cur.fixed()?)),
            5 => Datum::Bit(cur.u8()? != 0),
            6 => Datum::Real(f32::from_le_bytes(cur.fixed()?)),
            7 => Datum::Float(f64::from_le_bytes(cur.fixed()?)),
            8 => Datum::Decimal(i128::from_le_bytes(cur.fixed()?)),
            9 => Datum::Date(u32::from_le_bytes(cur.fixed()?)),
            10 => Datum::Time(u64::from_le_bytes(cur.fixed()?)),
            11 => Datum::DateTime2(
                u32::from_le_bytes(cur.fixed()?),
                u64::from_le_bytes(cur.fixed()?),
            ),
            12 => Datum::UniqueIdentifier(cur.fixed()?),
            13 => Datum::VarChar(cur.string()?),
            14 => Datum::NVarChar(cur.string()?),
            15 => Datum::VarBinary(cur.bytes()?.to_vec()),
            other => {
                return Err(StorageError::InvalidFile(format!(
                    "spill row: unknown datum tag {other}"
                )));
            }
        };
        row.push(datum);
    }
    Ok(row)
}

/// A little-endian reader over a spill row buffer.
struct Cursor<'b> {
    bytes: &'b [u8],
    pos: usize,
}

impl<'b> Cursor<'b> {
    fn take(&mut self, n: usize) -> Result<&'b [u8], StorageError> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.bytes.len());
        let Some(end) = end else {
            return Err(StorageError::InvalidFile("spill row: truncated".into()));
        };
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, StorageError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], StorageError> {
        Ok(self.take(N)?.try_into().unwrap())
    }
    fn bytes(&mut self) -> Result<&'b [u8], StorageError> {
        let len = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        self.take(len)
    }
    fn string(&mut self) -> Result<String, StorageError> {
        let raw = self.bytes()?;
        String::from_utf8(raw.to_vec())
            .map_err(|_| StorageError::InvalidFile("spill row: invalid utf8".into()))
    }
}

/// Sequential reader returned by [`RowSpool::reader`].
pub struct RowSpoolReader<'s, 'a> {
    spool: &'s RowSpool<'a>,
    /// Bytes read from pages but not yet consumed into rows.
    buf: Vec<u8>,
    buf_pos: usize,
    next_page: u64,
    bytes_read: u64,
}

impl RowSpoolReader<'_, '_> {
    /// Reads the next spool page into `buf`, bounded by the spool's payload
    /// length so the padding of the final page is never parsed. Returns whether
    /// a page was read.
    fn fill(&mut self) -> Result<bool, StorageError> {
        if self.bytes_read >= self.spool.byte_len {
            return Ok(false);
        }
        let index = self.next_page;
        let extent_ix = (index / EXTENT_PAGES) as usize;
        let page = self.spool.extents[extent_ix] + index % EXTENT_PAGES;
        let mut frame = vec![0u8; PAGE_SIZE];
        self.spool.storage.spill_read_page(page, &mut frame)?;
        let remaining = self.spool.byte_len - self.bytes_read;
        let take = (PAGE_SIZE as u64).min(remaining) as usize;
        // Drop already-consumed bytes to keep `buf` bounded, then append.
        self.buf.drain(..self.buf_pos);
        self.buf_pos = 0;
        self.buf.extend_from_slice(&frame[..take]);
        self.bytes_read += take as u64;
        self.next_page += 1;
        Ok(true)
    }

    /// The next row, or `None` at end of spool.
    pub fn next_row(&mut self) -> Result<Option<Vec<Datum>>, StorageError> {
        loop {
            // Need the 4-byte length prefix.
            if self.buf.len() - self.buf_pos < 4 {
                if !self.fill()? {
                    return Ok(None);
                }
                continue;
            }
            let len_bytes = [
                self.buf[self.buf_pos],
                self.buf[self.buf_pos + 1],
                self.buf[self.buf_pos + 2],
                self.buf[self.buf_pos + 3],
            ];
            let len = u32::from_le_bytes(len_bytes) as usize;
            if self.buf.len() - self.buf_pos < 4 + len {
                if !self.fill()? {
                    return Ok(None); // truncated tail — treat as end
                }
                continue;
            }
            let start = self.buf_pos + 4;
            let row = decode_spill_row(&self.buf[start..start + len])?;
            self.buf_pos += 4 + len;
            return Ok(Some(row));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageOptions;

    fn temp_storage(label: &str) -> Storage {
        let mut path = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("truthdb-spill-{label}-{nanos}.db"));
        Storage::create_with_wal_bounds(
            path,
            StorageOptions {
                size_gib: 1,
                wal_ratio: 0.05,
                metadata_ratio: 0.08,
                snapshot_ratio: 0.02,
                allocator_ratio: 0.02,
                reserved_ratio: 0.17,
                default_collation: None,
            },
            8 * 1024 * 1024,
            8 * 1024 * 1024,
        )
        .expect("create storage")
    }

    fn read_all(spool: &RowSpool) -> Vec<Vec<Datum>> {
        let mut reader = spool.reader();
        let mut out = Vec::new();
        while let Some(row) = reader.next_row().expect("read") {
            out.push(row);
        }
        out
    }

    #[test]
    fn round_trips_many_rows_across_extents() {
        let storage = temp_storage("roundtrip");
        // Enough rows (each with a chunky string) to fill well past one 64-page
        // (256 KiB) extent, so the multi-extent path is exercised.
        let expected: Vec<Vec<Datum>> = (0..3000)
            .map(|i| {
                vec![
                    Datum::Int(i),
                    Datum::NVarChar(format!("row-{i}-{}", "x".repeat((i % 200) as usize))),
                ]
            })
            .collect();

        let mut spool = RowSpool::new(&storage);
        for row in &expected {
            spool.write_row(row).expect("write");
        }
        spool.finish_writing().expect("finish");
        assert_eq!(spool.row_count(), expected.len() as u64);
        assert!(spool.extents.len() > 1, "should span multiple temp extents");

        assert_eq!(read_all(&spool), expected);
        // A second read yields the same rows (the spool is not consumed).
        assert_eq!(read_all(&spool), expected);
    }

    #[test]
    fn null_empty_and_wide_rows() {
        let storage = temp_storage("nulls");
        let expected = vec![
            vec![Datum::Int(1), Datum::Null],
            vec![Datum::Int(2), Datum::NVarChar(String::new())],
            // A row far WIDER than the 3900-byte in-row table cap and than one
            // page — the spill codec has no cap, so it must round-trip (this is
            // the wide join/derived source row the table codec would reject).
            vec![Datum::Int(3), Datum::NVarChar("y".repeat(20_000))],
            vec![Datum::Int(4), Datum::VarBinary(vec![0xAB; 9000])],
            vec![Datum::Int(5), Datum::NVarChar("z".into())],
        ];
        let mut spool = RowSpool::new(&storage);
        for row in &expected {
            spool.write_row(row).expect("write");
        }
        spool.finish_writing().expect("finish");
        assert_eq!(read_all(&spool), expected);
    }

    #[test]
    fn every_datum_variant_round_trips() {
        let storage = temp_storage("variants");
        let row = vec![
            Datum::Null,
            Datum::TinyInt(255),
            Datum::SmallInt(-12345),
            Datum::Int(-1),
            Datum::BigInt(i64::MIN),
            Datum::Bit(true),
            Datum::Real(-2.5),
            Datum::Float(std::f64::consts::PI),
            Datum::Decimal(-170141183460469231731687303715884105728),
            Datum::Date(738000),
            Datum::Time(863990000000),
            Datum::DateTime2(738000, 12345),
            Datum::UniqueIdentifier([7u8; 16]),
            Datum::VarChar("héllo".into()),
            Datum::NVarChar("naïve 😀".into()),
            Datum::VarBinary(vec![0, 1, 2, 255]),
        ];
        let mut spool = RowSpool::new(&storage);
        spool.write_row(&row).expect("write");
        spool.finish_writing().expect("finish");
        assert_eq!(read_all(&spool), vec![row]);
    }

    #[test]
    fn empty_spool_reads_nothing() {
        let storage = temp_storage("empty");
        let mut spool = RowSpool::new(&storage);
        spool.finish_writing().expect("finish");
        assert_eq!(spool.row_count(), 0);
        assert!(read_all(&spool).is_empty());
    }

    #[test]
    fn dropping_a_spool_frees_its_temp_extents_for_reuse() {
        let storage = temp_storage("reclaim");
        let first_extent = {
            let mut spool = RowSpool::new(&storage);
            for i in 0..500 {
                spool
                    .write_row(&[Datum::Int(i), Datum::NVarChar("data".repeat(50))])
                    .expect("write");
            }
            spool.finish_writing().expect("finish");
            let start = spool.extents[0];
            assert!(storage.is_page_allocated(start));
            start
        }; // spool dropped here -> extents freed
        assert!(
            !storage.is_page_allocated(first_extent),
            "temp extent must be freed on drop"
        );
    }
}
