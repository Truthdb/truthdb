//! Temp-extent-backed row spool for spill-capable operators (external merge
//! sort, grace-hash) — Stage 8.
//!
//! Rows are serialized with the ordinary [`row`](crate::relstore::row) codec and
//! laid out as a length-prefixed byte stream across one or more 64-page
//! *temporary* extents ([`Storage::allocate_extent`] with `temp = true`). Temp
//! extents are excluded from the checkpoint bitmap and reclaimed wholesale on
//! restart, and spill page writes bypass the WAL, so a spool is pure
//! query-scratch: never durable, never recovered. The extents are freed when the
//! [`RowSpool`] is dropped.
//!
//! Concurrency: page I/O currently goes through the single storage lock (one
//! locked call per 4 KiB page). That is correct but serializes with other
//! storage ops; a dedicated spill file handle is a later optimization.

use crate::allocator::EXTENT_PAGES;
use crate::relstore::row::{Schema, decode_row, encode_row};
use crate::relstore::types::Datum;
use crate::storage::{Storage, StorageError};
use crate::storage_layout::PAGE_SIZE;

/// An append-then-scan spool of rows on temp extents. Write every row, call
/// [`RowSpool::finish_writing`], then read them back in order with
/// [`RowSpool::reader`]. Dropping the spool frees its temp extents.
pub struct RowSpool<'a> {
    storage: &'a Storage,
    schema: Schema,
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
    /// Creates an empty spool. `schema` must match the rows written (its column
    /// types drive the row codec; names/nullability only need to round-trip).
    pub fn new(storage: &'a Storage, schema: Schema) -> Self {
        RowSpool {
            storage,
            schema,
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
        let encoded = encode_row(&self.schema, row).map_err(spill_codec_err)?;
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

/// Maps a row-codec failure to a storage error (spill payloads are engine-
/// generated, so this indicates a bug rather than user input).
fn spill_codec_err(err: crate::relstore::types::TypeError) -> StorageError {
    StorageError::InvalidConfig(format!("spill row codec: {err}"))
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
            let row = decode_row(&self.spool.schema, &self.buf[start..start + len])
                .map_err(spill_codec_err)?;
            self.buf_pos += 4 + len;
            return Ok(Some(row));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relstore::row::Column;
    use crate::relstore::types::ColumnType;
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
            },
            8 * 1024 * 1024,
            8 * 1024 * 1024,
        )
        .expect("create storage")
    }

    fn schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "id".into(),
                    column_type: ColumnType::Int,
                    nullable: false,
                    collation: None,
                },
                Column {
                    name: "s".into(),
                    column_type: ColumnType::NVarChar { max_len: 4000 },
                    nullable: true,
                    collation: None,
                },
            ],
        }
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

        let mut spool = RowSpool::new(&storage, schema());
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
    fn null_and_empty_and_boundary_rows() {
        let storage = temp_storage("nulls");
        let expected = vec![
            vec![Datum::Int(1), Datum::Null],
            vec![Datum::Int(2), Datum::NVarChar(String::new())],
            // A near-cap row (rows straddle page boundaries; the codec caps a
            // single row below a page, so a row never exceeds one page itself).
            vec![Datum::Int(3), Datum::NVarChar("y".repeat(1800))],
            vec![Datum::Int(4), Datum::NVarChar("z".into())],
        ];
        let mut spool = RowSpool::new(&storage, schema());
        for row in &expected {
            spool.write_row(row).expect("write");
        }
        spool.finish_writing().expect("finish");
        assert_eq!(read_all(&spool), expected);
    }

    #[test]
    fn empty_spool_reads_nothing() {
        let storage = temp_storage("empty");
        let mut spool = RowSpool::new(&storage, schema());
        spool.finish_writing().expect("finish");
        assert_eq!(spool.row_count(), 0);
        assert!(read_all(&spool).is_empty());
    }

    #[test]
    fn dropping_a_spool_frees_its_temp_extents_for_reuse() {
        let storage = temp_storage("reclaim");
        let first_extent = {
            let mut spool = RowSpool::new(&storage, schema());
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
