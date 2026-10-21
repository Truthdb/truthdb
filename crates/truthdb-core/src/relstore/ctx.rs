//! Execution context for relational page operations.
//!
//! Every mutation flows through [`RelCtx::apply_op`]: the physiological redo
//! op is applied to the page by the same code recovery uses
//! ([`apply_redo_to_page`]), then logged. The first modification of a page
//! since the last checkpoint is logged as a full page image instead
//! (torn-page repair); the record still carries the operation's undo.
//!
//! Write-ahead ordering: records are appended (page-written but not
//! necessarily fsynced) before the buffer frame is marked dirty with the
//! record's LSN; the buffer pool fsyncs the log up to a page's LSN before
//! stealing it, and commit forces the log.

use std::collections::HashMap;

use crate::allocator::PageAllocator;
use crate::direct_io::{AlignedPageBuf, DirectFile};
use crate::relstore::buffer_pool::{BufferPool, FrameId, PoolBackend};
use crate::relstore::page::{self, PageHeader};
use crate::relstore::slotted::SlottedPage;
use crate::storage::StorageError;
use crate::storage_layout::{PAGE_SIZE, WAL_ENTRY_TYPE_REL};
use crate::wal::WalWriter;
use crate::wal::records::{
    PageOpRedo, PageOpUndo, REL_KIND_PAGE_IMAGE, REL_KIND_PAGE_IMAGES, REL_KIND_PAGE_OP, RelRecord,
};

// v2 adds a commit-record timestamp (in the commit's `redo`) for point-in-time
// restore. Nothing gates on the version, so v1 records decode unchanged.
const REL_WAL_ENTRY_VERSION: u16 = 2;

/// Raw page I/O + WAL watermark for the buffer pool, over the data region.
pub struct PoolIo<'a> {
    pub file: &'a mut DirectFile,
    pub wal: &'a mut WalWriter,
    pub codec: &'a crate::encryption::PageCodec,
    pub data_offset: u64,
    pub data_pages: u64,
}

impl PoolIo<'_> {
    fn offset_of(&self, page_no: u64) -> Result<u64, StorageError> {
        if page_no >= self.data_pages {
            return Err(StorageError::InvalidFile(format!(
                "page {page_no} outside the data region"
            )));
        }
        Ok(self.data_offset + page_no * PAGE_SIZE as u64)
    }
}

impl PoolIo<'_> {
    /// While `encryption enable` converts the database: zeroes the pages of
    /// a fresh allocation that hold something (old plaintext, maybe; the
    /// conversion's own zeroing may already have passed them, and a page an
    /// allocation never writes keeps its old bytes). The log goes to disk
    /// first: the frees that made these pages free must be durable before
    /// their old contents go.
    pub(crate) fn zero_if_converting(
        &mut self,
        start_page: u64,
        num_pages: u64,
    ) -> Result<(), StorageError> {
        if !self
            .codec
            .wal_keys()
            .is_some_and(|keys| keys.converting() || keys.rekeying())
        {
            return Ok(());
        }
        let mut page = AlignedPageBuf::new();
        let zero = AlignedPageBuf::new();
        let mut synced = false;
        for page_no in start_page..start_page + num_pages {
            self.read_page(page_no, &mut page)?;
            if !page::is_zero_page(page.as_slice()) {
                // Forced once, and only when there is something to zero.
                if !synced {
                    self.wal.sync_all()?;
                    synced = true;
                }
                self.write_page(page_no, &zero)?;
            }
        }
        Ok(())
    }
}

impl PoolBackend for PoolIo<'_> {
    fn read_page(&mut self, page_no: u64, frame: &mut AlignedPageBuf) -> Result<(), StorageError> {
        let offset = self.offset_of(page_no)?;
        self.file.read_page_into(offset, frame)?;
        Ok(())
    }

    fn write_page(&mut self, page_no: u64, frame: &AlignedPageBuf) -> Result<(), StorageError> {
        let offset = self.offset_of(page_no)?;
        self.file.write_page_from(offset, frame)?;
        Ok(())
    }

    fn flushed_lsn(&self) -> u64 {
        self.wal.flushed_lsn()
    }

    fn flush_wal_to(&mut self, lsn: u64) -> Result<(), StorageError> {
        self.wal.sync_to(lsn)
    }

    fn codec(&self) -> &crate::encryption::PageCodec {
        self.codec
    }
}

/// Page and transaction state the log's commit and recovery protocol owns:
/// the buffer pool, the dirty-page table, the transaction-id counter, the
/// open explicit transactions, and the wedge flag.
pub struct PageState {
    pub pool: BufferPool,
    /// Set when a commit or rollback could not be logged: the pool may hold
    /// state the WAL cannot explain, so all relational work — and above all
    /// checkpoints, which would make that state durable and unrecoverable —
    /// is refused until a restart replays the log.
    pub wedged: bool,
    /// Pages dirtied since the last checkpoint -> LSN of their first change.
    pub dpt: HashMap<u64, u64>,
    pub next_txn_id: u64,
    /// Open explicit (multi-statement) transactions: txn id → its `BEGIN` LSN.
    /// A (fuzzy) checkpoint may run while these are open — it flushes their
    /// (uncommitted) pages under the steal policy — but must clamp the WAL head
    /// to the *oldest* begin LSN here so their undo records survive for crash
    /// rollback. Autocommit statements never leave one open across calls.
    pub active_txn_begins: std::collections::BTreeMap<u64, u64>,
}

impl PageState {
    pub fn new(pool_capacity_bytes: u64) -> Self {
        PageState {
            pool: BufferPool::new(pool_capacity_bytes),
            wedged: false,
            dpt: HashMap::new(),
            next_txn_id: 1,
            active_txn_begins: std::collections::BTreeMap::new(),
        }
    }
}

/// An active (statement-scoped, autocommit) transaction: the WAL chain tail
/// and an in-memory undo log so statement rollback never re-reads the ring.
pub struct TxnLink {
    pub txn_id: u64,
    pub last_lsn: u64,
    pub undo_log: Vec<(u64, PageOpUndo)>,
    /// Extents freed inside this transaction, `(start_page, num_pages)`:
    /// returned to the allocator only once the commit record is appended, so
    /// a rollback never has to take back pages someone else allocated.
    pub pending_frees: Vec<(u64, u64)>,
}

/// A marker in a live transaction's log to which a later partial rollback can
/// return, undoing only the work done since (statement-level atomicity). Captures
/// the undo-log length and the WAL chain tail at capture time.
#[derive(Clone, Copy)]
pub struct Savepoint {
    pub undo_len: usize,
    pub last_lsn: u64,
    /// How many extent frees the transaction had logged: a partial rollback
    /// cannot take back a free logged after the savepoint (see
    /// [`crate::relstore::recovery::rollback_to`]).
    pub pending_frees_len: usize,
}

impl TxnLink {
    /// Captures a savepoint at the current point in this transaction.
    pub fn savepoint(&self) -> Savepoint {
        Savepoint {
            undo_len: self.undo_log.len(),
            last_lsn: self.last_lsn,
            pending_frees_len: self.pending_frees.len(),
        }
    }
}

/// How an access-method operation logs its user-visible page op: as a
/// forward transaction record (with undo) or as a compensation record
/// during rollback/recovery-undo. Structural changes (splits, new pages)
/// are always system records regardless.
pub enum OpMode<'t> {
    Txn(&'t mut TxnLink),
    Clr {
        txn: &'t mut TxnLink,
        undo_next: u64,
    },
}

impl OpMode<'_> {
    /// Builds the [`LogMode`] for one page op: forward mode attaches the
    /// undo, CLR mode drops it (CLRs are never undone).
    pub fn log_mode(&mut self, undo: PageOpUndo) -> LogMode<'_> {
        match self {
            OpMode::Txn(txn) => LogMode::Txn(txn, undo),
            OpMode::Clr { txn, undo_next } => LogMode::Clr {
                txn,
                undo_next: *undo_next,
            },
        }
    }
}

/// How a page mutation is logged.
pub enum LogMode<'t> {
    /// User-transaction record with an undo action.
    Txn(&'t mut TxnLink, PageOpUndo),
    /// Compensation record during rollback/recovery-undo: `undo_next` points
    /// at the next record to undo.
    Clr {
        txn: &'t mut TxnLink,
        undo_next: u64,
    },
    /// System change (splits, page formats, chain links): redo-only,
    /// txn 0, never undone.
    System,
}

#[cfg(any(test, feature = "test-hooks"))]
thread_local! {
    /// Test fault injection: `Some(n)` makes the (n+1)-th apply_op fail
    /// after syncing the log — prior appends stay durable, simulating a
    /// crash at that exact point (including inside recovery undo).
    pub static FAIL_APPLY_OPS_AFTER: std::cell::Cell<Option<u32>> =
        const { std::cell::Cell::new(None) };
}

pub struct RelCtx<'a> {
    pub pool: &'a mut BufferPool,
    pub io: PoolIo<'a>,
    pub allocator: &'a mut PageAllocator,
    /// Pages dirtied since the last checkpoint -> LSN of their first change
    /// (the FPI). Drives FPI-on-first-touch.
    pub dpt: &'a mut HashMap<u64, u64>,
    /// True on undo paths (rollback, recovery): appends may use the WAL's
    /// compensation reserve, which forward statements must not touch.
    pub use_reserve: bool,
    /// The CONTAINER TAG stamped into forward page-scoped records' `flags`:
    /// the database id whose data the statement mutates (page ops on a
    /// database's tables, its catalog-row writes), or 0 = global/unattributed
    /// (transaction control, allocator, catalog root, EVERY CLR, and every
    /// pre-tag log). CLRs are never tagged: compensation is recovery
    /// machinery, and a rollback can span databases — the review showed a
    /// per-file latch mis-tagging the CLRs of a cross-database rollback. The
    /// tag is a FILTERING aid for future log subscribers, never a correctness
    /// input: consumers must treat 0 as unfiltered and include it in every
    /// subscription (which delivers them all compensation, keeping a filtered
    /// copy convergent through rollbacks).
    pub container: u16,
}

impl RelCtx<'_> {
    pub fn fetch(&mut self, page_no: u64) -> Result<FrameId, StorageError> {
        self.pool.fetch(page_no, &mut self.io)
    }

    pub fn fetch_zeroed(&mut self, page_no: u64) -> Result<FrameId, StorageError> {
        self.pool.fetch_zeroed(page_no, &mut self.io)
    }

    /// Allocates one page in the data region, WAL-logged (redo-idempotent).
    pub fn allocate_page(&mut self, txn_id: u64) -> Result<u64, StorageError> {
        let page_no = self
            .allocator
            .allocate(1)
            .ok_or_else(|| StorageError::InvalidConfig("data region full".to_string()))?;
        let record = RelRecord::alloc_extent(page_no, 1);
        if let Err(err) = self.append(&record, false) {
            self.allocator.free(page_no, 1);
            return Err(err);
        }
        self.io.zero_if_converting(page_no, 1)?;
        let _ = txn_id;
        Ok(page_no)
    }

    /// Frees one page outside any transaction: a structural change (a B+
    /// tree page emptied and unlinked), never undone. The caller logs the
    /// unlinking first, so a crash between the two leaks the page rather than
    /// freeing a page still linked.
    pub fn free_page(&mut self, page_no: u64) -> Result<(), StorageError> {
        self.append(&RelRecord::free_extent(page_no, 1), false)?;
        // Its content no longer matters: the frame goes, so it cannot be
        // flushed over whatever the page holds next.
        self.pool.discard(page_no);
        self.allocator.free(page_no, 1);
        Ok(())
    }

    pub fn append(&mut self, record: &RelRecord, sync: bool) -> Result<u64, StorageError> {
        let mut payload = record.encode();
        // Stamp the container tag into FORWARD page-scoped records (the
        // envelope's otherwise-unused `flags` at bytes 18..20 — no layout
        // change, old logs and old builds are unaffected). Everything else
        // stays 0: a transaction can span containers, the allocator and
        // catalog root are global, and CLRs are compensation — a rollback
        // can undo several databases' ops under one stale-prone context, so
        // they are always 0 (subscribers include 0 in every subscription).
        if self.container != 0
            && matches!(
                record.kind,
                REL_KIND_PAGE_OP | REL_KIND_PAGE_IMAGE | REL_KIND_PAGE_IMAGES
            )
        {
            payload[18..20].copy_from_slice(&self.container.to_le_bytes());
        }
        self.io.wal.append_entry_reserve(
            WAL_ENTRY_TYPE_REL,
            REL_WAL_ENTRY_VERSION,
            0,
            &payload,
            sync,
            self.use_reserve,
        )
    }

    pub fn begin(&mut self, txn_id: u64) -> Result<TxnLink, StorageError> {
        let lsn = self.append(&RelRecord::txn_begin(txn_id), false)?;
        Ok(TxnLink {
            txn_id,
            last_lsn: lsn,
            undo_log: Vec::new(),
            pending_frees: Vec::new(),
        })
    }

    /// Commit = force the log at the commit record, then an end record. The
    /// end record is an ATT-cleanup optimization (analysis already treats
    /// COMMIT as terminal), so its failure must not fail a durable commit.
    /// Returns the commit record's LSN — the version store orders commits and
    /// finds the durable prefix by it.
    pub fn commit(&mut self, txn: TxnLink) -> Result<u64, StorageError> {
        // The commit record is written but NOT fsynced here: group commit makes
        // it durable via the log-writer once the executor calls
        // `Storage::ensure_durable` at the end of the batch, so one fsync serves
        // every commit in the window. The record must reach the disk before the
        // batch is acknowledged; nothing before that ack depends on it.
        let timestamp_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let commit_lsn = self.append(
            &RelRecord::txn_commit(txn.txn_id, txn.last_lsn, timestamp_millis),
            false,
        )?;
        let _ = self.append(&RelRecord::txn_end(txn.txn_id, commit_lsn), false);
        // Any later allocation of these pages is logged after the commit
        // record, so a crash that keeps it keeps the commit too.
        for (start_page, num_pages) in txn.pending_frees {
            self.allocator.free(start_page, num_pages);
        }
        Ok(commit_lsn)
    }

    /// Allocates `num_pages` contiguous pages inside `txn`: the allocation is
    /// undone (the extent freed) if the transaction rolls back or loses at
    /// restart. For pages no structural change outside the transaction can
    /// reach — a B+ tree split, which is never undone, must not use this.
    ///
    /// The caller holds the storage lock for the whole transaction (or
    /// registers it as an active transaction): a checkpoint between this
    /// record and the commit would otherwise reclaim the log restart undo
    /// needs.
    pub fn allocate_extent_in_txn(
        &mut self,
        txn: &mut TxnLink,
        num_pages: u64,
    ) -> Result<u64, StorageError> {
        let start_page = self
            .allocator
            .allocate(num_pages)
            .ok_or_else(|| StorageError::InvalidConfig("data region full".to_string()))?;
        let record = RelRecord::alloc_extent_txn(txn.txn_id, txn.last_lsn, start_page, num_pages);
        let lsn = match self.append(&record, false) {
            Ok(lsn) => lsn,
            Err(err) => {
                self.allocator.free(start_page, num_pages);
                return Err(err);
            }
        };
        txn.last_lsn = lsn;
        txn.undo_log.push((
            lsn,
            PageOpUndo::FreeExtent {
                start_page,
                num_pages,
            },
        ));
        self.io.zero_if_converting(start_page, num_pages)?;
        Ok(start_page)
    }

    /// Writes a blob's body pages (`data` split into `BLOB_PAGE_DATA`-byte
    /// pages from `start_page`, the last one zero-padded) inside `txn`, over
    /// pages the transaction allocated. The record is forced to disk first
    /// (write-ahead), then each page goes straight to the data file — never
    /// through the buffer pool, so a large body does not flush the pool's hot
    /// pages. Returns the record's LSN, which every page carries.
    ///
    /// `data` must be non-empty, at most `BLOB_RECORD_MAX_PAGES` pages, and
    /// land on allocated pages: a durable record that redo cannot apply would
    /// stop the database from opening. If a page write fails after the record
    /// is durable, the caller rolls the transaction back.
    pub fn write_blob_pages(
        &mut self,
        txn: &mut TxnLink,
        object_id: u32,
        start_page: u64,
        data: &[u8],
    ) -> Result<u64, StorageError> {
        let pages = data.len().div_ceil(page::BLOB_PAGE_DATA);
        let end_page = start_page.checked_add(pages as u64);
        if pages == 0
            || pages > page::BLOB_RECORD_MAX_PAGES
            || end_page.is_none_or(|end| {
                (start_page..end)
                    .any(|page| page >= self.io.data_pages || !self.allocator.is_allocated(page))
            })
        {
            return Err(StorageError::InvalidConfig(format!(
                "blob write of {} bytes at page {start_page} is empty, too large, or not on \
                 allocated pages",
                data.len()
            )));
        }
        let record = RelRecord::blob_pages(txn.txn_id, txn.last_lsn, object_id, start_page, data);
        let lsn = self.append(&record, true)?;
        txn.last_lsn = lsn;
        for (index, chunk) in data.chunks(page::BLOB_PAGE_DATA).enumerate() {
            let page_no = start_page + index as u64;
            // A cached frame of the page's previous (freed) use must never be
            // flushed over the blob.
            self.pool.discard(page_no);
            let image = blob_page_image(self.io.codec, lsn, object_id, page_no, chunk);
            self.io.write_page(page_no, &image)?;
        }
        Ok(lsn)
    }

    /// Rewrites a blob page as it is (`plain`, read from disk), sealed by the
    /// current codec and keeping the LSN it carries: a forced BLOB_REWRITE
    /// record, then the page.
    pub fn rewrite_blob_page(&mut self, page_no: u64, plain: &[u8]) -> Result<(), StorageError> {
        let header = page::read_header(plain);
        let data = &plain[page::PAGE_HEADER_SIZE..];
        let record = RelRecord::blob_rewrite(page_no, header.page_lsn, header.object_id, data);
        self.append(&record, true)?;
        self.pool.discard(page_no);
        let image = blob_page_image(
            self.io.codec,
            header.page_lsn,
            header.object_id,
            page_no,
            data,
        );
        self.io.write_page(page_no, &image)?;
        Ok(())
    }

    /// Reads one blob page into `out` (`BLOB_PAGE_DATA` bytes) and returns its
    /// header. A page that fails its checksum, is not a blob page, or claims
    /// another page number is an error: the caller compares the header's
    /// object id and LSN with what it expects, since a freed extent can be
    /// reused.
    pub fn read_blob_page(
        &mut self,
        page_no: u64,
        out: &mut [u8],
    ) -> Result<PageHeader, StorageError> {
        let mut frame = AlignedPageBuf::new();
        self.io.read_page(page_no, &mut frame)?;
        let opened = self.io.codec.open(page_no, frame.as_mut_slice());
        let bytes = frame.as_slice();
        let header = page::read_header(bytes);
        if !opened || header.page_type != page::PAGE_TYPE_BLOB || header.page_no != page_no {
            return Err(StorageError::InvalidFile(format!(
                "page {page_no} is not a valid blob page"
            )));
        }
        out.copy_from_slice(&bytes[page::PAGE_HEADER_SIZE..]);
        Ok(header)
    }

    /// Frees an extent inside `txn`. The pages stay allocated until the
    /// transaction commits, and are never freed if it does not. The same
    /// locking rule as [`Self::allocate_extent_in_txn`] applies.
    pub fn free_extent_in_txn(
        &mut self,
        txn: &mut TxnLink,
        start_page: u64,
        num_pages: u64,
    ) -> Result<(), StorageError> {
        let record = RelRecord::free_extent_txn(txn.txn_id, txn.last_lsn, start_page, num_pages);
        txn.last_lsn = self.append(&record, false)?;
        txn.pending_frees.push((start_page, num_pages));
        Ok(())
    }

    /// Applies a physiological op to its page and logs it. Callers must have
    /// verified the op fits (page splits happen before this). If the log
    /// append fails, the in-memory page is restored from its pre-image: a
    /// mutation without a durable record must never survive in the pool.
    pub fn apply_op(&mut self, mode: LogMode<'_>, redo: PageOpRedo) -> Result<u64, StorageError> {
        #[cfg(any(test, feature = "test-hooks"))]
        {
            let fire = FAIL_APPLY_OPS_AFTER.with(|c| match c.get() {
                Some(0) => {
                    c.set(None);
                    true
                }
                Some(n) => {
                    c.set(Some(n - 1));
                    false
                }
                None => false,
            });
            if fire {
                self.io.wal.sync_all()?;
                return Err(StorageError::InvalidConfig(
                    "test fault injection".to_string(),
                ));
            }
        }

        let page_no = redo.page();
        let frame = self.fetch(page_no)?;
        let pre_image: Vec<u8> = self.pool.page(frame).to_vec();
        if let Err(err) = apply_redo_to_page(self.pool.page_mut(frame), &redo) {
            self.pool.page_mut(frame).copy_from_slice(&pre_image);
            self.pool.unpin(frame);
            return Err(err);
        }

        let first_touch = !self.dpt.contains_key(&page_no);
        let record = match &mode {
            LogMode::Txn(txn, undo) => {
                if first_touch {
                    RelRecord::page_image(
                        txn.txn_id,
                        txn.last_lsn,
                        page_no,
                        self.pool.page(frame),
                        undo,
                    )
                } else {
                    RelRecord::page_op(txn.txn_id, txn.last_lsn, &redo, undo)
                }
            }
            // CLRs compensate ops that already forced an FPI for this page
            // after the checkpoint, so they never need an image themselves.
            LogMode::Clr { txn, undo_next } => {
                RelRecord::clr(txn.txn_id, txn.last_lsn, *undo_next, &redo)
            }
            LogMode::System => {
                if first_touch {
                    RelRecord::page_image(0, 0, page_no, self.pool.page(frame), &PageOpUndo::None)
                } else {
                    RelRecord::page_op(0, 0, &redo, &PageOpUndo::None)
                }
            }
        };
        let lsn = match self.append(&record, false) {
            Ok(lsn) => lsn,
            Err(err) => {
                // No durable record: the mutation must not survive either.
                self.pool.page_mut(frame).copy_from_slice(&pre_image);
                self.pool.unpin(frame);
                return Err(err);
            }
        };
        match mode {
            LogMode::Txn(txn, undo) => {
                txn.last_lsn = lsn;
                txn.undo_log.push((lsn, undo));
            }
            LogMode::Clr { txn, .. } => {
                txn.last_lsn = lsn;
            }
            LogMode::System => {}
        }
        self.dpt.entry(page_no).or_insert(lsn);
        set_frame_lsn(self.pool.page_mut(frame), lsn);
        self.pool.unpin(frame);
        Ok(lsn)
    }

    /// Creates a table's row-counter page: allocated, formatted, zero count,
    /// logged as a system image (like a fresh tree root — a rolled-back
    /// CREATE TABLE leaves it an unreferenced orphan, which is safe).
    pub fn counter_create(&mut self, object_id: u32) -> Result<u64, StorageError> {
        let page_no = self.allocate_page(0)?;
        let frame = self.format_page(page_no, page::PAGE_TYPE_COUNTER, object_id, 0)?;
        let at = page::COUNTER_OFFSET;
        self.pool.page_mut(frame)[at..at + 8].copy_from_slice(&0u64.to_le_bytes());
        self.pool.unpin(frame);
        self.log_system_image(page_no)?;
        Ok(page_no)
    }

    /// Adds `delta` rows to a table's counter as a transactional page op: the
    /// undo record carries the inverse delta, so statement savepoints, txn
    /// rollback and crash recovery keep the count exactly consistent with the
    /// committed rows.
    pub fn counter_add(
        &mut self,
        txn: &mut TxnLink,
        page: u64,
        delta: i64,
    ) -> Result<(), StorageError> {
        if delta == 0 {
            return Ok(());
        }
        self.apply_op(
            LogMode::Txn(
                txn,
                PageOpUndo::CounterAdd {
                    page,
                    delta: -delta,
                },
            ),
            PageOpRedo::CounterAdd { page, delta },
        )?;
        Ok(())
    }

    /// Reads a counter page's row count.
    pub fn counter_read(&mut self, page: u64) -> Result<u64, StorageError> {
        let frame = self.fetch(page)?;
        let at = page::COUNTER_OFFSET;
        let count = u64::from_le_bytes(
            self.pool.page(frame)[at..at + 8]
                .try_into()
                .expect("8 bytes"),
        );
        self.pool.unpin(frame);
        Ok(count)
    }

    /// Logs the current full image of a page as a system record (used for
    /// freshly formatted pages whose creation would not be idempotent as
    /// per-op records). On append failure the page stays an orphan (never
    /// referenced), which is safe.
    pub fn log_system_image(&mut self, page_no: u64) -> Result<u64, StorageError> {
        let frame = self.fetch(page_no)?;
        let record = RelRecord::page_image(0, 0, page_no, self.pool.page(frame), &PageOpUndo::None);
        let lsn = match self.append(&record, false) {
            Ok(lsn) => lsn,
            Err(err) => {
                self.pool.unpin(frame);
                return Err(err);
            }
        };
        self.dpt.entry(page_no).or_insert(lsn);
        set_frame_lsn(self.pool.page_mut(frame), lsn);
        self.pool.unpin(frame);
        Ok(lsn)
    }

    /// Logs one ATOMIC record holding the full images of every page of a
    /// structure change (B+ tree split). The caller keeps all frames pinned
    /// from the first mutation until this returns, so no steal can write
    /// unlogged state; a crash either recovers the whole change (one WAL
    /// entry) or none of it. Frames stay pinned on return (caller unpins).
    pub fn log_system_images(&mut self, pages: &[(u64, FrameId)]) -> Result<u64, StorageError> {
        let record = {
            let images: Vec<(u64, &[u8])> = pages
                .iter()
                .map(|(page_no, frame)| (*page_no, self.pool.page(*frame)))
                .collect();
            RelRecord::page_images(&images)
        };
        let lsn = self.append(&record, false)?;
        for (page_no, frame) in pages {
            self.dpt.entry(*page_no).or_insert(lsn);
            set_frame_lsn(self.pool.page_mut(*frame), lsn);
        }
        Ok(lsn)
    }

    /// Formats a brand-new page (tree or heap) in the pool; the caller logs
    /// it via [`RelCtx::log_system_image`] once its content is complete.
    pub fn format_page(
        &mut self,
        page_no: u64,
        page_type: u16,
        object_id: u32,
        level: u16,
    ) -> Result<FrameId, StorageError> {
        let frame = self.fetch_zeroed(page_no)?;
        let bytes = self.pool.page_mut(frame);
        SlottedPage::format(bytes, level);
        page::write_header(
            bytes,
            &PageHeader {
                page_lsn: 0,
                page_type,
                flags: 0,
                object_id,
                page_no,
            },
        );
        Ok(frame)
    }
}

fn set_frame_lsn(page_bytes: &mut [u8], lsn: u64) {
    let mut header = page::read_header(page_bytes);
    header.page_lsn = lsn;
    page::write_header(page_bytes, &header);
}

/// Applies a physiological redo op to a page. Used identically by normal
/// operation and recovery redo, which is what makes redo exact replay.
pub(crate) fn apply_redo_to_page(
    page_bytes: &mut [u8],
    redo: &PageOpRedo,
) -> Result<(), StorageError> {
    let full =
        |what: &str| StorageError::InvalidFile(format!("page full applying {what} (logging bug)"));
    let mut page = SlottedPage::new(page_bytes);
    match redo {
        PageOpRedo::InsertAt { index, bytes, .. } => page
            .insert_at(*index as usize, bytes)
            .map_err(|_| full("InsertAt")),
        PageOpRedo::RemoveAt { index, .. } => {
            page.remove_at(*index as usize);
            Ok(())
        }
        PageOpRedo::UpdateAt { index, bytes, .. } => page
            .update_at(*index as usize, bytes)
            .map_err(|_| full("UpdateAt")),
        PageOpRedo::HeapInsert { slot, bytes, .. } => page
            .insert_stable_at(*slot as usize, bytes)
            .map_err(|_| full("HeapInsert")),
        PageOpRedo::HeapDelete { slot, .. } => {
            page.delete_stable(*slot as usize);
            Ok(())
        }
        PageOpRedo::HeapUpdate { slot, bytes, .. } => page
            .update_stable(*slot as usize, bytes)
            .map_err(|_| full("HeapUpdate")),
        PageOpRedo::SetNextPage { next, .. } => {
            page.set_next_page(*next);
            Ok(())
        }
        PageOpRedo::CounterAdd { delta, .. } => {
            let _ = page;
            let at = crate::relstore::page::COUNTER_OFFSET;
            let count = u64::from_le_bytes(page_bytes[at..at + 8].try_into().expect("8 bytes"));
            // Wrapping, deliberately: a delta the logic never produces (an
            // undo without its op, an op without its undo) must not turn into
            // a panic inside redo — the count is a statistic, and the
            // surrounding ARIES discipline is what keeps it exact.
            let count = count.wrapping_add_signed(*delta);
            page_bytes[at..at + 8].copy_from_slice(&count.to_le_bytes());
            Ok(())
        }
    }
}

/// A whole blob page as it goes to disk: header (stamped with `lsn`), the
/// body bytes, zero padding, sealed by `codec`. Shared by the write path and
/// redo so both produce identical pages.
pub(crate) fn blob_page_image(
    codec: &crate::encryption::PageCodec,
    lsn: u64,
    object_id: u32,
    page_no: u64,
    chunk: &[u8],
) -> AlignedPageBuf {
    let mut plain = [0u8; crate::storage_layout::PAGE_SIZE];
    let bytes = plain.as_mut_slice();
    page::write_header(
        bytes,
        &PageHeader {
            page_lsn: lsn,
            page_type: page::PAGE_TYPE_BLOB,
            flags: 0,
            object_id,
            page_no,
        },
    );
    bytes[page::PAGE_HEADER_SIZE..page::PAGE_HEADER_SIZE + chunk.len()].copy_from_slice(chunk);
    let mut image = AlignedPageBuf::new();
    codec.seal(page_no, &plain, image.as_mut_slice());
    image
}
