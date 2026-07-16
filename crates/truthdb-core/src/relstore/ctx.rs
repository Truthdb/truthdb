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
use crate::wal::records::{PageOpRedo, PageOpUndo, RelRecord};

const REL_WAL_ENTRY_VERSION: u16 = 1;

/// Raw page I/O + WAL watermark for the buffer pool, over the data region.
pub(crate) struct PoolIo<'a> {
    pub file: &'a mut DirectFile,
    pub wal: &'a mut WalWriter,
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
}

/// An active (statement-scoped, autocommit) transaction: the WAL chain tail
/// and an in-memory undo log so statement rollback never re-reads the ring.
pub(crate) struct TxnLink {
    pub txn_id: u64,
    pub last_lsn: u64,
    pub undo_log: Vec<(u64, PageOpUndo)>,
    /// Row versions staged by the running statement (Stage 13), published to
    /// the version store when the statement succeeds and discarded when it
    /// rolls back. Empty unless a versioned isolation is enabled.
    pub pending_versions: Vec<crate::relstore::version::PendingVersion>,
    /// Versions this transaction has published, so a rollback (full or to a
    /// savepoint) can reverse the publications exactly.
    pub published_versions: Vec<crate::relstore::version::PublishRecord>,
}

/// A marker in a live transaction's log to which a later partial rollback can
/// return, undoing only the work done since (statement-level atomicity). Captures
/// the undo-log length and the WAL chain tail at capture time.
#[derive(Clone, Copy)]
pub(crate) struct Savepoint {
    pub undo_len: usize,
    pub last_lsn: u64,
    /// Published-version count at capture, so a partial rollback unpublishes
    /// exactly the versions of the statements it undoes.
    pub published_len: usize,
}

impl TxnLink {
    /// Captures a savepoint at the current point in this transaction.
    pub fn savepoint(&self) -> Savepoint {
        Savepoint {
            undo_len: self.undo_log.len(),
            last_lsn: self.last_lsn,
            published_len: self.published_versions.len(),
        }
    }
}

/// How an access-method operation logs its user-visible page op: as a
/// forward transaction record (with undo) or as a compensation record
/// during rollback/recovery-undo. Structural changes (splits, new pages)
/// are always system records regardless.
pub(crate) enum OpMode<'t> {
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
pub(crate) enum LogMode<'t> {
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

#[cfg(test)]
thread_local! {
    /// Test fault injection: `Some(n)` makes the (n+1)-th apply_op fail
    /// after syncing the log — prior appends stay durable, simulating a
    /// crash at that exact point (including inside recovery undo).
    pub(crate) static FAIL_APPLY_OPS_AFTER: std::cell::Cell<Option<u32>> =
        const { std::cell::Cell::new(None) };
}

pub(crate) struct RelCtx<'a> {
    pub pool: &'a mut BufferPool,
    pub io: PoolIo<'a>,
    pub allocator: &'a mut PageAllocator,
    /// Pages dirtied since the last checkpoint -> LSN of their first change
    /// (the FPI). Drives FPI-on-first-touch.
    pub dpt: &'a mut HashMap<u64, u64>,
    /// True on undo paths (rollback, recovery): appends may use the WAL's
    /// compensation reserve, which forward statements must not touch.
    pub use_reserve: bool,
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
        let _ = txn_id;
        Ok(page_no)
    }

    pub fn append(&mut self, record: &RelRecord, sync: bool) -> Result<u64, StorageError> {
        self.io.wal.append_entry_reserve(
            WAL_ENTRY_TYPE_REL,
            REL_WAL_ENTRY_VERSION,
            0,
            &record.encode(),
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
            pending_versions: Vec::new(),
            published_versions: Vec::new(),
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
        let commit_lsn = self.append(&RelRecord::txn_commit(txn.txn_id, txn.last_lsn), false)?;
        let _ = self.append(&RelRecord::txn_end(txn.txn_id, commit_lsn), false);
        Ok(commit_lsn)
    }

    /// Applies a physiological op to its page and logs it. Callers must have
    /// verified the op fits (page splits happen before this). If the log
    /// append fails, the in-memory page is restored from its pre-image: a
    /// mutation without a durable record must never survive in the pool.
    pub fn apply_op(&mut self, mode: LogMode<'_>, redo: PageOpRedo) -> Result<u64, StorageError> {
        #[cfg(test)]
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
