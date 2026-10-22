//! Packing: a body a cold storage class holds is compressed in place, one
//! segment at a time. Each segment becomes one zstd frame (a read inflates
//! the frame it needs, see `body`), so every version sharing the body reads
//! it as before, whatever its class. A body to pack waits in the pack queue
//! with the level its class asks for and the next segment to do; a step
//! reads that segment under the storage lock, compresses it without the
//! lock, and applies the frame in a transaction of its own only when nothing
//! changed meanwhile.
//!
//! A body under a customer-provided key is not packed (its bytes are
//! ciphertext and would not shrink), nor a segment whose frame would not be
//! smaller than its bytes. In a database encrypted at rest, a packed
//! segment's page count still tells how well its bytes compress: what a cold
//! class costs in disclosure, as compression before encryption always does.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::relstore::page::BLOB_PAGE_DATA;
use truthdb_core::storage::StorageError;

use crate::codec::{self, BodyRow, BodyState, SegmentRow};
use crate::objects::StorageClass;
use crate::{BODIES_TREE, BODY_PAGES_OBJECT, ObjFile, ObjTxn, PACK_QUEUE_TREE, SEGMENTS_TREE};

impl StorageClass {
    /// The zstd level a body of this class is packed at (0: not packed;
    /// INTELLIGENT_TIERING and REDUCED_REDUNDANCY are names only).
    pub fn pack_level(self) -> u8 {
        match self {
            StorageClass::StandardIa | StorageClass::OnezoneIa | StorageClass::GlacierIr => 3,
            StorageClass::Glacier => 9,
            StorageClass::DeepArchive => 19,
            StorageClass::Standard
            | StorageClass::ReducedRedundancy
            | StorageClass::IntelligentTiering => 0,
        }
    }
}

/// `[level u8][next segment u64]`.
fn encode_queued(level: u8, next: u64) -> [u8; 9] {
    let mut row = [0u8; 9];
    row[0] = level;
    row[1..].copy_from_slice(&next.to_le_bytes());
    row
}

fn decode_queued(row: &[u8]) -> Result<(u8, u64), StorageError> {
    if row.len() != 9 {
        return Err(StorageError::InvalidFile(
            "corrupt pack queue row".to_string(),
        ));
    }
    Ok((row[0], u64::from_le_bytes(row[1..].try_into().unwrap())))
}

/// What the pack queue's head asks for next.
#[derive(Debug)]
pub enum PackStep {
    /// A segment to compress (outside the lock), then apply.
    Segment(PackWork),
    /// The body is done (every segment packed) or gone: finish its entry.
    Finish { body: u64, level: u8 },
    /// The body's next segment is damaged: its entry is to be dropped (see
    /// [`ObjTxn::drop_pack`]), the body left as it is, so the bodies after
    /// it are packed still.
    Unreadable {
        body: u64,
        level: u8,
        index: u64,
        error: StorageError,
    },
}

/// One segment to pack: its bytes, read under the lock, and what it was then.
#[derive(Debug)]
pub struct PackWork {
    body: u64,
    index: u64,
    level: u8,
    segment: SegmentRow,
    plain: Vec<u8>,
}

impl PackWork {
    /// The segment's zstd frame, when one is smaller than its bytes.
    pub fn compress(&self) -> Option<Vec<u8>> {
        zstd::bulk::compress(&self.plain, i32::from(self.level))
            .ok()
            .filter(|frame| frame.len() < self.plain.len())
    }
}

impl ObjTxn<'_, '_> {
    /// Queues `body` (a composite's parts) to be packed at `level`, unless
    /// it is packed (or queued) at that level or higher already.
    pub(crate) fn queue_pack(&mut self, body: u64, level: u8) -> Result<(), StorageError> {
        if level == 0 {
            return Ok(());
        }
        let row = self.load_body(body)?;
        if row.state == BodyState::Composite {
            let children = crate::body::composite_children(self.ctx, &self.trees.segments(), body)?;
            for child in children {
                self.queue_pack(child.child, level)?;
            }
            return Ok(());
        }
        if row.pack_level >= level {
            return Ok(());
        }
        let queue = self.trees.pack_queue();
        let key = codec::body_key(body);
        let mode = &mut OpMode::Txn(self.txn);
        match queue.get(self.ctx, &key)? {
            Some(queued) if decode_queued(&queued)?.0 >= level => {}
            Some(_) => {
                // A colder class asks for more: start over at its level.
                queue.update(self.ctx, mode, &key, &encode_queued(level, 0))?;
            }
            None => {
                queue.insert_unique(self.ctx, mode, &key, &encode_queued(level, 0))?;
            }
        }
        Ok(())
    }

    /// Applies a frame [`PackWork::compress`] made (`None`: the segment stays
    /// as it is), and moves the body's entry to its next segment: only when
    /// the entry, the body and the segment are as they were when the work was
    /// read (anything else is left for the next step to read anew).
    pub fn apply_pack(
        &mut self,
        work: &PackWork,
        frame: Option<Vec<u8>>,
    ) -> Result<(), StorageError> {
        let queue = self.trees.pack_queue();
        let key = codec::body_key(work.body);
        let Some(queued) = queue.get(self.ctx, &key)? else {
            return Ok(());
        };
        if decode_queued(&queued)? != (work.level, work.index) {
            return Ok(());
        }
        let Some(row) = self.live_body(work.body)? else {
            return Ok(());
        };
        let segments = self.trees.segments();
        let segment_key = codec::segment_key(work.body, work.index);
        let current = segments.get(self.ctx, &segment_key)?;
        if current.as_deref() != Some(&work.segment.encode()[..]) {
            return Ok(());
        }
        if let Some(frame) = frame {
            let pages = frame.len().div_ceil(BLOB_PAGE_DATA) as u64;
            let start_page = self.ctx.allocate_extent_in_txn(self.txn, pages)?;
            let lsn = self
                .ctx
                .write_blob_pages(self.txn, BODY_PAGES_OBJECT, start_page, &frame)?;
            let packed = SegmentRow {
                start_page,
                pages: pages as u32,
                len: frame.len() as u32,
                lsn,
                zstd: true,
            };
            segments.update(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &segment_key,
                &packed.encode(),
            )?;
            self.ctx.free_extent_in_txn(
                self.txn,
                work.segment.start_page,
                work.segment.pages as u64,
            )?;
        }
        let next = work.index + 1;
        if next >= row.segments {
            return self.finish_pack(work.body, work.level);
        }
        queue.update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key,
            &encode_queued(work.level, next),
        )?;
        Ok(())
    }

    /// Ends `body`'s entry: a body still there is now packed at `level`.
    pub fn finish_pack(&mut self, body: u64, level: u8) -> Result<(), StorageError> {
        self.trees.pack_queue().delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &codec::body_key(body),
        )?;
        if let Some(mut row) = self.live_body(body)?
            && row.pack_level < level
        {
            row.pack_level = level;
            self.store_body(body, &row)?;
        }
        Ok(())
    }

    /// Drops `body`'s entry after [`PackStep::Unreadable`], only while it is
    /// still at the `level` and segment `index` the step read (a colder class
    /// queued meanwhile starts over instead).
    pub fn drop_pack(&mut self, body: u64, level: u8, index: u64) -> Result<(), StorageError> {
        let queue = self.trees.pack_queue();
        let key = codec::body_key(body);
        if let Some(queued) = queue.get(self.ctx, &key)?
            && decode_queued(&queued)? == (level, index)
        {
            queue.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        Ok(())
    }

    /// The body's row while some version holds it (none once it is gone or
    /// waiting for collection).
    fn live_body(&mut self, body: u64) -> Result<Option<BodyRow>, StorageError> {
        let row = self
            .trees
            .bodies()
            .get(self.ctx, &codec::body_key(body))?
            .map(|row| BodyRow::decode(&row))
            .transpose()?;
        Ok(row.filter(|row| row.state.is_sealed() && row.refcount > 0))
    }
}

impl ObjFile<'_> {
    /// What the pack queue's head asks for next (`None`: nothing queued).
    pub fn next_pack(&mut self) -> Result<Option<PackStep>, StorageError> {
        let Some(queue) = self.existing_tree(PACK_QUEUE_TREE)? else {
            return Ok(None);
        };
        let (bodies, segments) = (
            self.existing_tree(BODIES_TREE)?,
            self.existing_tree(SEGMENTS_TREE)?,
        );
        let mut ctx = self.core.page_ctx(0);
        let Some((key, queued)) = queue.scan_range_limit(&mut ctx, None, None, 1)?.pop() else {
            return Ok(None);
        };
        let body = u64::from_be_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| StorageError::InvalidFile("corrupt pack queue key".to_string()))?,
        );
        let (level, next) = decode_queued(&queued)?;
        let (Some(bodies), Some(segments)) = (bodies, segments) else {
            return Ok(Some(PackStep::Finish { body, level }));
        };
        let row = bodies
            .get(&mut ctx, &codec::body_key(body))?
            .map(|row| BodyRow::decode(&row))
            .transpose()?
            .filter(|row| row.state == BodyState::Sealed && row.refcount > 0);
        let Some(row) = row else {
            return Ok(Some(PackStep::Finish { body, level }));
        };
        if next >= row.segments {
            return Ok(Some(PackStep::Finish { body, level }));
        }
        let mut read = || -> Result<(SegmentRow, Vec<u8>), StorageError> {
            let segment = segments
                .get(&mut ctx, &codec::segment_key(body, next))?
                .ok_or_else(|| {
                    StorageError::InvalidFile(format!("body {body} lost segment {next}"))
                })?;
            let segment = SegmentRow::decode(&segment)?;
            let plain_len = (row.len - next * crate::SEGMENT_BYTES as u64)
                .min(crate::SEGMENT_BYTES as u64) as usize;
            let plain = if segment.zstd {
                crate::body::read_frame(&mut ctx, body, &segment, plain_len)?
            } else {
                crate::body::read_segment(&mut ctx, body, &segment)?
            };
            Ok((segment, plain))
        };
        Ok(Some(match read() {
            Ok((segment, plain)) => PackStep::Segment(PackWork {
                body,
                index: next,
                level,
                segment,
                plain,
            }),
            // Damage; any other error (I/O) may pass, so the step fails
            // and is retried.
            Err(error @ StorageError::InvalidFile(_)) => PackStep::Unreadable {
                body,
                level,
                index: next,
                error,
            },
            Err(error) => return Err(error),
        }))
    }
}
