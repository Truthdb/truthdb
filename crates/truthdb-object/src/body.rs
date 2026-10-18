//! Object bodies: immutable byte strings written in segments of up to
//! [`SEGMENT_BYTES`], each segment one transaction that allocates its pages,
//! writes them (one `BLOB_PAGES` record) and records the segment. Nothing holds
//! a transaction open across a whole body, so a large upload never pins the
//! log. A body is *staging* until sealed; a sealed body is reference-counted
//! and goes to the garbage queue when its last reference is dropped.

use md5::{Digest, Md5};
use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::{OpMode, RelCtx};
use truthdb_core::relstore::page::{BLOB_PAGE_DATA, BLOB_RECORD_MAX_PAGES};
use truthdb_core::storage::{Storage, StorageError};

use crate::codec::{self, BodyRow, BodyState, SegmentRow};
use crate::{
    BODIES_TREE, BODY_PAGES_OBJECT, GC_TREE, ObjFile, SEGMENTS_TREE, STAGING_TREE, next_body_id,
};

/// Bytes per segment: one blob record's worth of pages. Every segment but a
/// body's last is exactly this long, so an offset maps straight to a segment.
pub const SEGMENT_BYTES: usize = BLOB_RECORD_MAX_PAGES * BLOB_PAGE_DATA;

/// A sealed body's identity and digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyInfo {
    pub body: u64,
    pub len: u64,
    pub md5: [u8; 16],
}

fn invalid(message: String) -> StorageError {
    StorageError::InvalidConfig(message)
}

impl ObjFile<'_> {
    /// Starts a staging body and returns its id.
    pub fn begin_body(&mut self, now_ms: u64) -> Result<u64, StorageError> {
        let meta = self.meta()?;
        let bodies = self.tree(BODIES_TREE)?;
        let staging = self.tree(STAGING_TREE)?;
        self.in_txn(|ctx, txn| {
            let body = next_body_id(ctx, txn, &meta)?;
            let row = BodyRow {
                state: BodyState::Staging,
                refcount: 0,
                len: 0,
                segments: 0,
                created_ms: now_ms,
                md5: [0; 16],
            };
            bodies.insert_unique(
                ctx,
                &mut OpMode::Txn(txn),
                &codec::body_key(body),
                &row.encode(),
            )?;
            staging.insert_unique(
                ctx,
                &mut OpMode::Txn(txn),
                &codec::timed_key(now_ms, body),
                &[],
            )?;
            Ok(body)
        })
    }

    /// Appends the next segment (`data`, at most [`SEGMENT_BYTES`]) to a staging
    /// body. Every segment but the last must be exactly `SEGMENT_BYTES`.
    pub fn append_segment(&mut self, body: u64, data: &[u8]) -> Result<(), StorageError> {
        if data.is_empty() || data.len() > SEGMENT_BYTES {
            return Err(invalid(format!(
                "a body segment is 1..={SEGMENT_BYTES} bytes, got {}",
                data.len()
            )));
        }
        let bodies = self.tree(BODIES_TREE)?;
        let segments = self.tree(SEGMENTS_TREE)?;
        self.in_txn(|ctx, txn| {
            let mut row = load_body(ctx, &bodies, body)?;
            if row.state != BodyState::Staging {
                return Err(invalid(format!("body {body} is sealed")));
            }
            if row.len != row.segments * SEGMENT_BYTES as u64 {
                return Err(invalid(format!(
                    "body {body} already has its short last segment"
                )));
            }
            let pages = data.len().div_ceil(BLOB_PAGE_DATA) as u64;
            let start_page = ctx.allocate_extent_in_txn(txn, pages)?;
            let lsn = ctx.write_blob_pages(txn, BODY_PAGES_OBJECT, start_page, data)?;
            let segment = SegmentRow {
                start_page,
                pages: pages as u32,
                len: data.len() as u32,
                lsn,
            };
            segments.insert_unique(
                ctx,
                &mut OpMode::Txn(txn),
                &codec::segment_key(body, row.segments),
                &segment.encode(),
            )?;
            row.len += data.len() as u64;
            row.segments += 1;
            bodies.update(
                ctx,
                &mut OpMode::Txn(txn),
                &codec::body_key(body),
                &row.encode(),
            )?;
            Ok(())
        })
    }

    /// Seals a staging body with its digest and one reference.
    pub fn seal_body(&mut self, body: u64, md5: [u8; 16]) -> Result<BodyInfo, StorageError> {
        let bodies = self.tree(BODIES_TREE)?;
        let staging = self.tree(STAGING_TREE)?;
        self.in_txn(|ctx, txn| {
            let mut row = load_body(ctx, &bodies, body)?;
            if row.state != BodyState::Staging {
                return Err(invalid(format!("body {body} is already sealed")));
            }
            staging.delete(
                ctx,
                &mut OpMode::Txn(txn),
                &codec::timed_key(row.created_ms, body),
            )?;
            row.state = BodyState::Sealed;
            row.refcount = 1;
            row.md5 = md5;
            bodies.update(
                ctx,
                &mut OpMode::Txn(txn),
                &codec::body_key(body),
                &row.encode(),
            )?;
            Ok(BodyInfo {
                body,
                len: row.len,
                md5,
            })
        })
    }

    /// The sealed body's length and digest.
    pub fn body_info(&mut self, body: u64) -> Result<BodyInfo, StorageError> {
        let bodies = self.tree(BODIES_TREE)?;
        let mut ctx = self.core.page_ctx(0);
        let row = load_body(&mut ctx, &bodies, body)?;
        if row.state != BodyState::Sealed {
            return Err(invalid(format!("body {body} is still being written")));
        }
        Ok(BodyInfo {
            body,
            len: row.len,
            md5: row.md5,
        })
    }

    /// Reads `out.len()` bytes of a sealed body from `offset`. Each page's
    /// header must name the body pages and carry the LSN of the record that
    /// wrote its segment: anything else is a page reused since.
    pub fn read_body(
        &mut self,
        body: u64,
        offset: u64,
        out: &mut [u8],
    ) -> Result<(), StorageError> {
        let bodies = self.tree(BODIES_TREE)?;
        let segments = self.tree(SEGMENTS_TREE)?;
        let mut ctx = self.core.page_ctx(0);
        let row = load_body(&mut ctx, &bodies, body)?;
        if row.state != BodyState::Sealed {
            return Err(invalid(format!("body {body} is still being written")));
        }
        if offset
            .checked_add(out.len() as u64)
            .is_none_or(|end| end > row.len)
        {
            return Err(invalid(format!(
                "read of {} bytes at {offset} is past the end of body {body} ({} bytes)",
                out.len(),
                row.len
            )));
        }
        let mut page = vec![0u8; BLOB_PAGE_DATA];
        let mut done = 0usize;
        while done < out.len() {
            let at = offset + done as u64;
            let index = at / SEGMENT_BYTES as u64;
            let in_segment = (at % SEGMENT_BYTES as u64) as usize;
            let segment = segments
                .get(&mut ctx, &codec::segment_key(body, index))?
                .ok_or_else(|| {
                    StorageError::InvalidFile(format!("body {body} lost segment {index}"))
                })?;
            let segment = SegmentRow::decode(&segment)?;
            let page_no = segment.start_page + (in_segment / BLOB_PAGE_DATA) as u64;
            let header = ctx.read_blob_page(page_no, &mut page)?;
            if header.object_id != BODY_PAGES_OBJECT || header.page_lsn != segment.lsn {
                return Err(StorageError::InvalidFile(format!(
                    "page {page_no} of body {body} was overwritten"
                )));
            }
            let in_page = in_segment % BLOB_PAGE_DATA;
            let take = (BLOB_PAGE_DATA - in_page).min(out.len() - done);
            out[done..done + take].copy_from_slice(&page[in_page..in_page + take]);
            done += take;
        }
        Ok(())
    }

    /// Adds a reference to a sealed body (a copy sharing it).
    pub fn add_ref(&mut self, body: u64) -> Result<(), StorageError> {
        let bodies = self.tree(BODIES_TREE)?;
        self.in_txn(|ctx, txn| {
            let mut row = load_body(ctx, &bodies, body)?;
            if row.state != BodyState::Sealed || row.refcount == 0 {
                return Err(invalid(format!("body {body} is not live")));
            }
            row.refcount += 1;
            bodies.update(
                ctx,
                &mut OpMode::Txn(txn),
                &codec::body_key(body),
                &row.encode(),
            )?;
            Ok(())
        })
    }

    /// Drops a reference; the last one queues the body for garbage collection,
    /// due after `grace_ms`.
    pub fn drop_ref(&mut self, body: u64, now_ms: u64, grace_ms: u64) -> Result<(), StorageError> {
        let bodies = self.tree(BODIES_TREE)?;
        let gc = self.tree(GC_TREE)?;
        self.in_txn(|ctx, txn| {
            let mut row = load_body(ctx, &bodies, body)?;
            if row.state != BodyState::Sealed || row.refcount == 0 {
                return Err(invalid(format!("body {body} is not live")));
            }
            row.refcount -= 1;
            bodies.update(
                ctx,
                &mut OpMode::Txn(txn),
                &codec::body_key(body),
                &row.encode(),
            )?;
            if row.refcount == 0 {
                let inserted = gc.insert_unique(
                    ctx,
                    &mut OpMode::Txn(txn),
                    &codec::timed_key(now_ms.saturating_add(grace_ms), body),
                    &[],
                )?;
                debug_assert_eq!(inserted, TreeInsert::Inserted);
            }
            Ok(())
        })
    }

    /// Takes a read lease: garbage collection leaves the body alone until the
    /// matching [`Self::release_lease`].
    pub fn take_lease(&mut self, body: u64) {
        *self.eng.leases.entry(body).or_default() += 1;
    }

    pub fn release_lease(&mut self, body: u64) {
        if let Some(count) = self.eng.leases.get_mut(&body) {
            *count -= 1;
            if *count == 0 {
                self.eng.leases.remove(&body);
            }
        }
    }
}

pub(crate) fn load_body(
    ctx: &mut RelCtx<'_>,
    bodies: &truthdb_core::relstore::btree::BTree,
    body: u64,
) -> Result<BodyRow, StorageError> {
    let row = bodies
        .get(ctx, &codec::body_key(body))?
        .ok_or_else(|| invalid(format!("no body {body}")))?;
    BodyRow::decode(&row)
}

/// Streams a body into the store segment by segment, computing its MD5, and
/// seals it on [`Self::finish`]. Takes the storage lock once per segment.
pub struct BodyWriter {
    body: u64,
    buffer: Vec<u8>,
    md5: Md5,
}

impl BodyWriter {
    pub fn begin(storage: &Storage, now_ms: u64) -> Result<Self, StorageError> {
        let body = ObjFile::new(&mut storage.lock()).begin_body(now_ms)?;
        Ok(BodyWriter {
            body,
            buffer: Vec::with_capacity(SEGMENT_BYTES),
            md5: Md5::new(),
        })
    }

    pub fn body(&self) -> u64 {
        self.body
    }

    pub fn write(&mut self, storage: &Storage, mut bytes: &[u8]) -> Result<(), StorageError> {
        self.md5.update(bytes);
        while !bytes.is_empty() {
            let take = (SEGMENT_BYTES - self.buffer.len()).min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == SEGMENT_BYTES {
                ObjFile::new(&mut storage.lock()).append_segment(self.body, &self.buffer)?;
                self.buffer.clear();
            }
        }
        Ok(())
    }

    /// Writes the last partial segment and seals the body.
    pub fn finish(self, storage: &Storage) -> Result<BodyInfo, StorageError> {
        let mut guard = storage.lock();
        let mut file = ObjFile::new(&mut guard);
        if !self.buffer.is_empty() {
            file.append_segment(self.body, &self.buffer)?;
        }
        file.seal_body(self.body, self.md5.finalize().into())
    }
}
