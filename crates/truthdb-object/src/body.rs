//! Object bodies: immutable byte strings written in segments of up to
//! [`SEGMENT_BYTES`], each segment one transaction that allocates its pages,
//! writes them (one `BLOB_PAGES` record) and records the segment. Nothing holds
//! a transaction open across a whole body, so a large upload never pins the
//! log. A body is *staging* until sealed; a sealed body is reference-counted
//! and goes to the garbage queue when its last reference is dropped. Sealing
//! and reference changes run inside the caller's [`ObjTxn`], so they commit
//! together with whatever row points at the body.

use md5::{Digest, Md5};
use truthdb_core::relstore::btree::{BTree, TreeInsert};
use truthdb_core::relstore::ctx::{OpMode, RelCtx};
use truthdb_core::relstore::page::{BLOB_PAGE_DATA, BLOB_RECORD_MAX_PAGES};
use truthdb_core::storage::{Storage, StorageError};

use crate::codec::{self, BodyRow, BodyState, ChildRow, SegmentRow};
use crate::{BODIES_TREE, BODY_PAGES_OBJECT, ObjFile, ObjTxn, SEGMENTS_TREE, next_body_id};

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

fn no_body(body: u64) -> StorageError {
    invalid(format!("no body {body}"))
}

impl ObjTxn<'_, '_> {
    pub(crate) fn load_body(&mut self, body: u64) -> Result<BodyRow, StorageError> {
        load_body(self.ctx, &self.trees.bodies(), body)
    }

    pub(crate) fn store_body(&mut self, body: u64, row: &BodyRow) -> Result<(), StorageError> {
        let bodies = self.trees.bodies();
        bodies.update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &codec::body_key(body),
            &row.encode(),
        )?;
        Ok(())
    }

    /// Starts a staging body and returns its id.
    pub fn begin_body(&mut self, now_ms: u64) -> Result<u64, StorageError> {
        let body = next_body_id(self)?;
        let row = BodyRow {
            state: BodyState::Staging,
            refcount: 0,
            len: 0,
            segments: 0,
            touched_ms: now_ms,
            md5: [0; 16],
            pack_level: 0,
        };
        let (bodies, staging) = (self.trees.bodies(), self.trees.staging());
        let inserted = bodies.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &codec::body_key(body),
            &row.encode(),
        )?;
        if inserted != TreeInsert::Inserted {
            return Err(StorageError::InvalidFile(format!(
                "body id {body} handed out twice"
            )));
        }
        staging.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &codec::timed_key(now_ms, body),
            &[],
        )?;
        Ok(body)
    }

    /// Appends the next segment (`data`, at most [`SEGMENT_BYTES`]) to a staging
    /// body and marks it touched at `now_ms`, so garbage collection does not
    /// take a slow upload for an abandoned one. Every segment but the last must
    /// be exactly `SEGMENT_BYTES`.
    pub fn append_segment(
        &mut self,
        body: u64,
        data: &[u8],
        now_ms: u64,
    ) -> Result<(), StorageError> {
        if data.is_empty() || data.len() > SEGMENT_BYTES {
            return Err(invalid(format!(
                "a body segment is 1..={SEGMENT_BYTES} bytes, got {}",
                data.len()
            )));
        }
        let mut row = self.load_body(body)?;
        if row.state != BodyState::Staging {
            return Err(invalid(format!("body {body} is sealed")));
        }
        if row.len != row.segments * SEGMENT_BYTES as u64 {
            return Err(invalid(format!(
                "body {body} already has its short last segment"
            )));
        }
        let pages = data.len().div_ceil(BLOB_PAGE_DATA) as u64;
        let start_page = self.ctx.allocate_extent_in_txn(self.txn, pages)?;
        let lsn = self
            .ctx
            .write_blob_pages(self.txn, BODY_PAGES_OBJECT, start_page, data)?;
        let segment = SegmentRow {
            start_page,
            pages: pages as u32,
            len: data.len() as u32,
            lsn,
            zstd: false,
        };
        let (segments, staging) = (self.trees.segments(), self.trees.staging());
        segments.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &codec::segment_key(body, row.segments),
            &segment.encode(),
        )?;
        staging.delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &codec::timed_key(row.touched_ms, body),
        )?;
        staging.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &codec::timed_key(now_ms, body),
            &[],
        )?;
        row.len += data.len() as u64;
        row.segments += 1;
        row.touched_ms = now_ms;
        self.store_body(body, &row)
    }

    /// Seals a staging body with its digest and one reference: the reference
    /// held by whatever row the caller writes in this same transaction.
    pub fn seal_body(&mut self, body: u64, md5: [u8; 16]) -> Result<BodyInfo, StorageError> {
        let mut row = self.load_body(body)?;
        if row.state != BodyState::Staging {
            return Err(invalid(format!("body {body} is already sealed")));
        }
        let staging = self.trees.staging();
        staging.delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &codec::timed_key(row.touched_ms, body),
        )?;
        row.state = BodyState::Sealed;
        row.refcount = 1;
        row.md5 = md5;
        self.store_body(body, &row)?;
        Ok(BodyInfo {
            body,
            len: row.len,
            md5,
        })
    }

    /// Adds a reference to a sealed body (a copy sharing it).
    pub fn add_ref(&mut self, body: u64) -> Result<(), StorageError> {
        let mut row = self.load_body(body)?;
        if !row.state.is_sealed() || row.refcount == 0 {
            return Err(invalid(format!("body {body} is not live")));
        }
        row.refcount += 1;
        self.store_body(body, &row)
    }

    /// Drops a reference; the last one queues the body for garbage collection,
    /// due after `grace_ms`.
    pub fn drop_ref(&mut self, body: u64, now_ms: u64, grace_ms: u64) -> Result<(), StorageError> {
        let mut row = self.load_body(body)?;
        if !row.state.is_sealed() || row.refcount == 0 {
            return Err(invalid(format!("body {body} is not live")));
        }
        row.refcount -= 1;
        self.store_body(body, &row)?;
        if row.refcount == 0 {
            let gc = self.trees.gc();
            gc.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &codec::timed_key(now_ms.saturating_add(grace_ms), body),
                &[],
            )?;
        }
        Ok(())
    }
}

impl ObjTxn<'_, '_> {
    /// Makes a composite body of `children` (sealed bodies, in order, each
    /// handing its one reference to the composite), with `md5` as its digest
    /// and one reference of its own.
    pub(crate) fn compose_body(
        &mut self,
        children: &[(u64, u64)],
        md5: [u8; 16],
        now_ms: u64,
    ) -> Result<u64, StorageError> {
        let body = next_body_id(self)?;
        let segments = self.trees.segments();
        let mut offset = 0u64;
        for (index, (child, len)) in children.iter().enumerate() {
            segments.insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &codec::segment_key(body, index as u64),
                &ChildRow {
                    child: *child,
                    offset,
                    len: *len,
                }
                .encode(),
            )?;
            offset += len;
        }
        let row = BodyRow {
            state: BodyState::Composite,
            refcount: 1,
            len: offset,
            segments: children.len() as u64,
            touched_ms: now_ms,
            md5,
            pack_level: 0,
        };
        let bodies = self.trees.bodies();
        bodies.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &codec::body_key(body),
            &row.encode(),
        )?;
        Ok(body)
    }
}

impl ObjFile<'_> {
    /// Starts a staging body in its own transaction.
    pub fn begin_body(&mut self, now_ms: u64) -> Result<u64, StorageError> {
        self.transaction(|tx| tx.begin_body(now_ms))
    }

    /// Appends one segment in its own transaction.
    pub fn append_segment(
        &mut self,
        body: u64,
        data: &[u8],
        now_ms: u64,
    ) -> Result<(), StorageError> {
        self.transaction(|tx| tx.append_segment(body, data, now_ms))
    }

    /// The sealed body's length and digest.
    pub fn body_info(&mut self, body: u64) -> Result<BodyInfo, StorageError> {
        let bodies = self
            .existing_tree(BODIES_TREE)?
            .ok_or_else(|| no_body(body))?;
        let mut ctx = self.core.page_ctx(0);
        let row = load_body(&mut ctx, &bodies, body)?;
        if !row.state.is_sealed() {
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
        let bodies = self
            .existing_tree(BODIES_TREE)?
            .ok_or_else(|| no_body(body))?;
        let segments = self
            .existing_tree(SEGMENTS_TREE)?
            .ok_or_else(|| no_body(body))?;
        let mut ctx = self.core.page_ctx(0);
        let row = load_body(&mut ctx, &bodies, body)?;
        if !row.state.is_sealed() {
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
        if row.state == BodyState::Composite {
            // Each part is a plain body: read the slice of each that overlaps.
            let mut done = 0usize;
            for child in composite_children(&mut ctx, &segments, body)? {
                let end = offset + out.len() as u64;
                let (start, stop) = (
                    child.offset.max(offset),
                    (child.offset + child.len).min(end),
                );
                if start >= stop {
                    continue;
                }
                let child_row = load_body(&mut ctx, &bodies, child.child)?;
                let span = (stop - start) as usize;
                read_plain(
                    &mut ctx,
                    &segments,
                    child.child,
                    &child_row,
                    start - child.offset,
                    &mut out[done..done + span],
                )?;
                done += span;
            }
            return Ok(());
        }
        read_plain(&mut ctx, &segments, body, &row, offset, out)
    }

    /// A body's bytes as plain bodies: itself, or a composite's parts, as
    /// `(body, offset in the whole, length)`.
    pub fn body_layout(&mut self, body: u64) -> Result<Vec<(u64, u64, u64)>, StorageError> {
        let bodies = self
            .existing_tree(BODIES_TREE)?
            .ok_or_else(|| no_body(body))?;
        let segments = self
            .existing_tree(SEGMENTS_TREE)?
            .ok_or_else(|| no_body(body))?;
        let mut ctx = self.core.page_ctx(0);
        let row = load_body(&mut ctx, &bodies, body)?;
        Ok(if row.state == BodyState::Composite {
            composite_children(&mut ctx, &segments, body)?
                .into_iter()
                .map(|child| (child.child, child.offset, child.len))
                .collect()
        } else {
            vec![(body, 0, row.len)]
        })
    }
}

/// A composite body's children in order.
pub(crate) fn composite_children(
    ctx: &mut RelCtx<'_>,
    segments: &BTree,
    body: u64,
) -> Result<Vec<ChildRow>, StorageError> {
    segments
        .scan_range(
            ctx,
            Some(&codec::segment_key(body, 0)),
            Some(&codec::segment_key(body, u64::MAX)),
        )?
        .into_iter()
        .map(|(_, row)| ChildRow::decode(&row))
        .collect()
}

/// Reads a plain (segmented) body's bytes. Each page's header must name the
/// body pages and carry the LSN of the record that wrote its segment:
/// anything else is a page reused since.
fn read_plain(
    ctx: &mut RelCtx<'_>,
    segments: &BTree,
    body: u64,
    row: &BodyRow,
    offset: u64,
    out: &mut [u8],
) -> Result<(), StorageError> {
    if !row.state.is_sealed() || row.state == BodyState::Composite {
        return Err(StorageError::InvalidFile(format!(
            "body {body} is not a plain sealed body"
        )));
    }
    let mut page = vec![0u8; BLOB_PAGE_DATA];
    let mut done = 0usize;
    while done < out.len() {
        let at = offset + done as u64;
        let index = at / SEGMENT_BYTES as u64;
        let in_segment = (at % SEGMENT_BYTES as u64) as usize;
        let segment = segments
            .get(ctx, &codec::segment_key(body, index))?
            .ok_or_else(|| {
                StorageError::InvalidFile(format!("body {body} lost segment {index}"))
            })?;
        let segment = SegmentRow::decode(&segment)?;
        if segment.zstd {
            // A packed segment is one zstd frame: read it whole, inflate it,
            // and copy out what is wanted.
            let plain_len = (row.len - index * SEGMENT_BYTES as u64).min(SEGMENT_BYTES as u64);
            let plain = read_frame(ctx, body, &segment, plain_len as usize)?;
            let take = (plain.len() - in_segment).min(out.len() - done);
            out[done..done + take].copy_from_slice(&plain[in_segment..in_segment + take]);
            done += take;
            continue;
        }
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

/// A segment's stored bytes (its `len` bytes over its pages), each page
/// checked as [`read_plain`] checks one.
pub(crate) fn read_segment(
    ctx: &mut RelCtx<'_>,
    body: u64,
    segment: &SegmentRow,
) -> Result<Vec<u8>, StorageError> {
    let mut stored = vec![0u8; segment.len as usize];
    let mut page = vec![0u8; BLOB_PAGE_DATA];
    for (index, chunk) in stored.chunks_mut(BLOB_PAGE_DATA).enumerate() {
        let page_no = segment.start_page + index as u64;
        let header = ctx.read_blob_page(page_no, &mut page)?;
        if header.object_id != BODY_PAGES_OBJECT || header.page_lsn != segment.lsn {
            return Err(StorageError::InvalidFile(format!(
                "page {page_no} of body {body} was overwritten"
            )));
        }
        chunk.copy_from_slice(&page[..chunk.len()]);
    }
    Ok(stored)
}

/// A packed segment's bytes: its zstd frame, inflated to `plain_len`.
pub(crate) fn read_frame(
    ctx: &mut RelCtx<'_>,
    body: u64,
    segment: &SegmentRow,
    plain_len: usize,
) -> Result<Vec<u8>, StorageError> {
    let frame = read_segment(ctx, body, segment)?;
    let plain = zstd::bulk::decompress(&frame, plain_len).map_err(|err| {
        StorageError::InvalidFile(format!("a packed segment of body {body}: {err}"))
    })?;
    if plain.len() != plain_len {
        return Err(StorageError::InvalidFile(format!(
            "a packed segment of body {body} holds {} bytes, not {plain_len}",
            plain.len()
        )));
    }
    Ok(plain)
}

pub(crate) fn load_body(
    ctx: &mut RelCtx<'_>,
    bodies: &BTree,
    body: u64,
) -> Result<BodyRow, StorageError> {
    let row = bodies
        .get(ctx, &codec::body_key(body))?
        .ok_or_else(|| no_body(body))?;
    BodyRow::decode(&row)
}

/// A reader's hold on a body: garbage collection leaves the body alone until
/// the lease drops. Taking and dropping it lock the storage, so neither may
/// happen while the caller already holds the lock.
pub struct BodyLease<'s> {
    storage: &'s Storage,
    body: u64,
}

impl<'s> BodyLease<'s> {
    /// Leases a live (sealed, referenced) body (see [`ObjFile::take_lease`]).
    pub fn take(storage: &'s Storage, body: u64) -> Result<Self, StorageError> {
        ObjFile::new(&mut storage.lock()).take_lease(body)?;
        Ok(BodyLease { storage, body })
    }
}

impl Drop for BodyLease<'_> {
    fn drop(&mut self) {
        ObjFile::new(&mut self.storage.lock()).release_lease(self.body);
    }
}

impl ObjFile<'_> {
    /// Leases a live (sealed, referenced) body: garbage collection leaves it
    /// alone until the matching [`Self::release_lease`]. The body is checked
    /// under the same lock hold that takes the lease, so it cannot be
    /// collected between the check and the lease.
    pub fn take_lease(&mut self, body: u64) -> Result<(), StorageError> {
        let bodies = self
            .existing_tree(BODIES_TREE)?
            .ok_or_else(|| no_body(body))?;
        let row = load_body(&mut self.core.page_ctx(0), &bodies, body)?;
        if !row.state.is_sealed() || row.refcount == 0 {
            return Err(invalid(format!("body {body} is not live")));
        }
        *self.eng.leases.entry(body).or_default() += 1;
        Ok(())
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

/// Streams a body into the store segment by segment, computing its MD5. Each
/// segment commits on its own; the body is sealed by [`SealedBody::seal_in`]
/// inside the transaction that records the row referencing it. After any
/// error the writer refuses further use: the body is abandoned, and garbage
/// collection reclaims it.
pub struct BodyWriter {
    body: u64,
    buffer: Vec<u8>,
    md5: Md5,
    failed: bool,
}

impl BodyWriter {
    pub fn begin(storage: &Storage, now_ms: u64) -> Result<Self, StorageError> {
        let body = ObjFile::new(&mut storage.lock()).begin_body(now_ms)?;
        Ok(BodyWriter {
            body,
            buffer: Vec::with_capacity(SEGMENT_BYTES),
            md5: Md5::new(),
            failed: false,
        })
    }

    pub fn body(&self) -> u64 {
        self.body
    }

    fn check(&self) -> Result<(), StorageError> {
        if self.failed {
            return Err(invalid(format!(
                "the writer of body {} failed earlier; start a new body",
                self.body
            )));
        }
        Ok(())
    }

    pub fn write(
        &mut self,
        storage: &Storage,
        mut bytes: &[u8],
        now_ms: u64,
    ) -> Result<(), StorageError> {
        self.check()?;
        self.md5.update(bytes);
        while !bytes.is_empty() {
            let take = (SEGMENT_BYTES - self.buffer.len()).min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.buffer.len() == SEGMENT_BYTES {
                let written = ObjFile::new(&mut storage.lock()).append_segment(
                    self.body,
                    &self.buffer,
                    now_ms,
                );
                self.failed = written.is_err();
                written?;
                self.buffer.clear();
            }
        }
        Ok(())
    }

    /// Writes the last partial segment (its own transaction) and returns the
    /// body ready to seal.
    pub fn finish(self, storage: &Storage, now_ms: u64) -> Result<SealedBody, StorageError> {
        self.check()?;
        if !self.buffer.is_empty() {
            ObjFile::new(&mut storage.lock()).append_segment(self.body, &self.buffer, now_ms)?;
        }
        Ok(SealedBody {
            body: self.body,
            md5: self.md5.finalize().into(),
            size: None,
        })
    }
}

/// A body whose bytes are all written, to be sealed inside the transaction
/// that records the row referencing it.
pub struct SealedBody {
    pub body: u64,
    pub md5: [u8; 16],
    /// The object's (or part's) length when it is not the body's: a body
    /// encrypted with a customer-provided key stores more than it holds.
    pub size: Option<u64>,
}

impl SealedBody {
    pub fn seal_in(&self, tx: &mut ObjTxn<'_, '_>) -> Result<BodyInfo, StorageError> {
        tx.seal_body(self.body, self.md5)
    }
}
