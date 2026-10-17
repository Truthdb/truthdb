//! Overflow page chains (Stage 14): storage for (MAX)-class values above the
//! in-row threshold.
//!
//! A chain is a singly-linked run of [`page::PAGE_TYPE_OVERFLOW`] pages, each
//! `[32B page header | next_page u64 | len u16 | data]`, written once and
//! **immutable** thereafter: every page is logged as a full system image (like
//! B+ tree split pages), so redo replays it wholesale and nothing ever undoes
//! it. A statement that fails after writing a chain leaks its pages — the
//! same posture as dropped tables and indexes — and superseded chains (the
//! old value of an UPDATE, a DELETEd row's value) leak too. That immutability
//! is load-bearing for the version store: a row image holding an overflow
//! REFERENCE stays readable for as long as any snapshot can resolve it,
//! because the chain it points at is never freed or rewritten.
//!
//! The row carries `[total_len u64 | first_page u64]` behind the codec's MAX
//! tag byte (see `row.rs`); values at or below the inline threshold skip all
//! of this and live in the row.

use crate::relstore::ctx::RelCtx;
use crate::relstore::page::{self, PAGE_HEADER_SIZE};
use crate::storage::StorageError;
use crate::storage_layout::PAGE_SIZE;

/// (MAX) values longer than this many bytes go to an overflow chain; at or
/// below it they stay in the row (behind the same tag byte).
pub const OVERFLOW_INLINE_MAX: usize = 256;

const NEXT_OFFSET: usize = PAGE_HEADER_SIZE; // u64
const LEN_OFFSET: usize = NEXT_OFFSET + 8; // u16
const DATA_OFFSET: usize = LEN_OFFSET + 2;
/// Payload bytes per chain page.
pub const OVERFLOW_PAGE_DATA: usize = PAGE_SIZE - DATA_OFFSET;

/// No next page.
const CHAIN_END: u64 = u64::MAX;

/// Writes `bytes` as a new overflow chain and returns its first page. All
/// pages are allocated up front (WAL-logged), so every next pointer is known
/// before any page is filled and imaged.
pub(crate) fn write_chain(ctx: &mut RelCtx<'_>, bytes: &[u8]) -> Result<u64, StorageError> {
    debug_assert!(!bytes.is_empty(), "empty values are inline");
    let pages_needed = bytes.len().div_ceil(OVERFLOW_PAGE_DATA);
    let mut pages = Vec::with_capacity(pages_needed);
    for _ in 0..pages_needed {
        pages.push(ctx.allocate_page(0)?);
    }
    for (index, chunk) in bytes.chunks(OVERFLOW_PAGE_DATA).enumerate() {
        let page_no = pages[index];
        let next = pages.get(index + 1).copied().unwrap_or(CHAIN_END);
        let frame = ctx.fetch_zeroed(page_no)?;
        {
            let buf = ctx.pool.page_mut(frame);
            page::write_header(
                buf,
                &page::PageHeader {
                    page_lsn: 0,
                    page_type: page::PAGE_TYPE_OVERFLOW,
                    flags: 0,
                    object_id: 0,
                    page_no,
                },
            );
            buf[NEXT_OFFSET..NEXT_OFFSET + 8].copy_from_slice(&next.to_le_bytes());
            buf[LEN_OFFSET..LEN_OFFSET + 2].copy_from_slice(&(chunk.len() as u16).to_le_bytes());
            buf[DATA_OFFSET..DATA_OFFSET + chunk.len()].copy_from_slice(chunk);
        }
        ctx.pool.unpin(frame);
        ctx.log_system_image(page_no)?;
    }
    Ok(pages[0])
}

/// Reads a whole chain back. `total_len` bounds the walk: a chain that ends
/// early or runs long is corruption, reported rather than mis-read.
pub(crate) fn read_chain(
    ctx: &mut RelCtx<'_>,
    first_page: u64,
    total_len: u64,
) -> Result<Vec<u8>, StorageError> {
    let mut out = Vec::with_capacity(total_len as usize);
    let mut page_no = first_page;
    while out.len() < total_len as usize {
        if page_no == CHAIN_END {
            return Err(StorageError::InvalidFile(format!(
                "overflow chain ended at {} of {total_len} bytes",
                out.len()
            )));
        }
        let frame = ctx.fetch(page_no)?;
        let buf = ctx.pool.page(frame);
        let header = page::read_header(buf);
        if header.page_type != page::PAGE_TYPE_OVERFLOW {
            ctx.pool.unpin(frame);
            return Err(StorageError::InvalidFile(format!(
                "page {page_no} in an overflow chain has type {}",
                header.page_type
            )));
        }
        let next = u64::from_le_bytes(buf[NEXT_OFFSET..NEXT_OFFSET + 8].try_into().unwrap());
        let len = u16::from_le_bytes(buf[LEN_OFFSET..LEN_OFFSET + 2].try_into().unwrap()) as usize;
        if len > OVERFLOW_PAGE_DATA || out.len() + len > total_len as usize {
            ctx.pool.unpin(frame);
            return Err(StorageError::InvalidFile(
                "overflow chain page length out of bounds".to_string(),
            ));
        }
        out.extend_from_slice(&buf[DATA_OFFSET..DATA_OFFSET + len]);
        ctx.pool.unpin(frame);
        page_no = next;
    }
    Ok(out)
}
