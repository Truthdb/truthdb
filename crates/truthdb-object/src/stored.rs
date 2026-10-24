//! What bodies take on disk: for each body its logical bytes, the bytes its
//! segments store (zstd frames where packing shrank them) and the pages
//! those take. A body has no bucket or class of its own (copies share one),
//! so it is told apart by its use and the level it was packed at.

use truthdb_core::storage::StorageError;
use truthdb_core::storage_layout::PAGE_SIZE;

use crate::codec::{BodyRow, BodyState, SegmentRow, body_key, segment_key};
use crate::{BODIES_TREE, GC_TREE, ObjFile, PACK_QUEUE_TREE, SEGMENTS_TREE};

/// What a body is used for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BodyUse {
    /// Held by an object version or an upload's part.
    Live,
    /// Held by nothing: freed once its grace ends.
    Collectable,
    /// Still being written (an upload in progress, or abandoned).
    Staging,
}

impl BodyUse {
    pub fn as_str(self) -> &'static str {
        match self {
            BodyUse::Live => "LIVE",
            BodyUse::Collectable => "COLLECTABLE",
            BodyUse::Staging => "STAGING",
        }
    }
}

/// One body's bytes: as written (`logical`), as stored, and the pages they
/// take (in bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredBody {
    pub body_use: BodyUse,
    pub pack_level: u8,
    pub logical: u64,
    pub stored: u64,
    pub allocated: u64,
}

fn body_of(key: &[u8]) -> Result<u64, StorageError> {
    Ok(u64::from_be_bytes(
        key.try_into()
            .map_err(|_| crate::codec::corrupt("body key"))?,
    ))
}

/// Work the object store has queued: bodies waiting to be freed (each
/// past its grace or not) and bodies waiting to be packed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Backlogs {
    pub collect: u64,
    pub pack: u64,
}

/// How many queue rows a count reads per scan.
const COUNT_BATCH: usize = 4096;

impl ObjFile<'_> {
    /// How many bodies wait to be freed and to be packed.
    pub fn backlogs(&mut self) -> Result<Backlogs, StorageError> {
        let mut backlogs = Backlogs::default();
        for (tree, count) in [
            (GC_TREE, &mut backlogs.collect),
            (PACK_QUEUE_TREE, &mut backlogs.pack),
        ] {
            let Some(tree) = self.existing_tree(tree)? else {
                continue;
            };
            let mut ctx = self.core.page_ctx(0);
            let mut after: Option<Vec<u8>> = None;
            loop {
                let mut rows =
                    tree.scan_range_limit(&mut ctx, after.as_deref(), None, COUNT_BATCH + 1)?;
                // The scan starts at `after` itself, counted already.
                if after.is_some() && !rows.is_empty() {
                    rows.remove(0);
                }
                *count += rows.len() as u64;
                match rows.last() {
                    Some((key, _)) if rows.len() >= COUNT_BATCH => after = Some(key.clone()),
                    _ => break,
                }
            }
        }
        Ok(backlogs)
    }

    /// The bodies after `after` (by id), at most `limit` of them, and the
    /// last id read (`None` once there are no more). A body made of others
    /// (a completed multipart upload's) takes no pages of its own and is
    /// left out: its parts are bodies too.
    pub fn stored_bodies(
        &mut self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<(Vec<StoredBody>, Option<u64>), StorageError> {
        let (Some(bodies), Some(segments)) = (
            self.existing_tree(BODIES_TREE)?,
            self.existing_tree(SEGMENTS_TREE)?,
        ) else {
            return Ok((Vec::new(), None));
        };
        let mut ctx = self.core.page_ctx(0);
        let start = match after {
            None => body_key(0),
            Some(u64::MAX) => return Ok((Vec::new(), None)),
            Some(body) => body_key(body + 1),
        };
        let rows = bodies.scan_range_limit(&mut ctx, Some(&start), None, limit.max(1))?;
        let last = (rows.len() >= limit.max(1))
            .then(|| rows.last().map(|(key, _)| body_of(key)))
            .flatten()
            .transpose()?;
        let mut out = Vec::with_capacity(rows.len());
        for (key, row) in rows {
            let body = body_of(&key)?;
            let row = BodyRow::decode(&row)?;
            let body_use = match row.state {
                BodyState::Composite => continue,
                BodyState::Staging => BodyUse::Staging,
                BodyState::Sealed if row.refcount == 0 => BodyUse::Collectable,
                BodyState::Sealed => BodyUse::Live,
            };
            let (mut stored, mut pages) = (0u64, 0u64);
            for (_, segment) in segments.scan_range(
                &mut ctx,
                Some(&segment_key(body, 0)),
                Some(&segment_key(body, u64::MAX)),
            )? {
                let segment = SegmentRow::decode(&segment)?;
                stored += u64::from(segment.len);
                pages += u64::from(segment.pages);
            }
            out.push(StoredBody {
                body_use,
                pack_level: row.pack_level,
                logical: row.len,
                stored,
                allocated: pages * PAGE_SIZE as u64,
            });
        }
        Ok((out, last))
    }
}
