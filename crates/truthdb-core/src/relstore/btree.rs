//! Clustered B+ tree: leaves hold full rows, internal nodes hold separator
//! keys; leaves are sibling-linked for scans. The root page number is
//! stable for the table's lifetime (root splits copy the root's content into
//! a new child), so catalog entries and undo records can always re-descend
//! from the same page.
//!
//! Cell formats:
//! - leaf: `[key_len u16][key][row]`
//! - internal: `[key_len u16][key][child u64]`; entry 0 always carries the
//!   empty sentinel key (covers keys below every separator)
//!
//! Logging: the user-visible op (one leaf insert/remove/update) goes through
//! [`RelCtx::apply_op`] with a *logical* undo (the row may move across pages
//! through later splits, so undo re-descends by key). Splits are structural:
//! performed in memory, then logged as full-page images of every touched
//! page (redo is idempotent by page LSN), never undone.

use crate::relstore::ctx::{LogMode, OpMode, RelCtx};
use crate::relstore::page::PAGE_TYPE_TREE;
use crate::relstore::slotted::{NO_PAGE, SlottedPage, SlottedRead};
use crate::storage::StorageError;
use crate::storage_layout::PAGE_SIZE;
use crate::wal::records::{PageOpRedo, PageOpUndo};

/// Maximum tree cell size: two cells (plus slots) must fit in a page or
/// splits could fail to make progress. Heap rows keep the larger in-row cap;
/// tree rows are bounded by this until overflow pages (Stage 14).
pub const TREE_MAX_CELL: usize = (PAGE_SIZE - crate::relstore::slotted::STRUCT_HEADER_END) / 2 - 4;

/// A `(key bytes, row bytes)` pair from a scan.
pub type KeyRow = (Vec<u8>, Vec<u8>);

pub(crate) struct BTree {
    pub object_id: u32,
    pub root: u64,
}

#[derive(Debug, Clone, Copy)]
struct PathNode {
    page: u64,
    /// Index of the entry in the *parent* that routed here (unused for the
    /// root).
    parent_index: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TreeInsert {
    Inserted,
    DuplicateKey,
}

fn leaf_cell(key: &[u8], row: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(2 + key.len() + row.len());
    cell.extend_from_slice(&(key.len() as u16).to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(row);
    cell
}

fn internal_cell(key: &[u8], child: u64) -> Vec<u8> {
    let mut cell = Vec::with_capacity(2 + key.len() + 8);
    cell.extend_from_slice(&(key.len() as u16).to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(&child.to_le_bytes());
    cell
}

fn cell_key(cell: &[u8]) -> &[u8] {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    &cell[2..2 + key_len]
}

fn leaf_cell_row(cell: &[u8]) -> &[u8] {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    &cell[2 + key_len..]
}

fn internal_cell_child(cell: &[u8]) -> u64 {
    let key_len = u16::from_le_bytes([cell[0], cell[1]]) as usize;
    u64::from_le_bytes(cell[2 + key_len..2 + key_len + 8].try_into().unwrap())
}

/// Binary search in a leaf: Ok(index) on exact match, Err(insertion point)
/// otherwise.
fn leaf_search(page: &SlottedRead<'_>, key: &[u8]) -> Result<usize, usize> {
    let mut low = 0usize;
    let mut high = page.slot_count();
    while low < high {
        let mid = (low + high) / 2;
        let cell = page.get(mid).expect("tree pages have no tombstones");
        match cell_key(cell).cmp(key) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => return Ok(mid),
        }
    }
    Err(low)
}

/// Routing in an internal node: index of the rightmost entry whose key is
/// `<= key` (entry 0's sentinel empty key matches everything).
fn internal_route(page: &SlottedRead<'_>, key: &[u8]) -> usize {
    let mut low = 0usize;
    let mut high = page.slot_count();
    while low < high {
        let mid = (low + high) / 2;
        let cell = page.get(mid).expect("tree pages have no tombstones");
        if cell_key(cell) <= key {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    debug_assert!(low > 0, "sentinel entry must match");
    low - 1
}

impl BTree {
    /// Creates a single-leaf tree (logged as a system image).
    pub fn create(ctx: &mut RelCtx<'_>, object_id: u32) -> Result<BTree, StorageError> {
        let root = ctx.allocate_page(0)?;
        let frame = ctx.format_page(root, PAGE_TYPE_TREE, object_id, 0)?;
        ctx.pool.unpin(frame);
        ctx.log_system_image(root)?;
        Ok(BTree { object_id, root })
    }

    /// Descends to the leaf responsible for `key`, recording the path.
    fn descend(&self, ctx: &mut RelCtx<'_>, key: &[u8]) -> Result<Vec<PathNode>, StorageError> {
        let mut path = vec![PathNode {
            page: self.root,
            parent_index: 0,
        }];
        loop {
            let node = *path.last().expect("path non-empty");
            let frame = ctx.fetch(node.page)?;
            let page = SlottedRead::new(ctx.pool.page(frame));
            if page.level() == 0 {
                ctx.pool.unpin(frame);
                return Ok(path);
            }
            let index = internal_route(&page, key);
            let child = internal_cell_child(page.get(index).expect("routed entry"));
            ctx.pool.unpin(frame);
            path.push(PathNode {
                page: child,
                parent_index: index,
            });
        }
    }

    pub fn get(&self, ctx: &mut RelCtx<'_>, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let path = self.descend(ctx, key)?;
        let leaf = path.last().expect("path has a leaf").page;
        let frame = ctx.fetch(leaf)?;
        let page = SlottedRead::new(ctx.pool.page(frame));
        let row = leaf_search(&page, key)
            .ok()
            .map(|index| leaf_cell_row(page.get(index).expect("found cell")).to_vec());
        ctx.pool.unpin(frame);
        Ok(row)
    }

    /// Inserts a unique key. Splits happen structurally (system images)
    /// before the insert is logged with a logical delete-key undo.
    pub fn insert_unique(
        &self,
        ctx: &mut RelCtx<'_>,
        mode: &mut OpMode<'_>,
        key: &[u8],
        row: &[u8],
    ) -> Result<TreeInsert, StorageError> {
        let cell = leaf_cell(key, row);
        if cell.len() > TREE_MAX_CELL {
            return Err(StorageError::InvalidConfig(format!(
                "tree row of {} bytes exceeds the per-cell cap of {TREE_MAX_CELL}",
                cell.len()
            )));
        }
        loop {
            let path = self.descend(ctx, key)?;
            let leaf = path.last().expect("leaf").page;
            let frame = ctx.fetch(leaf)?;
            let page = SlottedRead::new(ctx.pool.page(frame));
            let position = match leaf_search(&page, key) {
                Ok(_) => {
                    ctx.pool.unpin(frame);
                    return Ok(TreeInsert::DuplicateKey);
                }
                Err(position) => position,
            };
            let fits = page.total_free() >= cell.len() + 4;
            ctx.pool.unpin(frame);

            if fits {
                ctx.apply_op(
                    mode.log_mode(PageOpUndo::TreeDeleteKey {
                        object_id: self.object_id,
                        key: key.to_vec(),
                    }),
                    PageOpRedo::InsertAt {
                        page: leaf,
                        index: position as u16,
                        bytes: cell.clone(),
                    },
                )?;
                return Ok(TreeInsert::Inserted);
            }
            self.split_one(ctx, &path)?;
            // Re-descend: the split may have moved the target range.
        }
    }

    /// Inserts a unique key logged as a system image (redo-only, no undo).
    /// Used to backfill a freshly-created index tree: the tree is not yet in
    /// the statement's undo roots, so per-insert undo could not re-descend it.
    /// A failed build simply leaves the whole tree an unreferenced orphan (the
    /// catalog entry that would name it is undone), matching the accepted
    /// DDL-rollback page-leak trade-off.
    pub fn insert_unique_bulk(
        &self,
        ctx: &mut RelCtx<'_>,
        key: &[u8],
        row: &[u8],
    ) -> Result<TreeInsert, StorageError> {
        let cell = leaf_cell(key, row);
        if cell.len() > TREE_MAX_CELL {
            return Err(StorageError::InvalidConfig(format!(
                "tree row of {} bytes exceeds the per-cell cap of {TREE_MAX_CELL}",
                cell.len()
            )));
        }
        loop {
            let path = self.descend(ctx, key)?;
            let leaf = path.last().expect("leaf").page;
            let frame = ctx.fetch(leaf)?;
            let page = SlottedRead::new(ctx.pool.page(frame));
            let position = match leaf_search(&page, key) {
                Ok(_) => {
                    ctx.pool.unpin(frame);
                    return Ok(TreeInsert::DuplicateKey);
                }
                Err(position) => position,
            };
            let fits = page.total_free() >= cell.len() + 4;
            ctx.pool.unpin(frame);
            if fits {
                ctx.apply_op(
                    LogMode::System,
                    PageOpRedo::InsertAt {
                        page: leaf,
                        index: position as u16,
                        bytes: cell.clone(),
                    },
                )?;
                return Ok(TreeInsert::Inserted);
            }
            self.split_one(ctx, &path)?;
        }
    }

    /// Deletes `key`, returning the removed row. No rebalancing (deletes
    /// leave pages sparse, SQL Server-style).
    pub fn delete(
        &self,
        ctx: &mut RelCtx<'_>,
        mode: &mut OpMode<'_>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let path = self.descend(ctx, key)?;
        let leaf = path.last().expect("leaf").page;
        let frame = ctx.fetch(leaf)?;
        let page = SlottedRead::new(ctx.pool.page(frame));
        let found = leaf_search(&page, key).ok().map(|index| {
            (
                index,
                leaf_cell_row(page.get(index).expect("cell")).to_vec(),
            )
        });
        ctx.pool.unpin(frame);
        let Some((index, row)) = found else {
            return Ok(None);
        };
        ctx.apply_op(
            mode.log_mode(PageOpUndo::TreeInsertRow {
                object_id: self.object_id,
                key: key.to_vec(),
                row: row.clone(),
            }),
            PageOpRedo::RemoveAt {
                page: leaf,
                index: index as u16,
            },
        )?;
        Ok(Some(row))
    }

    /// Replaces the row stored under `key` (the key itself is immutable).
    /// Returns the previous row, or None if the key is absent.
    pub fn update(
        &self,
        ctx: &mut RelCtx<'_>,
        mode: &mut OpMode<'_>,
        key: &[u8],
        new_row: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let cell = leaf_cell(key, new_row);
        if cell.len() > TREE_MAX_CELL {
            return Err(StorageError::InvalidConfig(format!(
                "tree row of {} bytes exceeds the per-cell cap of {TREE_MAX_CELL}",
                cell.len()
            )));
        }
        let path = self.descend(ctx, key)?;
        let leaf = path.last().expect("leaf").page;
        let frame = ctx.fetch(leaf)?;
        let page = SlottedRead::new(ctx.pool.page(frame));
        let found = leaf_search(&page, key).ok().map(|index| {
            let old_cell = page.get(index).expect("cell");
            (
                index,
                leaf_cell_row(old_cell).to_vec(),
                page.total_free() + old_cell.len() >= cell.len(),
            )
        });
        ctx.pool.unpin(frame);
        let Some((index, old_row, fits_in_place)) = found else {
            return Ok(None);
        };

        if fits_in_place {
            ctx.apply_op(
                mode.log_mode(PageOpUndo::TreeUpdateRow {
                    object_id: self.object_id,
                    key: key.to_vec(),
                    row: old_row.clone(),
                }),
                PageOpRedo::UpdateAt {
                    page: leaf,
                    index: index as u16,
                    bytes: cell,
                },
            )?;
            return Ok(Some(old_row));
        }

        // Grown row that no longer fits: remove + re-insert (with splits).
        // The undo pair restores the old row: undo(insert) deletes the key,
        // then undo(remove) re-inserts the old row.
        ctx.apply_op(
            mode.log_mode(PageOpUndo::TreeInsertRow {
                object_id: self.object_id,
                key: key.to_vec(),
                row: old_row.clone(),
            }),
            PageOpRedo::RemoveAt {
                page: leaf,
                index: index as u16,
            },
        )?;
        match self.insert_unique(ctx, mode, key, new_row)? {
            TreeInsert::Inserted => Ok(Some(old_row)),
            TreeInsert::DuplicateKey => Err(StorageError::InvalidFile(
                "key reappeared during tree update".to_string(),
            )),
        }
    }

    /// Full scan in key order via the leaf chain: (key, row) pairs.
    pub fn scan(&self, ctx: &mut RelCtx<'_>) -> Result<Vec<KeyRow>, StorageError> {
        // Find the leftmost leaf.
        let mut page_no = self.root;
        loop {
            let frame = ctx.fetch(page_no)?;
            let page = SlottedRead::new(ctx.pool.page(frame));
            if page.level() == 0 {
                ctx.pool.unpin(frame);
                break;
            }
            let child = internal_cell_child(page.get(0).expect("sentinel entry"));
            ctx.pool.unpin(frame);
            page_no = child;
        }
        let mut out = Vec::new();
        while page_no != NO_PAGE {
            let frame = ctx.fetch(page_no)?;
            let page = SlottedRead::new(ctx.pool.page(frame));
            for index in 0..page.slot_count() {
                let cell = page.get(index).expect("tree pages have no tombstones");
                out.push((cell_key(cell).to_vec(), leaf_cell_row(cell).to_vec()));
            }
            let next = page.next_page();
            ctx.pool.unpin(frame);
            page_no = next;
        }
        Ok(out)
    }

    /// Range scan in key order: `(key, row)` pairs with `lower <= key <= upper`
    /// (either bound `None` = unbounded on that side). Descends to `lower`'s
    /// leaf, then walks the leaf chain, stopping past `upper`.
    pub fn scan_range(
        &self,
        ctx: &mut RelCtx<'_>,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<Vec<KeyRow>, StorageError> {
        // Leaf and start index for the lower bound.
        let (mut page_no, mut start) = match lower {
            Some(key) => {
                let path = self.descend(ctx, key)?;
                let leaf = path.last().expect("leaf").page;
                let frame = ctx.fetch(leaf)?;
                let page = SlottedRead::new(ctx.pool.page(frame));
                let index = match leaf_search(&page, key) {
                    Ok(i) => i,
                    Err(i) => i,
                };
                ctx.pool.unpin(frame);
                (leaf, index)
            }
            None => (self.leftmost_leaf(ctx)?, 0),
        };
        let mut out = Vec::new();
        while page_no != NO_PAGE {
            let frame = ctx.fetch(page_no)?;
            let page = SlottedRead::new(ctx.pool.page(frame));
            for index in start..page.slot_count() {
                let cell = page.get(index).expect("tree pages have no tombstones");
                let key = cell_key(cell);
                if let Some(upper) = upper
                    && key > upper
                {
                    ctx.pool.unpin(frame);
                    return Ok(out);
                }
                out.push((key.to_vec(), leaf_cell_row(cell).to_vec()));
            }
            let next = page.next_page();
            ctx.pool.unpin(frame);
            page_no = next;
            start = 0;
        }
        Ok(out)
    }

    /// The leftmost leaf page (start of the leaf chain).
    fn leftmost_leaf(&self, ctx: &mut RelCtx<'_>) -> Result<u64, StorageError> {
        let mut page_no = self.root;
        loop {
            let frame = ctx.fetch(page_no)?;
            let page = SlottedRead::new(ctx.pool.page(frame));
            if page.level() == 0 {
                ctx.pool.unpin(frame);
                return Ok(page_no);
            }
            let child = internal_cell_child(page.get(0).expect("sentinel entry"));
            ctx.pool.unpin(frame);
            page_no = child;
        }
    }

    /// Splits exactly one node: the deepest node on `path` whose parent can
    /// absorb the separator (ancestors that cannot are split instead; the
    /// caller re-descends after each split).
    fn split_one(&self, ctx: &mut RelCtx<'_>, path: &[PathNode]) -> Result<(), StorageError> {
        let mut depth = path.len() - 1;
        loop {
            let (split_at, sep_len) = self.split_plan(ctx, path[depth].page)?;
            if depth == 0 {
                return self.split_root(ctx, split_at);
            }
            // Parent must fit an internal cell of (2 + sep_len + 8) bytes.
            let parent = path[depth - 1].page;
            let frame = ctx.fetch(parent)?;
            let parent_free = SlottedRead::new(ctx.pool.page(frame)).total_free();
            ctx.pool.unpin(frame);
            if parent_free >= 2 + sep_len + 8 + 4 {
                return self.split_child(
                    ctx,
                    path[depth].page,
                    parent,
                    path[depth].parent_index,
                    split_at,
                );
            }
            depth -= 1;
        }
    }

    /// Read-only split plan: (index to split at, separator key length).
    fn split_plan(
        &self,
        ctx: &mut RelCtx<'_>,
        page_no: u64,
    ) -> Result<(usize, usize), StorageError> {
        let frame = ctx.fetch(page_no)?;
        let page = SlottedRead::new(ctx.pool.page(frame));
        let count = page.slot_count();
        debug_assert!(count >= 2, "splitting a page with < 2 cells");
        let split_at = count / 2;
        let sep_len = cell_key(page.get(split_at).expect("cell")).len();
        ctx.pool.unpin(frame);
        Ok((split_at, sep_len))
    }

    /// Splits a non-root node: upper half moves to a fresh right sibling,
    /// separator goes into the parent.
    ///
    /// Crash-atomicity protocol: every touched frame stays PINNED from its
    /// first mutation until the single atomic image-group record is
    /// appended (so no buffer-pool steal can write unlogged state), and
    /// pre-images restore the in-memory pages if the append fails.
    fn split_child(
        &self,
        ctx: &mut RelCtx<'_>,
        page_no: u64,
        parent: u64,
        parent_index: usize,
        split_at: usize,
    ) -> Result<(), StorageError> {
        let right = ctx.allocate_page(0)?;

        // Pin the left node for the whole operation; read the upper half.
        let left_frame = ctx.fetch(page_no)?;
        let left_pre_image = ctx.pool.page(left_frame).to_vec();
        let (level, count, moved, old_next) = {
            let page = SlottedRead::new(ctx.pool.page(left_frame));
            let count = page.slot_count();
            let moved: Vec<Vec<u8>> = (split_at..count)
                .map(|i| page.get(i).expect("cell").to_vec())
                .collect();
            (page.level(), count, moved, page.next_page())
        };
        let separator = cell_key(&moved[0]).to_vec();

        // Build the right sibling (fresh page: no pre-image needed — on any
        // failure it stays an unreferenced orphan).
        let right_frame = match ctx.format_page(right, PAGE_TYPE_TREE, self.object_id, level) {
            Ok(frame) => frame,
            Err(err) => {
                ctx.pool.unpin(left_frame);
                return Err(err);
            }
        };
        let build_failed = {
            let mut right_page = SlottedPage::new(ctx.pool.page_mut(right_frame));
            let mut failed = false;
            for (i, cell) in moved.iter().enumerate() {
                let cell = if level > 0 && i == 0 {
                    // Promote the first key to the parent; the right node's
                    // first entry becomes the sentinel.
                    internal_cell(&[], internal_cell_child(cell))
                } else {
                    cell.clone()
                };
                if right_page.insert_at(i, &cell).is_err() {
                    failed = true;
                    break;
                }
            }
            if !failed && level == 0 {
                right_page.set_next_page(old_next);
            }
            failed
        };
        if build_failed {
            ctx.pool.unpin(left_frame);
            ctx.pool.unpin(right_frame);
            return Err(split_overflow());
        }

        // Shrink the left node.
        {
            let mut left_page = SlottedPage::new(ctx.pool.page_mut(left_frame));
            for i in (split_at..count).rev() {
                left_page.remove_at(i);
            }
            if level == 0 {
                left_page.set_next_page(right);
            }
        }

        // Insert the separator into the parent (fit checked by caller), then
        // log the whole split as ONE atomic record.
        let result = (|| -> Result<crate::relstore::buffer_pool::FrameId, StorageError> {
            let parent_frame = ctx.fetch(parent)?;
            let parent_pre_image = ctx.pool.page(parent_frame).to_vec();
            if SlottedPage::new(ctx.pool.page_mut(parent_frame))
                .insert_at(parent_index + 1, &internal_cell(&separator, right))
                .is_err()
            {
                ctx.pool.unpin(parent_frame);
                return Err(split_overflow());
            }
            match ctx.log_system_images(&[
                (page_no, left_frame),
                (right, right_frame),
                (parent, parent_frame),
            ]) {
                Ok(_) => Ok(parent_frame),
                Err(err) => {
                    ctx.pool
                        .page_mut(parent_frame)
                        .copy_from_slice(&parent_pre_image);
                    ctx.pool.unpin(parent_frame);
                    Err(err)
                }
            }
        })();
        match result {
            Ok(parent_frame) => {
                ctx.pool.unpin(left_frame);
                ctx.pool.unpin(right_frame);
                ctx.pool.unpin(parent_frame);
                Ok(())
            }
            Err(err) => {
                // Restore the left node; the orphan right page is harmless.
                ctx.pool
                    .page_mut(left_frame)
                    .copy_from_slice(&left_pre_image);
                ctx.pool.unpin(left_frame);
                ctx.pool.unpin(right_frame);
                Err(err)
            }
        }
    }

    /// Splits the root while keeping its page number: contents move into a
    /// fresh left child, the root becomes (or stays) internal with two
    /// entries. Same pin-until-logged atomic protocol as `split_child`.
    fn split_root(&self, ctx: &mut RelCtx<'_>, split_at: usize) -> Result<(), StorageError> {
        let left = ctx.allocate_page(0)?;
        let right = ctx.allocate_page(0)?;

        let root_frame = ctx.fetch(self.root)?;
        let root_pre_image = ctx.pool.page(root_frame).to_vec();
        let (level, cells) = {
            let page = SlottedRead::new(ctx.pool.page(root_frame));
            let cells: Vec<Vec<u8>> = (0..page.slot_count())
                .map(|i| page.get(i).expect("cell").to_vec())
                .collect();
            (page.level(), cells)
        };
        let separator = cell_key(&cells[split_at]).to_vec();

        let result = (|| -> Result<
            (
                crate::relstore::buffer_pool::FrameId,
                crate::relstore::buffer_pool::FrameId,
            ),
            StorageError,
        > {
            let left_frame = ctx.format_page(left, PAGE_TYPE_TREE, self.object_id, level)?;
            let build_failed = {
                let mut left_page = SlottedPage::new(ctx.pool.page_mut(left_frame));
                let mut failed = false;
                for (i, cell) in cells[..split_at].iter().enumerate() {
                    if left_page.insert_at(i, cell).is_err() {
                        failed = true;
                        break;
                    }
                }
                if !failed && level == 0 {
                    left_page.set_next_page(right);
                }
                failed
            };
            if build_failed {
                ctx.pool.unpin(left_frame);
                return Err(split_overflow());
            }
            let right_frame = match ctx.format_page(right, PAGE_TYPE_TREE, self.object_id, level)
            {
                Ok(frame) => frame,
                Err(err) => {
                    ctx.pool.unpin(left_frame);
                    return Err(err);
                }
            };
            // The root was the only (or rightmost) node at its level, so
            // the right child terminates the chain at leaf level.
            let build_failed = {
                let mut right_page = SlottedPage::new(ctx.pool.page_mut(right_frame));
                let mut failed = false;
                for (i, cell) in cells[split_at..].iter().enumerate() {
                    let cell = if level > 0 && i == 0 {
                        internal_cell(&[], internal_cell_child(cell))
                    } else {
                        cell.clone()
                    };
                    if right_page.insert_at(i, &cell).is_err() {
                        failed = true;
                        break;
                    }
                }
                failed
            };
            if build_failed {
                ctx.pool.unpin(left_frame);
                ctx.pool.unpin(right_frame);
                return Err(split_overflow());
            }

            let rebuild_failed = {
                let mut root_page = SlottedPage::format(ctx.pool.page_mut(root_frame), level + 1);
                root_page
                    .insert_at(0, &internal_cell(&[], left))
                    .and_then(|()| root_page.insert_at(1, &internal_cell(&separator, right)))
                    .is_err()
            };
            if rebuild_failed {
                ctx.pool.unpin(left_frame);
                ctx.pool.unpin(right_frame);
                return Err(split_overflow());
            }
            match ctx.log_system_images(&[
                (left, left_frame),
                (right, right_frame),
                (self.root, root_frame),
            ]) {
                Ok(_) => Ok((left_frame, right_frame)),
                Err(err) => {
                    ctx.pool.unpin(left_frame);
                    ctx.pool.unpin(right_frame);
                    Err(err)
                }
            }
        })();
        match result {
            Ok((left_frame, right_frame)) => {
                ctx.pool.unpin(root_frame);
                ctx.pool.unpin(left_frame);
                ctx.pool.unpin(right_frame);
                Ok(())
            }
            Err(err) => {
                ctx.pool
                    .page_mut(root_frame)
                    .copy_from_slice(&root_pre_image);
                ctx.pool.unpin(root_frame);
                Err(err)
            }
        }
    }
}

fn split_overflow() -> StorageError {
    StorageError::InvalidFile("page overflow during split (cell size invariant broken)".to_string())
}

/// Applies a logical tree undo as compensation. Ops are tolerant of partial
/// prior application (a crash between the CLRs of one undone record replays
/// the whole undo, so each piece must be a no-op when already done).
pub(crate) fn apply_tree_undo(
    ctx: &mut RelCtx<'_>,
    mode: &mut OpMode<'_>,
    tree: &BTree,
    undo: &PageOpUndo,
) -> Result<(), StorageError> {
    match undo {
        PageOpUndo::TreeDeleteKey { key, .. } => {
            // Undo of an insert: delete if present.
            let _ = tree.delete(ctx, mode, key)?;
        }
        PageOpUndo::TreeInsertRow { key, row, .. } => {
            // Undo of a delete: re-insert if absent.
            let _ = tree.insert_unique(ctx, mode, key, row)?;
        }
        PageOpUndo::TreeUpdateRow { key, row, .. } => {
            // Undo of an update: restore the old row (insert if the new row
            // vanished mid-undo).
            if tree.update(ctx, mode, key, row)?.is_none() {
                let _ = tree.insert_unique(ctx, mode, key, row)?;
            }
        }
        _ => unreachable!("not a tree undo"),
    }
    Ok(())
}
