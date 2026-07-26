use super::super::*;

impl StorageFile {
    /// Maintains the standby's active-transaction table as shipped records are
    /// applied — the SAME begin/resolve rules recovery's analysis uses
    /// (`analyze_and_redo`): TXN_BEGIN opens at its own LSN, TXN_COMMIT and
    /// TXN_END resolve. A page op for a transaction whose BEGIN was not seen
    /// cannot arise (a chunk never precedes a transaction's BEGIN — chunks
    /// apply in order and BEGIN is its first record); the defensive
    /// `or_insert` still clamps the floor at that record if it ever did.
    pub(in crate::storage) fn standby_track_rel_record(&mut self, lsn: u64, record: &RelRecord) {
        use crate::wal::records::{REL_KIND_TXN_BEGIN, REL_KIND_TXN_COMMIT, REL_KIND_TXN_END};
        match record.kind {
            REL_KIND_TXN_BEGIN => {
                self.standby_att.insert(record.txn_id, lsn);
            }
            REL_KIND_TXN_COMMIT | REL_KIND_TXN_END => {
                self.standby_att.remove(&record.txn_id);
            }
            _ => {
                if record.txn_id != 0 {
                    self.standby_att.entry(record.txn_id).or_insert(lsn);
                }
            }
        }
    }

    /// Drops any replication slot lagging the WAL tail by more than
    /// Readable standby: mirrors a fully-redone shipped range into the version
    /// store — the pre-image of every row change is already in the record's
    /// UNDO payload, so `publish` + `record_commit` reproduce exactly what the
    /// primary's own commit path builds. Uncommitted (in-flight) writers have
    /// no commit seq, so a snapshot reader at the last-applied-commit sequence
    /// resolves past their chain heads to the pre-image — the committed state.
    /// CLRs pop and unpublish the compensated suffix (savepoint/statement
    /// rollbacks inside a transaction that later commits); a commit-less
    /// TXN_END unwinds the rest (a full abort). Heap undo payloads are CELL
    /// bytes — decoded (tag stripped, moved rows re-homed) before publishing.
    /// Only live TABLE objects get chains (index-maintenance undos carry the
    /// index's object id; nothing resolves index chains, and pruning clears
    /// them), a page freed anywhere in the range is skipped (its historical
    /// owner is not derivable from the post-range header), and a DDL in the
    /// range stamps every live table so an older pinned snapshot fails 3961
    /// instead of decoding rows under the wrong schema. Per-record failures
    /// are logged and skipped — a capture problem must not wedge the stream
    /// (the cost is one row's pre-image, not divergence).
    pub(in crate::storage) fn standby_capture_versions(&mut self, records: &[(u64, RelRecord)]) {
        use crate::wal::records::{
            REL_KIND_CLR, REL_KIND_FREE_EXTENT, REL_KIND_PAGE_IMAGE, REL_KIND_PAGE_OP,
            REL_KIND_SET_CATALOG_ROOT, REL_KIND_TXN_COMMIT, REL_KIND_TXN_END,
        };
        let alive: std::collections::HashSet<u32> =
            self.rel.all_tables().map(|def| def.object_id).collect();
        let mut freed_pages: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (_, record) in records {
            if record.kind == REL_KIND_FREE_EXTENT
                && let Ok((start, pages)) = record.decode_extent_redo()
            {
                for page in start..start.saturating_add(pages) {
                    freed_pages.insert(page);
                }
            }
        }
        let mut catalog_changed = false;
        for (lsn, record) in records {
            if *lsn < self.standby_version_floor {
                continue;
            }
            match record.kind {
                REL_KIND_PAGE_OP | REL_KIND_PAGE_IMAGE if record.txn_id != 0 => {
                    let pending = self
                        .standby_pending_version(record, &alive, &freed_pages)
                        .unwrap_or_else(|err| {
                            eprintln!(
                                "standby version capture: skipping record at LSN {lsn}: {err}"
                            );
                            None
                        });
                    let published = pending.map(|p| self.version.publish(p, record.txn_id));
                    self.standby_published
                        .entry(record.txn_id)
                        .or_default()
                        .push((*lsn, published));
                }
                REL_KIND_CLR if record.txn_id != 0 => {
                    // The CLR's redo opens with the `undo_next` LSN: everything
                    // the transaction logged ABOVE it is now compensated.
                    if record.redo.len() >= 8 {
                        let undo_next = u64::from_le_bytes(record.redo[0..8].try_into().unwrap());
                        if let Some(stack) = self.standby_published.get_mut(&record.txn_id) {
                            while stack.last().is_some_and(|(l, _)| *l > undo_next) {
                                if let Some((_, Some(rec))) = stack.pop() {
                                    self.version.unpublish(rec, record.txn_id);
                                }
                            }
                        }
                    }
                }
                REL_KIND_TXN_COMMIT => {
                    self.version.record_commit(record.txn_id, *lsn);
                    self.standby_published.remove(&record.txn_id);
                }
                REL_KIND_TXN_END => {
                    if let Some(stack) = self.standby_published.remove(&record.txn_id) {
                        for (_, published) in stack.into_iter().rev() {
                            if let Some(rec) = published {
                                self.version.unpublish(rec, record.txn_id);
                            }
                        }
                    }
                }
                REL_KIND_SET_CATALOG_ROOT => catalog_changed = true,
                _ => {}
            }
        }
        if catalog_changed {
            // A shipped DDL cannot wait out pinned snapshots the way the
            // primary's Database X does; fence them instead (3961 on the next
            // access), for every live table — conservative and correct.
            for object_id in alive {
                self.version.stamp_schema(object_id);
            }
        }
    }

    /// Builds the version-store change for one shipped row op, or `None` for
    /// structural/system/foreign records.
    pub(in crate::storage) fn standby_pending_version(
        &mut self,
        record: &RelRecord,
        alive: &std::collections::HashSet<u32>,
        freed_pages: &std::collections::HashSet<u64>,
    ) -> Result<Option<crate::relstore::version::PendingVersion>, StorageError> {
        use crate::relstore::version::{PendingVersion, RowChange};
        use crate::wal::records::PageOpUndo;
        type HeapChange = Option<(u32, Vec<u8>, Option<Vec<u8>>)>;
        let heap_change = |this: &mut Self,
                           page: u64,
                           slot: u16,
                           cell: Option<Vec<u8>>|
         -> Result<HeapChange, StorageError> {
            if freed_pages.contains(&page) {
                return Ok(None);
            }
            let Some(object_id) = this.heap_page_object_id(page)? else {
                return Ok(None);
            };
            if !alive.contains(&object_id) {
                return Ok(None);
            }
            match cell {
                None => Ok(Some((
                    object_id,
                    rid_identity(crate::relstore::heap::Rid { page, slot }),
                    None,
                ))),
                Some(cell) => {
                    // The undo payload is a heap CELL: strip the tag, and home
                    // a MOVED copy's identity to the RID readers scan under.
                    let Some((home, row)) = crate::relstore::heap::cell_row(&cell) else {
                        return Ok(None); // a stub — a pointer, not a row
                    };
                    let identity =
                        rid_identity(home.unwrap_or(crate::relstore::heap::Rid { page, slot }));
                    Ok(Some((object_id, identity, Some(row.to_vec()))))
                }
            }
        };
        let pending =
            match record.decode_page_op_undo()? {
                PageOpUndo::TreeDeleteKey { object_id, key } if alive.contains(&object_id) => {
                    Some(PendingVersion {
                        object_id,
                        identity: key,
                        change: RowChange::Insert,
                    })
                }
                PageOpUndo::TreeInsertRow {
                    object_id,
                    key,
                    row,
                } if alive.contains(&object_id) => Some(PendingVersion {
                    object_id,
                    identity: key,
                    change: RowChange::Delete { prior: row },
                }),
                PageOpUndo::TreeUpdateRow {
                    object_id,
                    key,
                    row,
                } if alive.contains(&object_id) => Some(PendingVersion {
                    object_id,
                    identity: key,
                    change: RowChange::Update { prior: row },
                }),
                PageOpUndo::HeapDeleteSlot { page, slot } => heap_change(self, page, slot, None)?
                    .map(|(object_id, identity, _)| PendingVersion {
                        object_id,
                        identity,
                        change: RowChange::Insert,
                    }),
                PageOpUndo::HeapInsertRow { page, slot, bytes } => {
                    heap_change(self, page, slot, Some(bytes))?.map(|(object_id, identity, row)| {
                        PendingVersion {
                            object_id,
                            identity,
                            change: RowChange::Delete {
                                prior: row.expect("cell decoded"),
                            },
                        }
                    })
                }
                PageOpUndo::HeapUpdateRow { page, slot, bytes } => {
                    heap_change(self, page, slot, Some(bytes))?.map(|(object_id, identity, row)| {
                        PendingVersion {
                            object_id,
                            identity,
                            change: RowChange::Update {
                                prior: row.expect("cell decoded"),
                            },
                        }
                    })
                }
                _ => None,
            };
        Ok(pending)
    }

    /// The owning object of a heap page, from its self-identifying header
    /// (`None` for a page that is not heap-formatted — a stale undo against a
    /// since-freed page).
    pub(in crate::storage) fn heap_page_object_id(
        &mut self,
        page: u64,
    ) -> Result<Option<u32>, StorageError> {
        let mut ctx = self.rel_ctx();
        let frame = ctx.fetch(page)?;
        let header = crate::relstore::page::read_header(ctx.pool.page(frame));
        ctx.pool.unpin(frame);
        Ok((header.page_type == crate::relstore::page::PAGE_TYPE_HEAP).then_some(header.object_id))
    }
}
