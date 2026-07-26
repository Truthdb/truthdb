use super::*;

impl Storage {
    /// Scan slices this store has read.
    #[cfg(test)]
    pub(crate) fn scan_slices(&self) -> usize {
        self.scan_slices.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// SELECTs this store has answered on the row-at-a-time path.
    #[cfg(test)]
    pub(crate) fn scan_selects(&self) -> usize {
        self.scan_selects.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Index scans answered from the leaves alone (covering, no base lookup).
    #[cfg(test)]
    pub(crate) fn covering_scans(&self) -> usize {
        self.covering_scans
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Lazily-scanned sources drained WHOLE (`SourceRows::materialize` on a
    /// scan): what the join operators do, and what the streamed input path
    /// must NOT do.
    #[cfg(test)]
    pub(crate) fn scan_materializations(&self) -> usize {
        self.scan_materializations
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Counts one whole-scan drain (called by `SourceRows::materialize`).
    #[cfg(test)]
    pub(crate) fn count_scan_materialization(&self) {
        self.scan_materializations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Columns the last scan slice decoded per row (`usize::MAX` = every one).
    #[cfg(test)]
    pub(crate) fn last_scan_width(&self) -> usize {
        self.last_scan_width
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Counts one row-at-a-time SELECT (called by `relational::scan_select`).
    #[cfg(test)]
    pub(crate) fn count_scan_select(&self) {
        self.scan_selects
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rel_index_scan(
        &self,
        db_id: u32,
        table: &str,
        index_object_id: u32,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        projection: Option<&[usize]>,
        covering: bool,
        snapshot: Option<ReadSnapshot>,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        #[cfg(test)]
        if covering {
            self.covering_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.lock().rel_index_scan(
            db_id,
            table,
            index_object_id,
            lower,
            upper,
            projection,
            covering,
            snapshot,
        )
    }
}

impl StorageFile {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rel_index_scan(
        &mut self,
        db_id: u32,
        table: &str,
        index_object_id: u32,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        projection: Option<&[usize]>,
        covering: bool,
        snapshot: Option<ReadSnapshot>,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(db_id, table)?;
        let index = def
            .indexes
            .iter()
            .find(|i| i.object_id == index_object_id)
            .cloned()
            .ok_or_else(|| StorageError::InvalidConfig("unknown index".to_string()))?;
        if let Some(snap) = snapshot
            && self.version.schema_changed_after(def.object_id, snap)
        {
            return Err(StorageError::SnapshotSchemaChange(def.name));
        }
        let entries = {
            let mut ctx = self.rel_ctx();
            let index_tree = BTree {
                object_id: index.object_id,
                root: index.root_page,
            };
            index_tree.scan_range(&mut ctx, lower.as_deref(), upper.as_deref())?
        };
        // The leaf-value format depends on the index: an INCLUDE index
        // length-prefixes its locator (a Key locator's payload would
        // otherwise swallow the include bytes that follow it).
        let locator_of = |value: &[u8]| -> Locator {
            if index.include.is_empty() {
                index::decode_locator(value)
            } else {
                index::decode_leaf_value_with_include(value).0
            }
        };
        // Resolve each entry against the version store first (a snapshot
        // reader may need an entry's row served from an older image, or
        // dropped when its writer is invisible), then do the page lookups.
        // Rows the seek could not encounter — their index entry was moved or
        // removed by a writer the snapshot does not see — are appended from
        // their chain images; the executor's predicate re-checks every row,
        // so over-returning is filtered, never wrong.
        enum Entry {
            Physical(Vec<u8>),
            Image(Vec<u8>),
        }
        let merging = snapshot.is_some_and(|_| self.version.table_has_chains(def.object_id));
        let mut decided: Vec<Entry> = Vec::with_capacity(entries.len());
        let mut extra_images: Vec<Vec<u8>> = Vec::new();
        if let (Some(snap), true) = (snapshot, merging) {
            let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            for (_, value) in entries {
                let identity = match locator_of(&value) {
                    Locator::Key(pk) => pk,
                    Locator::Rid(rid) => rid_identity(rid),
                };
                match self.version.resolve(def.object_id, &identity, snap) {
                    None | Some(Resolved::Current) => decided.push(Entry::Physical(value)),
                    Some(Resolved::Image(image)) => decided.push(Entry::Image(image)),
                    Some(Resolved::Gone) => {}
                }
                seen.insert(identity);
            }
            extra_images = self.version.unseen_images(def.object_id, &seen, snap);
        } else {
            decided.extend(entries.into_iter().map(|(_, value)| Entry::Physical(value)));
        }

        let mut rows = Vec::with_capacity(decided.len());
        if covering {
            // Answer from the leaves alone: every projected column's original
            // value is stored in the entry (after the length-prefixed
            // locator), so the base-table lookup is skipped entirely. The
            // planner only chooses covering when projection ⊆ include; this
            // re-checks so a planner bug reads as an error, not wrong data.
            let projection = projection.ok_or_else(|| {
                StorageError::InvalidConfig("covering scan requires a projection".to_string())
            })?;
            let positions: Vec<usize> = projection
                .iter()
                .map(|col| {
                    index.include.iter().position(|i| i == col).ok_or_else(|| {
                        StorageError::InvalidConfig(format!(
                            "column {col} is not included in index '{}'",
                            index.name
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;
            for entry in decided {
                match entry {
                    Entry::Physical(value) => {
                        let (_, include_bytes) = index::decode_leaf_value_with_include(&value);
                        let decoded = index::decode_include(&schema, &index.include, include_bytes)
                            .map_err(|err| StorageError::InvalidFile(err.0))?;
                        rows.push(positions.iter().map(|&p| decoded[p].clone()).collect());
                    }
                    // A version image is the full row: project it directly.
                    Entry::Image(image) => {
                        rows.push(decode_row_projected(&schema, &image, projection)?);
                    }
                }
            }
        } else {
            let mut ctx = self.rel_ctx();
            if def.is_tree() {
                let base = BTree {
                    object_id: def.object_id,
                    root: def.root_page,
                };
                for entry in decided {
                    match entry {
                        Entry::Physical(value) => {
                            if let Locator::Key(pk) = locator_of(&value)
                                && let Some(row) = base.get(&mut ctx, &pk)?
                            {
                                rows.push(decode_projected(&schema, &row, projection)?);
                            }
                        }
                        Entry::Image(image) => {
                            rows.push(decode_projected(&schema, &image, projection)?);
                        }
                    }
                }
            } else {
                let heap = Heap {
                    object_id: def.object_id,
                    first_page: def.root_page,
                };
                for entry in decided {
                    match entry {
                        Entry::Physical(value) => {
                            if let Locator::Rid(rid) = locator_of(&value)
                                && let Some(row) = heap.read_row(&mut ctx, rid)?
                            {
                                rows.push(decode_projected(&schema, &row, projection)?);
                            }
                        }
                        Entry::Image(image) => {
                            rows.push(decode_projected(&schema, &image, projection)?);
                        }
                    }
                }
            }
        }
        for image in extra_images {
            rows.push(decode_projected(&schema, &image, projection)?);
        }
        let types = Self::projected_types(&schema, projection);
        self.resolve_overflow_rows(&types, &mut rows)?;
        Ok(rows)
    }
}
