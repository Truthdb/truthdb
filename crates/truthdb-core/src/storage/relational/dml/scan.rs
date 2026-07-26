use super::*;

impl Storage {
    /// Atomic snapshot scan: the whole table under one storage-lock hold
    /// (a versioned reader holds no table lock, so a sliced cursor could be
    /// restructured under it mid-walk), merged against the version store.
    pub(crate) fn rel_scan_snapshot(
        &self,
        db_id: u32,
        name: &str,
        projection: Option<&[usize]>,
        snapshot: ReadSnapshot,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.lock()
            .rel_scan_snapshot(db_id, name, projection, snapshot)
    }

    pub fn rel_get(
        &self,
        db_id: u32,
        name: &str,
        key_values: &[Datum],
    ) -> Result<Option<Vec<Datum>>, StorageError> {
        self.lock().rel_get(db_id, name, key_values)
    }

    pub fn rel_scan(&self, db_id: u32, name: &str) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.lock().rel_scan(db_id, name)
    }

    /// Scans a table in bounded slices, dropping the storage lock between them,
    /// so one large read stops blocking every other session for its whole
    /// duration.
    ///
    /// Only for readers that hold the table's lock — which a SELECT does at
    /// every isolation level except READ UNCOMMITTED, whose whole contract is
    /// that it sees in-flight change. Nothing pins a page between slices, so a
    /// concurrent writer could restructure the tree; the cursor checks each
    /// resumed page still belongs to the table and stops rather than read
    /// another object's rows. The integrity checks (FK probes, WITH CHECK) keep
    /// using [`Self::rel_scan`], whose single lock hold makes them atomic — a
    /// validation that missed a row because a page split mid-walk would admit a
    /// violating row.
    pub fn rel_scan_sliced(
        &self,
        db_id: u32,
        name: &str,
        budget: usize,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        let mut out = Vec::new();
        let mut cursor = ScanCursor::start();
        while !cursor.done() {
            cursor = self.rel_scan_slice(db_id, name, cursor, budget, None, &mut out)?;
        }
        Ok(out)
    }

    /// One slice of a scan: reads up to `budget` rows from `cursor`, appends
    /// them to `out`, and returns where to resume (`done()` once the table is
    /// exhausted).
    ///
    /// The storage lock is taken for this call alone, so a caller that loops
    /// lets other sessions in between slices. That is [`Self::rel_scan_sliced`]
    /// with the loop handed to the caller — for a reader that consumes rows as
    /// it goes rather than wanting them all at once — and it carries the same
    /// contract: only for readers holding the table's lock, since nothing pins
    /// a page between slices.
    pub(crate) fn rel_scan_slice(
        &self,
        db_id: u32,
        name: &str,
        cursor: ScanCursor,
        budget: usize,
        projection: Option<&[usize]>,
        out: &mut Vec<Vec<Datum>>,
    ) -> Result<ScanCursor, StorageError> {
        #[cfg(test)]
        {
            self.scan_slices
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.last_scan_width.store(
                projection.map_or(usize::MAX, <[usize]>::len),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        self.lock()
            .rel_scan_slice(db_id, name, cursor, budget, projection, out)
    }

    /// Test hook: a table's definition + schema, for driving a batched scan.
    #[cfg(test)]
    pub(crate) fn rel_def_for_test(
        &self,
        name: &str,
    ) -> Result<
        (
            crate::relstore::catalog::TableDef,
            crate::relstore::row::Schema,
        ),
        StorageError,
    > {
        self.lock().rel_def(catalog::DEFAULT_DATABASE_ID, name)
    }

    /// Test hook: runs `f` against a page context, taking the storage lock for
    /// that call only — the shape a batched scan uses, one acquisition per
    /// slice rather than one across the whole table.
    #[cfg(test)]
    pub(crate) fn with_rel_ctx_for_test<R>(
        &self,
        f: impl FnOnce(&mut crate::relstore::ctx::RelCtx<'_>) -> R,
    ) -> R {
        let mut guard = self.lock();
        let mut ctx = guard.rel_ctx();
        f(&mut ctx)
    }
}

impl StorageFile {
    /// Point lookup by primary key (clustered tables only).
    pub fn rel_get(
        &mut self,
        db_id: u32,
        name: &str,
        key_values: &[Datum],
    ) -> Result<Option<Vec<Datum>>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(db_id, name)?;
        if !def.is_tree() {
            return Err(StorageError::InvalidConfig(format!(
                "table '{name}' has no primary key"
            )));
        }
        if key_values.len() != def.key_columns.len() {
            return Err(StorageError::InvalidConfig(
                "wrong number of key values".to_string(),
            ));
        }
        // Encode each key column under its collation, exactly as the stored key
        // was, so a character PK lookup matches whatever the collation calls
        // equal.
        let mut key = Vec::new();
        for (value, &col) in key_values.iter().zip(&def.key_columns) {
            crate::relstore::key::encode_datum_collated(
                value,
                schema.columns[col].collation.as_deref(),
                &mut key,
            )?;
        }
        let tree = BTree {
            object_id: def.object_id,
            root: def.root_page,
        };
        let fetched = {
            let mut ctx = self.rel_ctx();
            tree.get(&mut ctx, &key)?
        };
        match fetched {
            Some(row) => {
                let mut rows = vec![decode_row(&schema, &row)?];
                let types = Self::projected_types(&schema, None);
                self.resolve_overflow_rows(&types, &mut rows)?;
                Ok(rows.pop())
            }
            None => Ok(None),
        }
    }

    /// Full scan: rows as typed datums (key order for trees, chain order
    /// for heaps).
    /// One slice of a scan: appends at most `budget` rows from `cursor` and
    /// returns where to resume. The caller loops, so the storage lock is taken
    /// once per slice instead of once for the whole table.
    pub(crate) fn rel_scan_slice(
        &mut self,
        db_id: u32,
        name: &str,
        cursor: ScanCursor,
        budget: usize,
        projection: Option<&[usize]>,
        out: &mut Vec<Vec<Datum>>,
    ) -> Result<ScanCursor, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(db_id, name)?;
        let mut ctx = self.rel_ctx();
        let mut raw: Vec<Vec<u8>> = Vec::new();
        let next = if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            let mut keyed = Vec::new();
            let next = tree.scan_from(&mut ctx, cursor, budget, &mut keyed)?;
            raw.extend(keyed.into_iter().map(|(_, row)| row));
            next
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let mut located = Vec::new();
            let next = heap.scan_from(&mut ctx, cursor, budget, &mut located)?;
            raw.extend(located.into_iter().map(|(_, row)| row));
            next
        };
        let start = out.len();
        for row in raw {
            out.push(decode_projected(&schema, &row, projection)?);
        }
        let _ = ctx;
        let types = Self::projected_types(&schema, projection);
        self.resolve_overflow_rows(&types, &mut out[start..])?;
        Ok(next)
    }

    pub fn rel_scan(&mut self, db_id: u32, name: &str) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(db_id, name)?;
        let mut ctx = self.rel_ctx();
        let raw: Vec<Vec<u8>> = if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            tree.scan(&mut ctx)?
                .into_iter()
                .map(|(_, row)| row)
                .collect()
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            heap.scan(&mut ctx)?
                .into_iter()
                .map(|(_, row)| row)
                .collect()
        };
        let mut rows = raw
            .into_iter()
            .map(|row| decode_row(&schema, &row).map_err(StorageError::from))
            .collect::<Result<Vec<_>, _>>()?;
        let types = Self::projected_types(&schema, None);
        self.resolve_overflow_rows(&types, &mut rows)?;
        Ok(rows)
    }

    /// Snapshot scan (Stage 13): the whole table read atomically under this
    /// one lock hold, each row resolved through the version store — a row
    /// last written by a transaction the snapshot cannot see is served from
    /// its chain image instead — plus the images of rows the physical walk
    /// could not encounter (deleted or re-keyed by writers the snapshot does
    /// not see). Atomic because a versioned reader holds no table lock, so a
    /// sliced cursor could be restructured under it mid-walk.
    pub(in crate::storage) fn rel_scan_snapshot(
        &mut self,
        db_id: u32,
        name: &str,
        projection: Option<&[usize]>,
        snapshot: ReadSnapshot,
    ) -> Result<Vec<Vec<Datum>>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(db_id, name)?;
        if self.version.schema_changed_after(def.object_id, snapshot) {
            return Err(StorageError::SnapshotSchemaChange(def.name));
        }
        let physical: Vec<(Vec<u8>, Vec<u8>)> = {
            let mut ctx = self.rel_ctx();
            if def.is_tree() {
                let tree = BTree {
                    object_id: def.object_id,
                    root: def.root_page,
                };
                tree.scan(&mut ctx)?
            } else {
                let heap = Heap {
                    object_id: def.object_id,
                    first_page: def.root_page,
                };
                heap.scan(&mut ctx)?
                    .into_iter()
                    .map(|(rid, row)| (rid_identity(rid), row))
                    .collect()
            }
        };
        let mut out = Vec::with_capacity(physical.len());
        if !self.version.table_has_chains(def.object_id) {
            for (_, row) in physical {
                out.push(decode_projected(&schema, &row, projection)?);
            }
            let types = Self::projected_types(&schema, projection);
            self.resolve_overflow_rows(&types, &mut out)?;
            return Ok(out);
        }
        let mut seen: std::collections::HashSet<Vec<u8>> =
            std::collections::HashSet::with_capacity(physical.len());
        for (identity, row) in physical {
            match self.version.resolve(def.object_id, &identity, snapshot) {
                None | Some(Resolved::Current) => {
                    out.push(decode_projected(&schema, &row, projection)?);
                }
                Some(Resolved::Image(image)) => {
                    out.push(decode_projected(&schema, &image, projection)?);
                }
                Some(Resolved::Gone) => {}
            }
            seen.insert(identity);
        }
        for image in self.version.unseen_images(def.object_id, &seen, snapshot) {
            out.push(decode_projected(&schema, &image, projection)?);
        }
        let types = Self::projected_types(&schema, projection);
        self.resolve_overflow_rows(&types, &mut out)?;
        Ok(out)
    }
}
