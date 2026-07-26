/// Decodes a row, honouring a caller's projection: `None` means every column.
///
/// The read paths take `Option<&[usize]>` rather than always being handed a
/// full list, so a caller that wants the whole row neither builds one nor pays
/// to walk it.
fn decode_projected(
    schema: &Schema,
    row: &[u8],
    projection: Option<&[usize]>,
) -> Result<Vec<Datum>, crate::relstore::types::TypeError> {
    match projection {
        Some(projection) => decode_row_projected(schema, row, projection),
        None => decode_row(schema, row),
    }
}

/// Inserts one row's entries into every secondary index. A duplicate on a
/// UNIQUE index surfaces as a constraint error the SQL layer maps to 2601.
fn index_insert_row(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    indexes: &[IndexDef],
    schema: &Schema,
    collations: &[Option<String>],
    values: &[Datum],
    locator: &Locator,
) -> Result<(), StorageError> {
    for index in indexes {
        let index_key = index::encode_index_columns(values, &index.columns, collations)
            .map_err(|err| StorageError::InvalidConfig(err.0))?;
        let include = if index.include.is_empty() {
            None
        } else {
            Some(
                index::encode_include(schema, &index.include, values)
                    .map_err(|err| StorageError::InvalidConfig(err.0))?,
            )
        };
        let (key, value) = index::leaf_entry(&index_key, locator, index.unique, include.as_deref());
        let tree = BTree {
            object_id: index.object_id,
            root: index.root_page,
        };
        match tree.insert_unique(ctx, &mut OpMode::Txn(txn), &key, &value)? {
            TreeInsert::Inserted => {}
            TreeInsert::DuplicateKey => {
                return Err(StorageError::Constraint(format!(
                    "duplicate unique index '{}'",
                    index.name
                )));
            }
        }
    }
    Ok(())
}

/// Reindexes a set of updated rows: deletes every old entry first, then
/// inserts every new one, so a UNIQUE index tolerates value swaps within one
/// statement.
fn apply_index_updates(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    indexes: &[IndexDef],
    schema: &Schema,
    collations: &[Option<String>],
    ops: &[(Vec<Datum>, Locator, Vec<Datum>, Locator)],
) -> Result<(), StorageError> {
    if indexes.is_empty() {
        return Ok(());
    }
    for (old_values, old_locator, _, _) in ops {
        index_delete_row(ctx, txn, indexes, collations, old_values, old_locator)?;
    }
    for (_, _, new_values, new_locator) in ops {
        index_insert_row(
            ctx,
            txn,
            indexes,
            schema,
            collations,
            new_values,
            new_locator,
        )?;
    }
    Ok(())
}

/// Removes one row's entries from every secondary index.
fn index_delete_row(
    ctx: &mut RelCtx<'_>,
    txn: &mut TxnLink,
    indexes: &[IndexDef],
    collations: &[Option<String>],
    values: &[Datum],
    locator: &Locator,
) -> Result<(), StorageError> {
    for index in indexes {
        let index_key = index::encode_index_columns(values, &index.columns, collations)
            .map_err(|err| StorageError::InvalidConfig(err.0))?;
        let (key, _) = index::leaf_entry(&index_key, locator, index.unique, None);
        let tree = BTree {
            object_id: index.object_id,
            root: index.root_page,
        };
        tree.delete(ctx, &mut OpMode::Txn(txn), &key)?;
    }
    Ok(())
}

fn column_index(schema: &Schema, name: &str) -> Result<usize, StorageError> {
    column_index_by(schema, name)
}

fn column_index_by(schema: &Schema, name: &str) -> Result<usize, StorageError> {
    schema
        .columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| StorageError::InvalidConfig(format!("unknown column '{name}'")))
}

fn validate_not_null(schema: &Schema, values: &[Datum]) -> Result<(), StorageError> {
    for (column, value) in schema.columns.iter().zip(values) {
        if !column.nullable && value.is_null() {
            return Err(StorageError::Constraint(format!(
                "column '{}' does not allow NULL",
                column.name
            )));
        }
    }
    Ok(())
}

/// A row staged for insert: its clustered key (trees) and its encoding —
/// `None` when the table has (MAX) columns, whose oversize values must spill
/// inside the statement before the row can encode.
type StagedInsert = (Option<Vec<u8>>, Option<Vec<u8>>);
/// An in-place tree update: key, pre-encoded row or the values to encode
/// in-statement ((MAX) tables).
type StagedInPlace = (Vec<u8>, Option<Vec<u8>>, Option<Vec<Datum>>);
/// A re-keying tree update: old key, new key, then as [`StagedInPlace`].
type StagedRekey = (Vec<u8>, Vec<u8>, Option<Vec<u8>>, Option<Vec<Datum>>);
/// A heap update: RID, then as [`StagedInPlace`]'s tail.
type StagedHeapUpdate = (Rid, Option<Vec<u8>>, Option<Vec<Datum>>);

use super::super::*;

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

    pub fn rel_insert(
        &self,
        db_id: u32,
        name: &str,
        values: Vec<Datum>,
    ) -> Result<(), StorageError> {
        self.lock().rel_insert(db_id, name, values)
    }

    pub(crate) fn rel_insert_many(
        &self,
        db_id: u32,
        name: &str,
        rows: Vec<Vec<Datum>>,
        scope: &mut TxnScope,
    ) -> Result<(), StorageError> {
        self.lock().rel_insert_many(db_id, name, rows, scope)
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

    pub fn rel_delete_where(
        &self,
        db_id: u32,
        name: &str,
        column: &str,
        value: &Datum,
    ) -> Result<usize, StorageError> {
        self.lock().rel_delete_where(db_id, name, column, value)
    }

    pub fn rel_update_where(
        &self,
        db_id: u32,
        name: &str,
        column: &str,
        value: &Datum,
        assignments: &[(String, Datum)],
    ) -> Result<usize, StorageError> {
        self.lock()
            .rel_update_where(db_id, name, column, value, assignments)
    }

    pub(crate) fn rel_scan_located(
        &self,
        db_id: u32,
        name: &str,
    ) -> Result<Vec<(RowLocator, Vec<Datum>)>, StorageError> {
        self.lock().rel_scan_located(db_id, name)
    }

    /// SNAPSHOT-isolation DML target scan: snapshot rows plus a conflict mark
    /// per row whose current state a snapshot-invisible writer produced.
    pub(crate) fn rel_scan_located_snapshot(
        &self,
        db_id: u32,
        name: &str,
        snapshot: ReadSnapshot,
    ) -> Result<Vec<(RowLocator, Vec<Datum>, bool)>, StorageError> {
        self.lock().rel_scan_located_snapshot(db_id, name, snapshot)
    }

    pub(crate) fn rel_delete_located(
        &self,
        db_id: u32,
        name: &str,
        targets: Vec<(RowLocator, Vec<Datum>)>,
        scope: &mut TxnScope,
    ) -> Result<usize, StorageError> {
        self.lock().rel_delete_located(db_id, name, targets, scope)
    }

    pub(crate) fn rel_update_located(
        &self,
        db_id: u32,
        name: &str,
        updates: Vec<(RowLocator, Vec<Datum>, Vec<Datum>)>,
        scope: &mut TxnScope,
    ) -> Result<usize, StorageError> {
        self.lock().rel_update_located(db_id, name, updates, scope)
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

    pub fn rel_insert(
        &mut self,
        db_id: u32,
        name: &str,
        values: Vec<Datum>,
    ) -> Result<(), StorageError> {
        self.rel_insert_many(db_id, name, vec![values], &mut TxnScope::Auto)
    }

    /// Inserts many rows as ONE atomic statement: all rows land or none do
    /// (a later row's constraint failure rolls back the whole statement,
    /// matching T-SQL multi-row `INSERT ... VALUES` semantics).
    pub(crate) fn rel_insert_many(
        &mut self,
        db_id: u32,
        name: &str,
        rows: Vec<Vec<Datum>>,
        scope: &mut TxnScope,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let (def, schema) = self.rel_def(db_id, name)?;
        // Encode and validate every row up front (cheap failures before any
        // mutation), keeping the key alongside for tree tables. Rows with
        // (MAX) columns encode inside the statement instead: their oversize
        // values spill to overflow chains first, which needs the page
        // context.
        let has_max = schema.columns.iter().any(|c| c.column_type.is_max());
        let mut encoded: Vec<StagedInsert> = Vec::with_capacity(rows.len());
        for values in &rows {
            validate_not_null(&schema, values)?;
            let row = if has_max {
                None
            } else {
                Some(encode_row(&schema, values)?)
            };
            let key = if def.is_tree() {
                Some(encode_key(&schema, &def.key_columns, values)?)
            } else {
                None
            };
            encoded.push((key, row));
        }

        let indexes = def.indexes.clone();
        let collations = def.collations.clone();
        let counter_page = def.counter_page;
        let inserted = rows.len() as i64;
        let publishing = self.version.publishing();
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for ((key, pre), mut values) in encoded.into_iter().zip(rows.into_iter()) {
                    let key = key.expect("tree row has a key");
                    let row = match pre {
                        Some(row) => row,
                        None => {
                            Self::spill_max_values(ctx, &schema, &mut values)?;
                            encode_row(&schema, &values)?
                        }
                    };
                    match tree.insert_unique(ctx, &mut OpMode::Txn(txn), &key, &row)? {
                        TreeInsert::Inserted => {}
                        TreeInsert::DuplicateKey => {
                            return Err(StorageError::Constraint(
                                "duplicate primary key".to_string(),
                            ));
                        }
                    }
                    // Clustered rows locate by PK key.
                    index_insert_row(
                        ctx,
                        txn,
                        &indexes,
                        &schema,
                        &collations,
                        &values,
                        &Locator::Key(key.clone()),
                    )?;
                    if publishing {
                        txn.pending_versions.push(PendingVersion {
                            object_id: tree.object_id,
                            identity: key,
                            change: RowChange::Insert,
                        });
                    }
                }
                if let Some(page) = counter_page {
                    ctx.counter_add(txn, page, inserted)?;
                }
                Ok(())
            })
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for ((_, pre), mut values) in encoded.into_iter().zip(rows.into_iter()) {
                    let row = match pre {
                        Some(row) => row,
                        None => {
                            Self::spill_max_values(ctx, &schema, &mut values)?;
                            encode_row(&schema, &values)?
                        }
                    };
                    // Heap rows locate by their home RID.
                    let rid = heap.insert(ctx, txn, &row)?;
                    index_insert_row(
                        ctx,
                        txn,
                        &indexes,
                        &schema,
                        &collations,
                        &values,
                        &Locator::Rid(rid),
                    )?;
                    if publishing {
                        txn.pending_versions.push(PendingVersion {
                            object_id: heap.object_id,
                            identity: rid_identity(rid),
                            change: RowChange::Insert,
                        });
                    }
                }
                if let Some(page) = counter_page {
                    ctx.counter_add(txn, page, inserted)?;
                }
                Ok(())
            })
        }
    }

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

    /// Deletes every row where `column = value`; returns the count. Targets
    /// are materialized before any mutation (Halloween avoidance).
    ///
    /// Test-only surface (no SQL path reaches it): it compares UNRESOLVED
    /// rows, so a chained (MAX) value never matches, and it bypasses version
    /// staging. Resolve and stage before wiring it to anything real.
    pub fn rel_delete_where(
        &mut self,
        db_id: u32,
        name: &str,
        column: &str,
        value: &Datum,
    ) -> Result<usize, StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let (def, schema) = self.rel_def(db_id, name)?;
        let column_index = column_index(&schema, column)?;
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            let keys = {
                let mut ctx = self.rel_ctx();
                let mut keys = Vec::new();
                for (key, row) in tree.scan(&mut ctx)? {
                    if decode_row(&schema, &row)?[column_index] == *value {
                        keys.push(key);
                    }
                }
                keys
            };
            let count = keys.len();
            if count > 0 {
                self.rel_statement(move |ctx, txn| {
                    for key in &keys {
                        tree.delete(ctx, &mut OpMode::Txn(txn), key)?;
                    }
                    Ok(())
                })?;
            }
            Ok(count)
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let rids = {
                let mut ctx = self.rel_ctx();
                let mut rids = Vec::new();
                for (rid, row) in heap.scan(&mut ctx)? {
                    if decode_row(&schema, &row)?[column_index] == *value {
                        rids.push(rid);
                    }
                }
                rids
            };
            let count = rids.len();
            if count > 0 {
                self.rel_statement(move |ctx, txn| {
                    for rid in &rids {
                        heap.delete(ctx, txn, *rid)?;
                    }
                    Ok(())
                })?;
            }
            Ok(count)
        }
    }

    /// Updates every row where `column = value` with the given column
    /// assignments; returns the count. Key columns of clustered tables are
    /// immutable here (delete + insert to change a key).
    pub fn rel_update_where(
        &mut self,
        db_id: u32,
        name: &str,
        column: &str,
        value: &Datum,
        assignments: &[(String, Datum)],
    ) -> Result<usize, StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let (def, schema) = self.rel_def(db_id, name)?;
        let column_index = column_index(&schema, column)?;
        let mut set: Vec<(usize, Datum)> = Vec::new();
        for (set_name, set_value) in assignments {
            let index = column_index_by(&schema, set_name)?;
            if def.key_columns.contains(&index) {
                return Err(StorageError::InvalidConfig(format!(
                    "cannot update primary key column '{set_name}'"
                )));
            }
            set.push((index, set_value.clone()));
        }

        let apply_set = |mut values: Vec<Datum>| -> Vec<Datum> {
            for (index, new_value) in &set {
                values[*index] = new_value.clone();
            }
            values
        };

        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            let targets = {
                let mut ctx = self.rel_ctx();
                let mut targets = Vec::new();
                for (key, row) in tree.scan(&mut ctx)? {
                    let values = decode_row(&schema, &row)?;
                    if values[column_index] == *value {
                        targets.push((key, values));
                    }
                }
                targets
            };
            let count = targets.len();
            let mut encoded = Vec::with_capacity(count);
            for (key, values) in targets {
                let new_values = apply_set(values);
                validate_not_null(&schema, &new_values)?;
                encoded.push((key, encode_row(&schema, &new_values)?));
            }
            if count > 0 {
                self.rel_statement(move |ctx, txn| {
                    for (key, row) in &encoded {
                        tree.update(ctx, &mut OpMode::Txn(txn), key, row)?;
                    }
                    Ok(())
                })?;
            }
            Ok(count)
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let targets = {
                let mut ctx = self.rel_ctx();
                let mut targets = Vec::new();
                for (rid, row) in heap.scan(&mut ctx)? {
                    let values = decode_row(&schema, &row)?;
                    if values[column_index] == *value {
                        targets.push((rid, values));
                    }
                }
                targets
            };
            let count = targets.len();
            let mut encoded = Vec::with_capacity(count);
            for (rid, values) in targets {
                let new_values = apply_set(values);
                validate_not_null(&schema, &new_values)?;
                encoded.push((rid, encode_row(&schema, &new_values)?));
            }
            if count > 0 {
                self.rel_statement(move |ctx, txn| {
                    for (rid, row) in &encoded {
                        heap.update(ctx, txn, *rid, row)?;
                    }
                    Ok(())
                })?;
            }
            Ok(count)
        }
    }

    /// Full scan returning each row with an opaque locator that addresses it
    /// for a later targeted delete/update. The caller filters the whole
    /// materialized set before any mutation, so this is Halloween-safe by
    /// construction (matched targets are chosen from a snapshot of the table).
    pub(crate) fn rel_scan_located(
        &mut self,
        db_id: u32,
        name: &str,
    ) -> Result<Vec<(RowLocator, Vec<Datum>)>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(db_id, name)?;
        let mut ctx = self.rel_ctx();
        let mut out = Vec::new();
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            for (key, row) in tree.scan(&mut ctx)? {
                out.push((RowLocator::Key(key), decode_row(&schema, &row)?));
            }
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            for (rid, row) in heap.scan(&mut ctx)? {
                out.push((RowLocator::Rid(rid), decode_row(&schema, &row)?));
            }
        }
        let _ = ctx;
        if schema.columns.iter().any(|c| c.column_type.is_max()) {
            let types = Self::projected_types(&schema, None);
            let mut ctx = self.rel_ctx();
            for (_, row) in out.iter_mut() {
                for (column_type, value) in types.iter().zip(row.iter_mut()) {
                    if let Datum::OverflowRef {
                        total_len,
                        first_page,
                    } = *value
                    {
                        let bytes = overflow::read_chain(&mut ctx, first_page, total_len)?;
                        let base = match column_type {
                            ColumnType::VarCharMax => ColumnType::VarChar { max_len: u16::MAX },
                            ColumnType::NVarCharMax => ColumnType::NVarChar { max_len: u16::MAX },
                            _ => ColumnType::VarBinary { max_len: u16::MAX },
                        };
                        *value = Datum::decode_var(&base, &bytes)?;
                    }
                }
            }
        }
        Ok(out)
    }

    /// The SNAPSHOT-isolation DML target scan: like [`Self::rel_scan_located`]
    /// but rows are the snapshot's versions, and each carries a conflict mark
    /// when its current state was produced by a writer the snapshot cannot
    /// see — physically present rows served from an older image, and rows
    /// deleted (or re-keyed away) since the snapshot, whose locators are
    /// synthesized from their identities. Targeting a marked row is a 3960
    /// update conflict; the mark is computed here because only this layer
    /// sees both the physical state and the chains, atomically under the
    /// storage mutex. (A marked row can also mean a live writer is mid-flight
    /// on it — but the caller holds the statement's X locks, so a marked row
    /// it actually targets can only be a committed-invisible change.)
    pub(in crate::storage) fn rel_scan_located_snapshot(
        &mut self,
        db_id: u32,
        name: &str,
        snapshot: ReadSnapshot,
    ) -> Result<Vec<(RowLocator, Vec<Datum>, bool)>, StorageError> {
        self.ensure_rel_usable()?;
        let (def, schema) = self.rel_def(db_id, name)?;
        if self.version.schema_changed_after(def.object_id, snapshot) {
            return Err(StorageError::SnapshotSchemaChange(def.name));
        }
        let physical: Vec<(Vec<u8>, RowLocator, Vec<u8>)> = {
            let mut ctx = self.rel_ctx();
            if def.is_tree() {
                let tree = BTree {
                    object_id: def.object_id,
                    root: def.root_page,
                };
                tree.scan(&mut ctx)?
                    .into_iter()
                    .map(|(key, row)| (key.clone(), RowLocator::Key(key), row))
                    .collect()
            } else {
                let heap = Heap {
                    object_id: def.object_id,
                    first_page: def.root_page,
                };
                heap.scan(&mut ctx)?
                    .into_iter()
                    .map(|(rid, row)| (rid_identity(rid), RowLocator::Rid(rid), row))
                    .collect()
            }
        };
        let merging = self.version.table_has_chains(def.object_id);
        let mut out = Vec::with_capacity(physical.len());
        let mut seen: std::collections::HashSet<Vec<u8>> =
            std::collections::HashSet::with_capacity(if merging { physical.len() } else { 0 });
        for (identity, locator, row) in physical {
            if !merging {
                out.push((locator, decode_row(&schema, &row)?, false));
                continue;
            }
            match self.version.resolve(def.object_id, &identity, snapshot) {
                None | Some(Resolved::Current) => {
                    out.push((locator, decode_row(&schema, &row)?, false));
                }
                // Served from an older image: the current row belongs to a
                // writer the snapshot cannot see.
                Some(Resolved::Image(image)) => {
                    out.push((locator, decode_row(&schema, &image)?, true));
                }
                Some(Resolved::Gone) => {}
            }
            seen.insert(identity);
        }
        if merging {
            for (identity, image) in
                self.version
                    .unseen_images_with_identity(def.object_id, &seen, snapshot)
            {
                // Deleted or re-keyed since the snapshot: visible to it, but
                // its current state is gone — always a conflict if targeted.
                let locator = if def.is_tree() {
                    RowLocator::Key(identity)
                } else {
                    RowLocator::Rid(decode_rid_identity(&identity))
                };
                out.push((locator, decode_row(&schema, &image)?, true));
            }
        }
        if schema.columns.iter().any(|c| c.column_type.is_max()) {
            let types = Self::projected_types(&schema, None);
            let mut ctx = self.rel_ctx();
            for (_, row, _) in out.iter_mut() {
                for (column_type, value) in types.iter().zip(row.iter_mut()) {
                    if let Datum::OverflowRef {
                        total_len,
                        first_page,
                    } = *value
                    {
                        let bytes = overflow::read_chain(&mut ctx, first_page, total_len)?;
                        let base = match column_type {
                            ColumnType::VarCharMax => ColumnType::VarChar { max_len: u16::MAX },
                            ColumnType::NVarCharMax => ColumnType::NVarChar { max_len: u16::MAX },
                            _ => ColumnType::VarBinary { max_len: u16::MAX },
                        };
                        *value = Datum::decode_var(&base, &bytes)?;
                    }
                }
            }
        }
        Ok(out)
    }

    /// Deletes the located rows (each carrying its old values for index
    /// upkeep) in one atomic statement; returns the count.
    pub(crate) fn rel_delete_located(
        &mut self,
        db_id: u32,
        name: &str,
        targets: Vec<(RowLocator, Vec<Datum>)>,
        scope: &mut TxnScope,
    ) -> Result<usize, StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let (def, schema) = self.rel_def(db_id, name)?;
        let count = targets.len();
        if count == 0 {
            return Ok(0);
        }
        let indexes = def.indexes.clone();
        let collations = def.collations.clone();
        let counter_page = def.counter_page;
        // Version priors come from the physical pre-images inside the
        // statement (raw bytes, overflow refs intact), not a re-encode.
        let publishing = self.version.publishing();
        let _ = &schema;
        // The counter follows the rows actually removed inside the statement,
        // which the arms count as they go (a locator of the wrong kind is
        // skipped, exactly as the row loop skips it).
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                let mut removed: i64 = 0;
                for (loc, values) in &targets {
                    if let RowLocator::Key(key) = loc {
                        let prior = tree.delete(ctx, &mut OpMode::Txn(txn), key)?;
                        index_delete_row(
                            ctx,
                            txn,
                            &indexes,
                            &collations,
                            values,
                            &Locator::Key(key.clone()),
                        )?;
                        if publishing && let Some(prior) = prior {
                            txn.pending_versions.push(PendingVersion {
                                object_id: tree.object_id,
                                identity: key.clone(),
                                change: RowChange::Delete { prior },
                            });
                        }
                        removed += 1;
                    }
                }
                if let Some(page) = counter_page {
                    ctx.counter_add(txn, page, -removed)?;
                }
                Ok(())
            })?;
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            self.rel_statement_scoped(scope, move |ctx, txn| {
                let mut removed: i64 = 0;
                for (loc, values) in &targets {
                    if let RowLocator::Rid(rid) = loc {
                        let prior = if publishing {
                            heap.read_row(ctx, *rid)?
                        } else {
                            None
                        };
                        heap.delete(ctx, txn, *rid)?;
                        index_delete_row(
                            ctx,
                            txn,
                            &indexes,
                            &collations,
                            values,
                            &Locator::Rid(*rid),
                        )?;
                        if publishing && let Some(prior) = prior {
                            txn.pending_versions.push(PendingVersion {
                                object_id: heap.object_id,
                                identity: rid_identity(*rid),
                                change: RowChange::Delete { prior },
                            });
                        }
                        removed += 1;
                    }
                }
                if let Some(page) = counter_page {
                    ctx.counter_add(txn, page, -removed)?;
                }
                Ok(())
            })?;
        }
        Ok(count)
    }

    /// Applies full-row updates (each carrying its old and new values; already
    /// type-checked and NOT-NULL-checked by the caller) in one atomic
    /// statement. For a clustered table a row whose key changed is re-keyed
    /// (delete + insert with uniqueness enforced); heaps update in place by
    /// RID. Secondary indexes are maintained by deleting every old entry then
    /// inserting every new one (so a unique index tolerates value swaps).
    /// Returns the count.
    pub(crate) fn rel_update_located(
        &mut self,
        db_id: u32,
        name: &str,
        updates: Vec<(RowLocator, Vec<Datum>, Vec<Datum>)>,
        scope: &mut TxnScope,
    ) -> Result<usize, StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let (def, schema) = self.rel_def(db_id, name)?;
        let count = updates.len();
        if count == 0 {
            return Ok(0);
        }
        let indexes = def.indexes.clone();
        let collations = def.collations.clone();
        let publishing = self.version.publishing();
        let has_max = schema.columns.iter().any(|c| c.column_type.is_max());
        // (old values, old locator, new values, new locator) for index upkeep.
        let mut idx_ops: Vec<(Vec<Datum>, Locator, Vec<Datum>, Locator)> = Vec::new();
        if def.is_tree() {
            let tree = BTree {
                object_id: def.object_id,
                root: def.root_page,
            };
            // Partition into in-place (key unchanged) and re-key (key
            // changed). Version priors are captured INSIDE the statement
            // from the physical ops' returned pre-images — raw row bytes,
            // so a (MAX) image keeps its overflow reference instead of
            // re-inlining the whole value. (MAX) rows also encode inside,
            // after their oversize values spill.
            let mut in_place: Vec<StagedInPlace> = Vec::new();
            let mut rekey: Vec<StagedRekey> = Vec::new();
            for (loc, old_values, new_values) in updates {
                let RowLocator::Key(old_key) = loc else {
                    return Err(StorageError::InvalidConfig(
                        "expected key locator for clustered table".to_string(),
                    ));
                };
                validate_not_null(&schema, &new_values)?;
                let new_key = encode_key(&schema, &def.key_columns, &new_values)?;
                let (row, carried) = if has_max {
                    (None, Some(new_values.clone()))
                } else {
                    (Some(encode_row(&schema, &new_values)?), None)
                };
                if !indexes.is_empty() {
                    idx_ops.push((
                        old_values,
                        Locator::Key(old_key.clone()),
                        new_values,
                        Locator::Key(new_key.clone()),
                    ));
                }
                if new_key == old_key {
                    in_place.push((old_key, row, carried));
                } else {
                    rekey.push((old_key, new_key, row, carried));
                }
            }
            let object_id = tree.object_id;
            self.rel_statement_scoped(scope, move |ctx, txn| {
                let encode_new = |ctx: &mut RelCtx<'_>,
                                  row: Option<Vec<u8>>,
                                  carried: Option<Vec<Datum>>|
                 -> Result<Vec<u8>, StorageError> {
                    match row {
                        Some(row) => Ok(row),
                        None => {
                            let mut values = carried.expect("carried values for a MAX row");
                            Self::spill_max_values(ctx, &schema, &mut values)?;
                            Ok(encode_row(&schema, &values)?)
                        }
                    }
                };
                // Delete all re-keyed olds first so a new key may reuse one.
                for (old_key, _, _, _) in &rekey {
                    let prior = tree.delete(ctx, &mut OpMode::Txn(txn), old_key)?;
                    if publishing && let Some(prior) = prior {
                        txn.pending_versions.push(PendingVersion {
                            object_id,
                            identity: old_key.clone(),
                            change: RowChange::Delete { prior },
                        });
                    }
                }
                for (_, new_key, row, carried) in rekey {
                    let row = encode_new(ctx, row, carried)?;
                    match tree.insert_unique(ctx, &mut OpMode::Txn(txn), &new_key, &row)? {
                        TreeInsert::Inserted => {}
                        TreeInsert::DuplicateKey => {
                            return Err(StorageError::Constraint(
                                "duplicate primary key".to_string(),
                            ));
                        }
                    }
                    if publishing {
                        txn.pending_versions.push(PendingVersion {
                            object_id,
                            identity: new_key,
                            change: RowChange::Insert,
                        });
                    }
                }
                for (key, row, carried) in in_place {
                    let row = encode_new(ctx, row, carried)?;
                    let prior = tree.update(ctx, &mut OpMode::Txn(txn), &key, &row)?;
                    if publishing && let Some(prior) = prior {
                        txn.pending_versions.push(PendingVersion {
                            object_id,
                            identity: key,
                            change: RowChange::Update { prior },
                        });
                    }
                }
                apply_index_updates(ctx, txn, &indexes, &schema, &collations, &idx_ops)?;
                Ok(())
            })?;
        } else {
            let heap = Heap {
                object_id: def.object_id,
                first_page: def.root_page,
            };
            let mut encoded: Vec<StagedHeapUpdate> = Vec::with_capacity(count);
            for (loc, old_values, new_values) in updates {
                let RowLocator::Rid(rid) = loc else {
                    return Err(StorageError::InvalidConfig(
                        "expected rid locator for heap".to_string(),
                    ));
                };
                validate_not_null(&schema, &new_values)?;
                if has_max {
                    encoded.push((rid, None, Some(new_values.clone())));
                } else {
                    encoded.push((rid, Some(encode_row(&schema, &new_values)?), None));
                }
                if !indexes.is_empty() {
                    // Heap RIDs are stable across an update.
                    idx_ops.push((old_values, Locator::Rid(rid), new_values, Locator::Rid(rid)));
                }
            }
            self.rel_statement_scoped(scope, move |ctx, txn| {
                for (rid, pre, carried) in encoded {
                    let row = match pre {
                        Some(row) => row,
                        None => {
                            let mut values = carried.expect("carried values for a MAX row");
                            Self::spill_max_values(ctx, &schema, &mut values)?;
                            encode_row(&schema, &values)?
                        }
                    };
                    // The pre-image (raw bytes, overflow refs intact) is the
                    // version prior; read before the in-place update.
                    let prior = if publishing {
                        heap.read_row(ctx, rid)?
                    } else {
                        None
                    };
                    heap.update(ctx, txn, rid, &row)?;
                    if publishing && let Some(prior) = prior {
                        txn.pending_versions.push(PendingVersion {
                            object_id: heap.object_id,
                            identity: rid_identity(rid),
                            change: RowChange::Update { prior },
                        });
                    }
                }
                apply_index_updates(ctx, txn, &indexes, &schema, &collations, &idx_ops)?;
                Ok(())
            })?;
        }
        Ok(count)
    }

    /// Spills every (MAX) value above the inline threshold to an overflow
    /// chain, replacing the datum with a reference. Runs inside statement
    /// closures, before the row is encoded; chain pages are WAL-imaged, so
    /// they are crash-durable with the statement (and leak if it fails —
    /// the drop-table posture).
    pub(in crate::storage) fn spill_max_values(
        ctx: &mut RelCtx<'_>,
        schema: &Schema,
        values: &mut [Datum],
    ) -> Result<(), StorageError> {
        for (column, value) in schema.columns.iter().zip(values.iter_mut()) {
            if !column.column_type.is_max() || value.is_null() {
                continue;
            }
            let bytes = match value {
                Datum::VarChar(_) | Datum::NVarChar(_) | Datum::VarBinary(_) => value.encode_var(),
                _ => continue,
            };
            if bytes.len() <= OVERFLOW_INLINE_MAX {
                continue;
            }
            let first_page = overflow::write_chain(ctx, &bytes)?;
            *value = Datum::OverflowRef {
                total_len: bytes.len() as u64,
                first_page,
            };
        }
        Ok(())
    }

    /// Resolves overflow references in decoded rows back to their values.
    /// `types` must align with the rows' columns (the projection's types for
    /// projected reads).
    pub(in crate::storage) fn resolve_overflow_rows(
        &mut self,
        types: &[ColumnType],
        rows: &mut [Vec<Datum>],
    ) -> Result<(), StorageError> {
        if !types.iter().any(ColumnType::is_max) {
            return Ok(());
        }
        let mut ctx = self.rel_ctx();
        for row in rows.iter_mut() {
            for (column_type, value) in types.iter().zip(row.iter_mut()) {
                if let Datum::OverflowRef {
                    total_len,
                    first_page,
                } = *value
                {
                    let bytes = overflow::read_chain(&mut ctx, first_page, total_len)?;
                    let base = match column_type {
                        ColumnType::VarCharMax => ColumnType::VarChar { max_len: u16::MAX },
                        ColumnType::NVarCharMax => ColumnType::NVarChar { max_len: u16::MAX },
                        ColumnType::VarBinaryMax => ColumnType::VarBinary { max_len: u16::MAX },
                        other => {
                            return Err(StorageError::InvalidFile(format!(
                                "overflow reference under non-MAX column type {}",
                                other.name()
                            )));
                        }
                    };
                    *value = Datum::decode_var(&base, &bytes)?;
                }
            }
        }
        Ok(())
    }

    /// The column types a projection selects (`None` = every column).
    pub(in crate::storage) fn projected_types(
        schema: &Schema,
        projection: Option<&[usize]>,
    ) -> Vec<ColumnType> {
        match projection {
            None => schema.columns.iter().map(|c| c.column_type).collect(),
            Some(projection) => projection
                .iter()
                .map(|&i| schema.columns[i].column_type)
                .collect(),
        }
    }
}
