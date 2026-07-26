use super::*;

impl Storage {
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
}
