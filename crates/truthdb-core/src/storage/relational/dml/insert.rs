use super::*;

impl Storage {
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
}

impl StorageFile {
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
}
