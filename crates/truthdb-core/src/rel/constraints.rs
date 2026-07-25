use super::prelude::*;

/// Collects and validates a table's FOREIGN KEY constraints (column-level, then
/// table-level), assigning a name to unnamed ones. `check_names` are the names
/// already taken by the table's CHECK constraints so a FK cannot reuse one
/// (constraint names are unique across kinds).
pub(super) fn build_foreign_key_defs(
    db_id: u32,
    storage: &Storage,
    create: &CreateTable,
    columns: &[Column],
    table_name: &str,
    check_names: &[String],
) -> Result<Vec<catalog::ForeignKeyDef>, SqlError> {
    let raw = create
        .columns
        .iter()
        .flat_map(|c| c.foreign_keys.iter())
        .chain(create.foreign_keys.iter());

    // The parent's primary key (name, type) per PK column, in PK order. A
    // self-reference reads it from this CREATE; otherwise from the catalog.
    let self_pk = || -> Result<Vec<(String, ColumnType)>, SqlError> {
        create
            .primary_key
            .iter()
            .map(|k| {
                let col = columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&k.value))
                    .expect("primary key column bound");
                Ok((col.name.clone(), col.column_type))
            })
            .collect()
    };

    let mut names: Vec<String> = check_names.to_vec();
    let mut defs = Vec::new();
    for fk in raw {
        let parent_bare = strip_schema(&fk.parent.value);
        let is_self = parent_bare.eq_ignore_ascii_case(table_name);
        // Parent primary key: (column name, type) in PK order.
        let parent_pk: Vec<(String, ColumnType)> = if is_self {
            self_pk()?
        } else {
            let parent = resolve_table(storage, db_id, &fk.parent.value)
                .ok_or_else(|| SqlError::invalid_object(&fk.parent.value).at(fk.parent.span))?;
            let schema = parent
                .schema()
                .map_err(|e| map_storage_err(e, &parent.name))?;
            parent
                .key_columns
                .iter()
                .map(|&i| {
                    (
                        schema.columns[i].name.clone(),
                        schema.columns[i].column_type,
                    )
                })
                .collect()
        };
        let def = bind_foreign_key(fk, columns, table_name, &parent_pk, parent_bare, &names)?;
        names.push(def.name.clone());
        defs.push(def);
    }
    Ok(defs)
}

/// Validates one FOREIGN KEY against the parent's primary key and produces a
/// [`catalog::ForeignKeyDef`] whose child column indices are ordered to match
/// the parent's PK. Referenced columns must be exactly the parent PK (SQL
/// Server requires a unique/PK target); child and parent column types and
/// counts must match.
pub(super) fn bind_foreign_key(
    fk: &ForeignKey,
    columns: &[Column],
    table_name: &str,
    parent_pk: &[(String, ColumnType)],
    parent_bare: &str,
    existing_names: &[String],
) -> Result<catalog::ForeignKeyDef, SqlError> {
    let no_key = || {
        SqlError::new(
            1776,
            16,
            0,
            format!(
                "There are no primary or candidate keys in the referenced table '{parent_bare}' that match the referencing column list in the foreign key."
            ),
        )
        .at(fk.parent.span)
    };
    if parent_pk.is_empty() {
        return Err(no_key());
    }
    // Referenced parent columns (defaulting to the whole PK) paired with the
    // child columns positionally.
    let parent_cols: Vec<String> = if fk.parent_columns.is_empty() {
        parent_pk.iter().map(|(n, _)| n.clone()).collect()
    } else {
        fk.parent_columns.iter().map(|n| n.value.clone()).collect()
    };
    if fk.columns.len() != parent_cols.len() {
        return Err(SqlError::new(
            1776,
            16,
            0,
            "The number of referencing columns differs from the number of referenced columns.",
        )
        .at(fk.span));
    }
    // The referenced set must be exactly the parent PK (order-independent).
    if parent_cols.len() != parent_pk.len()
        || !parent_pk
            .iter()
            .all(|(pk, _)| parent_cols.iter().any(|c| c.eq_ignore_ascii_case(pk)))
    {
        return Err(no_key());
    }

    // Resolve child column indices and check each child/parent type matches.
    let child_index = |name: &Name| -> Result<usize, SqlError> {
        columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&name.value))
            .ok_or_else(|| SqlError::invalid_column(&name.value).at(name.span))
    };
    // For each parent PK column (in PK order), find the child column mapped to
    // it and record its index — so the stored order matches the parent PK.
    let mut ordered = Vec::with_capacity(parent_pk.len());
    for (pk_name, pk_type) in parent_pk {
        // Which referenced position names this PK column?
        let pos = parent_cols
            .iter()
            .position(|c| c.eq_ignore_ascii_case(pk_name))
            .ok_or_else(no_key)?;
        let child_col = &fk.columns[pos];
        let idx = child_index(child_col)?;
        if columns[idx].column_type != *pk_type {
            return Err(SqlError::new(
                1778,
                16,
                0,
                format!(
                    "Column '{table_name}.{}' is not the same data type as referencing column '{parent_bare}.{pk_name}' in the foreign key.",
                    columns[idx].name
                ),
            )
            .at(child_col.span));
        }
        ordered.push(idx);
    }

    let name = match &fk.name {
        Some(n) => {
            if existing_names
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&n.value))
            {
                return Err(SqlError::new(
                    2714,
                    16,
                    5,
                    format!(
                        "There is already an object named '{}' in the database.",
                        n.value
                    ),
                )
                .at(n.span));
            }
            n.value.clone()
        }
        None => {
            let mut seq = 0u32;
            loop {
                seq += 1;
                let candidate = format!("FK__{table_name}__{parent_bare}__{seq}");
                if !existing_names
                    .iter()
                    .any(|e| e.eq_ignore_ascii_case(&candidate))
                {
                    break candidate;
                }
            }
        }
    };
    Ok(catalog::ForeignKeyDef {
        name,
        columns: ordered,
        parent: parent_bare.to_string(),
    })
}

/// Collects a table's CHECK constraints (column-level, then table-level) and
/// binds each ([`bind_check`]), threading the running name list so unnamed
/// constraints get unique auto names and duplicate explicit names are caught.
pub(super) fn build_check_defs(
    create: &CreateTable,
    columns: &[Column],
    table_name: &str,
) -> Result<Vec<catalog::CheckDef>, SqlError> {
    let raw = create
        .columns
        .iter()
        .flat_map(|c| c.checks.iter())
        .chain(create.check_constraints.iter());

    let mut names: Vec<String> = Vec::new();
    let mut defs = Vec::new();
    for check in raw {
        let def = bind_check(check, columns, table_name, &names)?;
        names.push(def.name.clone());
        defs.push(def);
    }
    Ok(defs)
}

/// Validates one CHECK constraint against a table's columns and its existing
/// constraint names: the predicate must parse and reference only real columns
/// (207/4104); an explicit name must not collide (2714); an unnamed check is
/// assigned the first free `CK__<table>__<n>`.
pub(super) fn bind_check(
    check: &CheckConstraint,
    columns: &[Column],
    table_name: &str,
    existing_names: &[String],
) -> Result<catalog::CheckDef, SqlError> {
    let expr = truthdb_sql::parse_expr(&check.predicate)?;
    validate_check_columns(&expr, columns)?;
    let name = match &check.name {
        Some(n) => {
            if existing_names
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&n.value))
            {
                return Err(SqlError::new(
                    2714,
                    16,
                    5,
                    format!(
                        "There is already an object named '{}' in the database.",
                        n.value
                    ),
                )
                .at(n.span));
            }
            n.value.clone()
        }
        None => {
            let mut seq = 0u32;
            loop {
                seq += 1;
                let candidate = format!("CK__{table_name}__{seq}");
                if !existing_names
                    .iter()
                    .any(|e| e.eq_ignore_ascii_case(&candidate))
                {
                    break candidate;
                }
            }
        }
    };
    Ok(catalog::CheckDef {
        name,
        predicate: check.predicate.clone(),
    })
}

/// Rejects a CHECK predicate that references a column the table does not have
/// (error 207). Only column existence is checked here; type/boolean validity
/// is left to per-row evaluation.
pub(super) fn validate_check_columns(expr: &Expr, columns: &[Column]) -> Result<(), SqlError> {
    match &expr.kind {
        ExprKind::Column(name) => {
            // A CHECK may only reference columns of its own table by their bare
            // name. A multi-part identifier (`t.col`) can't be resolved by the
            // bare-name enforcement resolver, so reject it here (4104) rather
            // than accept a table that then rejects every INSERT with 207.
            if name.value.contains('.') {
                return Err(SqlError::new(
                    4104,
                    16,
                    1,
                    format!(
                        "The multi-part identifier \"{}\" could not be bound.",
                        name.value
                    ),
                )
                .at(name.span));
            }
            if columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&name.value))
            {
                Ok(())
            } else {
                Err(SqlError::invalid_column(&name.value).at(name.span))
            }
        }
        ExprKind::Unary { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. } => validate_check_columns(expr, columns),
        ExprKind::Binary { left, right, .. } => {
            validate_check_columns(left, columns)?;
            validate_check_columns(right, columns)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_check_columns(expr, columns)?;
            validate_check_columns(pattern, columns)
        }
        ExprKind::InList { expr, list, .. } => {
            validate_check_columns(expr, columns)?;
            list.iter()
                .try_for_each(|e| validate_check_columns(e, columns))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_check_columns(expr, columns)?;
            validate_check_columns(low, columns)?;
            validate_check_columns(high, columns)
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(op) = operand {
                validate_check_columns(op, columns)?;
            }
            for (when, then) in branches {
                validate_check_columns(when, columns)?;
                validate_check_columns(then, columns)?;
            }
            if let Some(e) = else_result {
                validate_check_columns(e, columns)?;
            }
            Ok(())
        }
        ExprKind::Function { args, .. } => args
            .iter()
            .try_for_each(|a| validate_check_columns(a, columns)),
        ExprKind::Aggregate { arg, .. } => arg
            .as_ref()
            .map_or(Ok(()), |a| validate_check_columns(a, columns)),
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => Ok(()),
        // Subqueries are not allowed in a CHECK constraint (SQL Server 1046).
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => {
            Err(SqlError::new(
                1046,
                15,
                1,
                "Subqueries are not allowed in this context. Only scalar expressions are allowed.",
            ))
        }
    }
}

/// Parses a table's stored CHECK predicates once (per statement) for row
/// enforcement, pairing each with its constraint name.
pub(super) fn parse_checks(def: &TableDef) -> Result<Vec<(String, Expr)>, SqlError> {
    def.check_constraints
        .iter()
        .map(|c| Ok((c.name.clone(), truthdb_sql::parse_expr(&c.predicate)?)))
        .collect()
}

/// Enforces CHECK constraints against a fully-built row (schema order). A
/// constraint passes on TRUE or UNKNOWN (NULL); FALSE is error 547.
#[allow(clippy::too_many_arguments)]
pub(super) fn enforce_checks(
    storage: &Storage,
    checks: &[(String, Expr)],
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
    eval_ctx: &EvalContext,
    verb: &str,
    database: &str,
    table: &str,
) -> Result<(), SqlError> {
    for (name, expr) in checks {
        // A user scalar function (or subquery) in the CHECK is folded against the
        // row before the pure evaluator runs, like the other clause positions.
        let bound;
        let expr = if expr_needs_binding(storage, eval_ctx.database_id, expr) {
            let outer = |n: &str| resolver.resolve(n);
            bound = substitute_correlated_in_expr(storage, expr, &outer, row, eval_ctx)?;
            &bound
        } else {
            expr
        };
        match eval::eval(expr, row, resolver, eval_ctx)? {
            SqlValue::Bool(false) => {
                return Err(SqlError::new(
                    547,
                    16,
                    0,
                    format!(
                        "The {verb} statement conflicted with the CHECK constraint \"{name}\". The conflict occurred in database \"{database}\", table \"dbo.{table}\".",
                    ),
                ));
            }
            SqlValue::Bool(true) | SqlValue::Null => {}
            _ => {
                return Err(SqlError::new(
                    4145,
                    15,
                    1,
                    format!(
                        "An expression of non-boolean type specified in a context where a condition is expected, near the CHECK constraint \"{name}\"."
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// A child row's referencing key for one foreign key (the FK columns in parent
/// primary-key order). `None` if any FK column is NULL — MATCH SIMPLE, which
/// skips enforcement (the NULL-FK trap).
pub(super) fn fk_key(fk: &catalog::ForeignKeyDef, row: &[Datum]) -> Option<Vec<Datum>> {
    let key: Vec<Datum> = fk.columns.iter().map(|&i| row[i].clone()).collect();
    if key.iter().any(|d| matches!(d, Datum::Null)) {
        None
    } else {
        Some(key)
    }
}

/// Whether a referencing `key` (parent PK order) exists in the parent — either
/// a committed parent row, or, for a self-reference, a sibling row in `batch`
/// (whose PK columns are `child.key_columns`).
pub(super) fn fk_parent_exists(
    storage: &Storage,
    fk: &catalog::ForeignKeyDef,
    key: &[Datum],
    child: &TableDef,
    batch: &[Vec<Datum>],
) -> Result<bool, SqlError> {
    if storage
        .rel_get(child.database_id, &fk.parent, key)
        .map_err(|e| map_storage_err(e, &fk.parent))?
        .is_some()
    {
        return Ok(true);
    }
    if fk.parent.eq_ignore_ascii_case(&child.name) && child.key_columns.len() == key.len() {
        // Fold both the referencing key and each sibling's PK by the parent PK
        // collation, so a case-insensitive self-reference matches a case-variant
        // sibling in the same statement — consistent with the folded `rel_get`
        // above (which handles the committed-row case).
        let key_coll: Vec<Option<String>> = child
            .key_columns
            .iter()
            .map(|&i| child.collations.get(i).cloned().flatten())
            .collect();
        let folded_key = collated_key(key, &key_coll);
        return Ok(batch.iter().any(|r| {
            let sibling: Vec<Datum> = child.key_columns.iter().map(|&i| r[i].clone()).collect();
            collated_key(&sibling, &key_coll) == folded_key
        }));
    }
    Ok(false)
}

/// The canonical name of a database id, for error text (the default
/// database's configured name when the id is unknown — a dropped database's
/// error still renders).
pub(super) fn database_name_of(storage: &Storage, db_id: u32) -> String {
    storage
        .rel_databases()
        .into_iter()
        .find(|(id, _)| *id == db_id)
        .map(|(_, name)| name)
        .unwrap_or_else(|| storage.default_database_name())
}

pub(super) fn fk_child_violation(database: &str, name: &str, verb: &str, parent: &str) -> SqlError {
    SqlError::new(
        547,
        16,
        0,
        format!(
            "The {verb} statement conflicted with the FOREIGN KEY constraint \"{name}\". The conflict occurred in database \"{database}\", table \"dbo.{parent}\".",
        ),
    )
}

/// Enforces this table's FOREIGN KEY constraints against a built child row:
/// each non-NULL referencing key must exist in the parent's primary key. For a
/// self-reference, a sibling row in the same statement (`batch`) also satisfies
/// it. A missing parent is error 547. `check_self_ref` skips self-referencing
/// foreign keys (an UPDATE validates those against its post-update snapshot,
/// since a pre-mutation probe would see stale rows).
pub(super) fn enforce_child_fks(
    storage: &Storage,
    def: &TableDef,
    row: &[Datum],
    batch: &[Vec<Datum>],
    verb: &str,
    check_self_ref: bool,
) -> Result<(), SqlError> {
    for fk in &def.foreign_keys {
        if !check_self_ref && fk.parent.eq_ignore_ascii_case(&def.name) {
            continue;
        }
        let Some(key) = fk_key(fk, row) else {
            continue; // NULL referencing column: not enforced
        };
        if !fk_parent_exists(storage, fk, &key, def, batch)? {
            return Err(fk_child_violation(
                &database_name_of(storage, def.database_id),
                &fk.name,
                verb,
                &fk.parent,
            ));
        }
    }
    Ok(())
}

/// A child index whose leading key columns are exactly the FK's child columns,
/// usable to probe for referencing rows by seeking the removed parent key
/// instead of scanning the whole child.
pub(super) fn fk_probe_index<'a>(
    child: &'a TableDef,
    fk: &catalog::ForeignKeyDef,
) -> Option<&'a catalog::IndexDef> {
    child.indexes.iter().find(|index| {
        index.columns.len() >= fk.columns.len()
            && index
                .columns
                .iter()
                .zip(&fk.columns)
                .all(|((col, _asc), &fk_col)| *col == fk_col)
    })
}

/// Whether the child FK columns and the referenced parent PK columns have the
/// same case sensitivity. The FK index fast path folds the probe key by the
/// *child* column collation (to match the child index's folded keys), while the
/// insert-time check (`rel_get`) and the scan fallback fold by the *parent* PK
/// collation; when they disagree (a mixed-collation FK) the fast path can miss a
/// reference, so it is only used when the collations match — otherwise the scan
/// fallback (parent collation, consistent with insert) handles it.
pub(super) fn fk_collations_match(
    child: &TableDef,
    fk: &catalog::ForeignKeyDef,
    parent: &TableDef,
) -> bool {
    fk.columns.len() == parent.key_columns.len()
        && fk.columns.iter().zip(&parent.key_columns).all(|(&c, &p)| {
            CollationSensitivity::from_optional(child.collations.get(c).and_then(|x| x.as_deref()))
                == CollationSensitivity::from_optional(
                    parent.collations.get(p).and_then(|x| x.as_deref()),
                )
        })
}

/// The error raised when a surviving child row references a removed parent key.
pub(super) fn reference_conflict(
    database: &str,
    verb: &str,
    fk_name: &str,
    child_name: &str,
) -> SqlError {
    SqlError::new(
        547,
        16,
        0,
        format!(
            "The {verb} statement conflicted with the REFERENCE constraint \"{fk_name}\". The conflict occurred in database \"{database}\", table \"dbo.{child_name}\"."
        ),
    )
}

/// Enforces NO ACTION on the parent side: no surviving child row may reference
/// any of `removed_keys` (parent primary-key values being deleted or vacated by
/// an UPDATE). A referencing child is error 547. When the child has an index on
/// the FK columns, each removed key is probed by an index seek; otherwise the
/// child is scanned.
pub(super) fn enforce_parent_fks(
    storage: &Storage,
    parent: &TableDef,
    removed_keys: &[Vec<Datum>],
    verb: &str,
    check_self_ref: bool,
) -> Result<(), SqlError> {
    if removed_keys.is_empty() {
        return Ok(());
    }
    // Fold the removed parent keys by the parent PK collation so the scan
    // fallback matches child references case-insensitively — the same folding the
    // index fast path gets from the child index's key encoding.
    let parent_key_coll: Vec<Option<String>> = parent
        .key_columns
        .iter()
        .map(|&i| parent.collations.get(i).cloned().flatten())
        .collect();
    let removed_folded: Vec<Vec<u8>> = removed_keys
        .iter()
        .map(|k| collated_key(k, &parent_key_coll))
        .collect();
    // Children live in the parent's database — cross-database foreign keys
    // do not exist, and lock analysis (fk_child_object_ids) filters the same
    // way; the two derivations must agree.
    let children: Vec<TableDef> = storage
        .rel_tables()
        .into_iter()
        .filter(|t| {
            t.database_id == parent.database_id
                && t.foreign_keys
                    .iter()
                    .any(|fk| fk.parent.eq_ignore_ascii_case(&parent.name))
        })
        .collect();
    for child in &children {
        let self_ref = child.name.eq_ignore_ascii_case(&parent.name);
        // A self-referencing table's own FKs are validated against the
        // post-update snapshot, not the pre-mutation child scan.
        if self_ref && !check_self_ref {
            continue;
        }
        for fk in &child.foreign_keys {
            if !fk.parent.eq_ignore_ascii_case(&parent.name) {
                continue;
            }
            // Fast path: an index on the FK columns lets us seek each removed
            // parent key instead of scanning the child. Not used for a
            // self-reference (whose own being-removed rows must be excluded). If
            // a key fails to encode (unexpected type mismatch), fall back to the
            // scan rather than risk missing a reference.
            if !self_ref
                && fk_collations_match(child, fk, parent)
                && let Some(index) = fk_probe_index(child, fk)
            {
                let mut handled = true;
                for key in removed_keys {
                    match crate::relstore::index::encode_index_prefix(
                        key,
                        &index.columns,
                        &child.collations,
                    ) {
                        Ok(lower) => {
                            let upper = crate::relstore::index::prefix_upper_bound(&lower);
                            let matches = storage
                                .rel_index_scan(
                                    child.database_id,
                                    &child.name,
                                    index.object_id,
                                    Some(lower),
                                    upper,
                                    None,
                                    false,
                                    // Integrity probe: must see the current
                                    // state, never a snapshot.
                                    None,
                                )
                                .map_err(|e| map_storage_err(e, &child.name))?;
                            if !matches.is_empty() {
                                return Err(reference_conflict(
                                    &database_name_of(storage, child.database_id),
                                    verb,
                                    &fk.name,
                                    &child.name,
                                ));
                            }
                        }
                        Err(_) => {
                            handled = false;
                            break;
                        }
                    }
                }
                if handled {
                    continue;
                }
            }
            // Fallback: scan the child and compare each row's FK key.
            let child_rows = storage
                .rel_scan(child.database_id, &child.name)
                .map_err(|e| map_storage_err(e, &child.name))?;
            for row in &child_rows {
                // A self-referencing row that is itself being removed does not
                // count as a surviving reference.
                if self_ref {
                    let pk: Vec<Datum> =
                        parent.key_columns.iter().map(|&i| row[i].clone()).collect();
                    if removed_folded.contains(&collated_key(&pk, &parent_key_coll)) {
                        continue;
                    }
                }
                let Some(key) = fk_key(fk, row) else {
                    continue;
                };
                if removed_folded.contains(&collated_key(&key, &parent_key_coll)) {
                    return Err(reference_conflict(
                        &database_name_of(storage, child.database_id),
                        verb,
                        &fk.name,
                        &child.name,
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The primary-key values of a row (in key-column order).
pub(super) fn pk_of(def: &TableDef, row: &[Datum]) -> Vec<Datum> {
    def.key_columns.iter().map(|&i| row[i].clone()).collect()
}

/// A key's collation-canonical bytes (`collations` parallel to `values`), for
/// comparing keys by value — the FK scan fallback and the self-reference checks.
///
/// This encodes exactly as the index key does, so "equal" here means what it
/// means to a seek: two keys match when the collation says they do, including
/// case- and accent-insensitively. Comparing the encoded bytes rather than the
/// values is what keeps the two definitions from drifting apart.
pub(super) fn collated_key(values: &[Datum], collations: &[Option<String>]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, value) in values.iter().enumerate() {
        // A key column always encodes; a type error here would mean the row did
        // not come from this table.
        let _ = crate::relstore::key::encode_datum_collated(
            value,
            collations.get(i).and_then(|c| c.as_deref()),
            &mut out,
        );
    }
    out
}

/// Maps a parsed [`DataType`] to a storage [`ColumnType`], validating length
/// bounds. `name` is only used for the length-overflow error message.
pub(super) fn data_type_to_column_type(
    data_type: &DataType,
    name: &str,
) -> Result<ColumnType, SqlError> {
    Ok(match data_type {
        DataType::TinyInt => ColumnType::TinyInt,
        DataType::SmallInt => ColumnType::SmallInt,
        DataType::Int => ColumnType::Int,
        DataType::BigInt => ColumnType::BigInt,
        DataType::Bit => ColumnType::Bit,
        DataType::Real => ColumnType::Real,
        DataType::Float => ColumnType::Float,
        DataType::Decimal { precision, scale } => ColumnType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Date => ColumnType::Date,
        DataType::Time => ColumnType::Time,
        DataType::DateTime2 => ColumnType::DateTime2,
        DataType::UniqueIdentifier => ColumnType::UniqueIdentifier,
        DataType::VarChar(n) => ColumnType::VarChar {
            max_len: length(*n, name)?,
        },
        DataType::NVarChar(n) => ColumnType::NVarChar {
            max_len: length(*n, name)?,
        },
        DataType::VarBinary(n) => ColumnType::VarBinary {
            max_len: length(*n, name)?,
        },
        DataType::VarCharMax => ColumnType::VarCharMax,
        DataType::NVarCharMax => ColumnType::NVarCharMax,
        DataType::VarBinaryMax => ColumnType::VarBinaryMax,
    })
}

/// Binds a declared column. A character column left without an explicit
/// `COLLATE` keeps `None` here and is resolved to the database default by
/// `rel_create_table`, the one point every CREATE TABLE passes through.
pub(super) fn bind_column(column: &ColumnDef) -> Result<Column, SqlError> {
    let column_type = data_type_to_column_type(&column.data_type, &column.name.value)?;
    // A COLLATE clause is only meaningful on character columns.
    if column.collation.is_some()
        && !matches!(
            column_type,
            ColumnType::VarChar { .. } | ColumnType::NVarChar { .. }
        )
    {
        return Err(SqlError::new(
            4536,
            16,
            1,
            format!(
                "COLLATE clause cannot be used on column '{}' because its data type is not character based.",
                column.name.value
            ),
        )
        .at(column.span));
    }
    // Columns are nullable by default (SQL Server ANSI default), PK columns
    // and explicit NOT NULL are not.
    let nullable = column.nullable.unwrap_or(!column.primary_key);
    Ok(Column {
        name: column.name.value.clone(),
        column_type,
        nullable,
        collation: column.collation.clone(),
    })
}

pub(super) fn length(n: u32, name: &str) -> Result<u16, SqlError> {
    u16::try_from(n).map_err(|_| {
        SqlError::new(
            131,
            15,
            2,
            format!("The size for column '{name}' exceeds the maximum."),
        )
    })
}
