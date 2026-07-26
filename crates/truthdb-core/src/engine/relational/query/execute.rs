use super::super::prelude::*;

/// Whether a WHERE/ON predicate keeps a row. The predicate must be
/// boolean-typed (SQL Server 4145): a bare numeric/string expression is
/// rejected rather than silently coerced, and UNKNOWN drops the row (3VL).
pub(in crate::engine::relational) fn where_keeps(
    predicate: &Expr,
    row: &[SqlValue],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<bool, SqlError> {
    match eval::eval(predicate, row, resolver, eval_ctx)? {
        SqlValue::Bool(b) => Ok(b),
        SqlValue::Null => Ok(false),
        _ => Err(SqlError::new(
            4145,
            15,
            1,
            "An expression of non-boolean type specified in a context where a condition is expected, near 'WHERE'.",
        )
        .at(predicate.span)),
    }
}

pub(in crate::engine::relational) fn exec_select(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    // A top-level assignment SELECT is routed to exec_select_assign; one reaching
    // here has been nested in a subquery / derived table / CTE, which is invalid.
    if select
        .items
        .iter()
        .any(|i| matches!(i, SelectItem::Assign { .. }))
    {
        return Err(SqlError::message_only(
            141,
            "A SELECT that assigns to a variable cannot be used inside a query expression.",
        ));
    }
    // A single-table scan whose every output column comes from the schema needs
    // no stage that waits for the whole input, so it runs row by row instead.
    // The gate goes before the CTE expansion and the subquery rewrite because it
    // excludes both — and the rewrite would otherwise clone the whole statement
    // and run any subquery eagerly.
    if let Some(plan) = scan_plan(storage, select, eval_ctx) {
        return scan_select(storage, &plan, select, eval_ctx);
    }

    // Inline any WITH common table expressions (as derived tables) first.
    let expanded;
    let select = if select.ctes.is_empty() {
        select
    } else {
        expanded = expand_ctes(select);
        &expanded
    };
    // Resolve each (uncorrelated) subquery once, up front, replacing it with a
    // literal / boolean / value-list so the rest of execution is subquery-free.
    let rewritten = rewrite_select_subqueries(storage, select, eval_ctx)?;
    let select = &rewritten;

    let source = build_source(
        storage,
        select.from.as_ref(),
        &select.where_clause,
        eval_ctx,
    )?;
    let resolver = source.scope();
    let types = source.types();

    // WHERE. The predicate must be boolean-typed (SQL Server 4145): a bare
    // numeric/string expression is rejected rather than silently coerced. Any
    // subquery left in the (already-rewritten) predicate is correlated: bind the
    // outer row into it and evaluate per row.
    let where_correlated = select
        .where_clause
        .as_ref()
        .is_some_and(|w| expr_needs_binding(storage, eval_ctx.database_id, w));
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    // One row: filter it into `rows` or drop it. Shared by both input shapes.
    let take = |row: Vec<Datum>, rows: &mut Vec<Vec<Datum>>| -> Result<(), SqlError> {
        check_cancelled()?;
        let sql_row = row_values(&row, &types);
        let keep = match &select.where_clause {
            None => true,
            Some(predicate) => {
                let bound;
                let predicate = if where_correlated {
                    bound = substitute_correlated_in_expr(
                        storage,
                        predicate,
                        &|name| resolver.resolve(name),
                        &sql_row,
                        eval_ctx,
                    )?;
                    &bound
                } else {
                    predicate
                };
                where_keeps(predicate, &sql_row, &resolver, eval_ctx)?
            }
        };
        if keep {
            rows.push(row);
        }
        Ok(())
    };
    // A scanned base table streams in slices: peak input memory is one slice
    // plus the survivors, not the table (Stage 8 streaming scans). Everything
    // downstream (aggregate/sort/join operators) bounds or spills its own
    // working set, so a filtered pipeline is bounded end to end.
    match source.rows {
        SourceRows::Materialized(input) => {
            for row in input {
                take(row, &mut rows)?;
            }
        }
        SourceRows::Scan(mut stream) => {
            while let Some(slice) = stream.next_slice(storage)? {
                for row in slice {
                    take(row, &mut rows)?;
                }
            }
        }
    }

    // A grouped/aggregated or DISTINCT query projects first (its ORDER BY
    // references the output), while a plain query orders the source rows so it
    // can order by columns that are not in the SELECT list.
    if aggregate::is_aggregated(select) || select.distinct {
        let mut out = if aggregate::is_aggregated(select) {
            aggregate::execute(storage, select, &rows, &types, &resolver, eval_ctx)?
        } else {
            project(
                storage,
                &select.items,
                &source.columns,
                &rows,
                &types,
                &resolver,
                eval_ctx,
            )?
        };
        if select.distinct {
            // Each output column's collation (resolved back to its source column;
            // a computed/aliased column has no source column → the case-
            // insensitive default), so DISTINCT honours an explicit `_CS`/`_BIN`
            // column exactly like GROUP BY / COUNT(DISTINCT) do.
            let out_sens: Vec<CollationSensitivity> = out
                .columns
                .iter()
                .map(|c| {
                    resolver
                        .resolve(&c.name)
                        .map(|i| resolver.collation(i))
                        .unwrap_or(CollationSensitivity::default_collation())
                })
                .collect();
            dedup_rows(storage, &mut out, &out_sens)?;
        }
        order_output(storage, &mut out, &select.order_by, eval_ctx)?;
        if let Some(top) = select.top {
            out.rows.truncate(top as usize);
        }
        return Ok(out);
    }

    // ORDER BY (evaluated against the source row; stable so equal keys keep
    // input order). Spills to temp extents when the input exceeds the budget.
    if !select.order_by.is_empty() {
        let order_by = order_by_with_aliases(&select.order_by, &select.items, &resolver)?;
        rows = order_rows(
            storage,
            rows,
            &order_by,
            &types,
            &source.collations,
            &resolver,
            eval_ctx,
        )?;
    }

    // TOP.
    if let Some(top) = select.top {
        rows.truncate(top as usize);
    }

    project(
        storage,
        &select.items,
        &source.columns,
        &rows,
        &types,
        &resolver,
        eval_ctx,
    )
}

/// `SELECT @a = expr, @b = expr2 [FROM ...]` — an assignment SELECT. The value
/// expressions are projected as an ordinary result set; each variable then
/// takes the value from the *last* row the query produces (SQL Server's
/// documented behaviour for the final value). Zero rows leave the variables
/// unchanged. A value that reads a variable being assigned in the same
/// statement (running aggregation, cross-referencing targets) is rejected
/// rather than evaluated against the pre-statement snapshot, which would give a
/// result that silently differs from SQL Server's per-row assignment.
pub(in crate::engine::relational) fn exec_select_assign(
    storage: &Storage,
    select: &Select,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // Every target must be a declared variable; capture their declared types.
    let mut targets: Vec<(String, ColumnType)> = Vec::with_capacity(select.items.len());
    for item in &select.items {
        let SelectItem::Assign { target, .. } = item else {
            // The dispatcher only routes here when every item is an assignment.
            unreachable!("assignment SELECT has a non-assignment item");
        };
        let column_type = txn_ctx
            .variables
            .get(target)
            .map(|(t, _)| *t)
            .ok_or_else(|| undeclared_variable_err(target))?;
        targets.push((target.clone(), column_type));
    }

    // Every value is evaluated against the variables' pre-statement values, so a
    // value that references a variable being assigned here would silently
    // diverge from SQL Server's per-row / left-to-right assignment (running
    // aggregation, cross-referencing targets). Reject those rather than compute
    // a wrong result; the caller can use SET or a set-based aggregate instead.
    let target_names: std::collections::HashSet<&str> =
        targets.iter().map(|(name, _)| name.as_str()).collect();
    for item in &select.items {
        let SelectItem::Assign { value, .. } = item else {
            unreachable!()
        };
        if expr_uses_local_var(value, &target_names) {
            return Err(SqlError::message_only(
                141,
                "An assignment SELECT cannot reference a variable it is assigning in the same statement; use SET or a set-based aggregate.",
            ));
        }
    }

    // Project the value expressions as an ordinary result set.
    let projected = Select {
        items: select
            .items
            .iter()
            .map(|item| {
                let SelectItem::Assign { value, .. } = item else {
                    unreachable!()
                };
                SelectItem::Expr {
                    expr: value.clone(),
                    alias: None,
                }
            })
            .collect(),
        ..select.clone()
    };
    let rowset = exec_select(storage, &projected, &txn_ctx.eval_context())?;

    // Assign the last row's values (SQL Server: the variable holds the value
    // from the final row). No rows -> variables keep their current values.
    if let Some(last) = rowset.rows.last() {
        for (index, (name, column_type)) in targets.iter().enumerate() {
            let produced = value::datum_to_sql(&last[index], &rowset.columns[index].column_type);
            let datum = value::sql_to_datum(&produced, column_type, name)?;
            let coerced = value::datum_to_sql(&datum, column_type);
            txn_ctx
                .variables
                .insert(name.clone(), (*column_type, coerced));
        }
    }
    // SQL Server counts the rows an assignment SELECT processed: the DONE
    // carries it and `@@ROWCOUNT` reports it.
    Ok(StatementResult::RowsAffected(rowset.rows.len() as u64))
}

/// Removes duplicate output rows (SELECT DISTINCT), keeping first occurrence.
/// NULLs are equal to each other (`Datum` equality), matching SQL Server.
pub(in crate::engine::relational) fn dedup_rows(
    storage: &Storage,
    rowset: &mut RowSet,
    sensitivities: &[CollationSensitivity],
) -> Result<(), SqlError> {
    // Hash-based DISTINCT — O(n) instead of the old O(n²) linear scan. Each
    // output column is single-typed (projection coerced it), so `HashKey`'s
    // `order_key_cmp` equality agrees with the former `Vec<Datum>` equality for
    // every realistic input. (Edge: two `float` NaN rows now collapse to one —
    // `order_key_cmp` treats NaN as equal, like GROUP BY already did — where the
    // old raw `Datum` `==` kept them distinct.)
    let types: Vec<ColumnType> = rowset.columns.iter().map(|c| c.column_type).collect();
    // DISTINCT folds string columns by each output column's collation
    // (`sensitivities`, parallel to the columns), so a `_CI` column folds case
    // and a `_CS`/`_BIN` column stays exact — consistent with GROUP BY and
    // COUNT(DISTINCT). `dedup_key` keeps the original row for output.
    let dedup_key = |row: &[Datum]| hash::fold_hash_key(&row_values(row, &types), sensitivities);
    let approx: usize = rowset.rows.iter().map(|r| approx_row_bytes(r)).sum();
    if approx <= sort_budget() {
        // In-memory: keep first-appearance order (DISTINCT without ORDER BY has
        // no guaranteed order, but this is the least-surprising small-set result).
        let mut seen: std::collections::HashSet<hash::HashKey> = std::collections::HashSet::new();
        rowset
            .rows
            .retain(|row| seen.insert(hash::HashKey(dedup_key(row))));
        return Ok(());
    }

    // Grace-hash DISTINCT: partition rows by key hash into temp extents (equal
    // rows share a partition), then dedup each partition in memory. The per-
    // partition dedup set is bounded to ~one partition instead of the whole
    // input. Output is by partition (immaterial — a spilling DISTINCT is not
    // order-sensitive; any ORDER BY runs afterward).
    let partitions = (approx / sort_budget() + 1).max(2);
    let partition_of = |key: &[SqlValue]| -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        hash::HashKey(key.to_vec()).hash(&mut hasher);
        (hasher.finish() % partitions as u64) as usize
    };
    let spill_err = |e| map_storage_err(e, "<distinct spill>");
    let mut parts: Vec<_> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    for row in &rowset.rows {
        // Partition by the *folded* key so case-insensitive-equal rows land in
        // the same partition (else a cross-partition duplicate is missed); the
        // stored row stays original for output.
        let key = dedup_key(row);
        parts[partition_of(&key)]
            .write_row(row)
            .map_err(spill_err)?;
    }
    for part in parts.iter_mut() {
        part.finish_writing().map_err(spill_err)?;
    }
    let mut out: Vec<Vec<Datum>> = Vec::new();
    for part in parts.iter_mut() {
        let mut seen: std::collections::HashSet<hash::HashKey> = std::collections::HashSet::new();
        let mut reader = part.reader();
        while let Some(row) = reader.next_row().map_err(spill_err)? {
            if seen.insert(hash::HashKey(dedup_key(&row))) {
                out.push(row);
            }
        }
    }
    rowset.rows = out;
    Ok(())
}

/// Orders an output RowSet by ORDER BY items referencing the output: a bare
/// integer is a 1-based output-column ordinal; any other expression is
/// evaluated against the output row (its columns are the resolver). Uses
/// code-point ordering (NULLs first), stable.
pub(in crate::engine::relational) fn order_output(
    storage: &Storage,
    rowset: &mut RowSet,
    order_by: &[OrderItem],
    eval_ctx: &EvalContext,
) -> Result<(), SqlError> {
    if order_by.is_empty() {
        return Ok(());
    }
    let names: Vec<String> = rowset.columns.iter().map(|c| c.name.clone()).collect();
    let scope = OutputScope { names };
    let types: Vec<ColumnType> = rowset.columns.iter().map(|c| c.column_type).collect();
    let mut keyed: Vec<(Vec<SqlValue>, usize)> = Vec::with_capacity(rowset.rows.len());
    for (index, row) in rowset.rows.iter().enumerate() {
        let sql_row = row_values(row, &types);
        let mut key = Vec::with_capacity(order_by.len());
        for item in order_by {
            let value = if let ExprKind::Int(n) = &item.expr.kind {
                let ordinal = usize::try_from(*n)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .filter(|&i| i < sql_row.len())
                    .ok_or_else(|| {
                        SqlError::new(
                            108,
                            16,
                            1,
                            format!("The ORDER BY position number {n} is out of range."),
                        )
                    })?;
                sql_row[ordinal].clone()
            } else {
                eval_maybe_bound(storage, &item.expr, &sql_row, &scope, eval_ctx)?
            };
            key.push(value);
        }
        keyed.push((key, index));
    }
    keyed.sort_by(|(ka, ia), (kb, ib)| {
        for (index, item) in order_by.iter().enumerate() {
            let mut ord = order_key_cmp(&ka[index], &kb[index]);
            if item.descending {
                ord = ord.reverse();
            }
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        ia.cmp(ib)
    });
    rowset.rows = keyed.iter().map(|(_, i)| rowset.rows[*i].clone()).collect();
    Ok(())
}

pub(in crate::engine::relational) fn project(
    storage: &Storage,
    items: &[SelectItem],
    source_columns: &[ResultColumn],
    rows: &[Vec<Datum>],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    // Output column plan: a source column (typed, pass-through) or a
    // computed expression (evaluated then typed by inference).
    enum Proj<'a> {
        SourceColumn { index: usize, name: String },
        Expr { name: String, expr: &'a Expr },
    }
    let mut projs = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => {
                for (index, column) in source_columns.iter().enumerate() {
                    projs.push(Proj::SourceColumn {
                        index,
                        name: column.name.clone(),
                    });
                }
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                let indices = resolver.indices_for_qualifier(&qualifier.value);
                if indices.is_empty() {
                    return Err(SqlError::new(
                        4104,
                        16,
                        1,
                        format!(
                            "The multi-part identifier \"{}.*\" could not be bound.",
                            qualifier.value
                        ),
                    )
                    .at(qualifier.span));
                }
                for index in indices {
                    projs.push(Proj::SourceColumn {
                        index,
                        name: source_columns[index].name.clone(),
                    });
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| bare_column_name(expr))
                    .unwrap_or_default();
                match bare_column_index(expr, resolver) {
                    // A bare column still carries its resolved output name so an
                    // `AS alias` (or the referenced name's casing) is preserved.
                    Some(index) => projs.push(Proj::SourceColumn { index, name }),
                    None => projs.push(Proj::Expr { name, expr }),
                }
            }
            // Assignment SELECTs are rewritten to Expr items before projection.
            SelectItem::Assign { .. } => {
                unreachable!("assignment SELECT handled before projection")
            }
        }
    }

    // Precompute all row values once for expression evaluation.
    let row_sql: Vec<Vec<SqlValue>> = rows.iter().map(|r| row_values(r, types)).collect();

    let mut columns = Vec::with_capacity(projs.len());
    let mut out_rows: Vec<Vec<Datum>> = vec![Vec::with_capacity(projs.len()); rows.len()];
    for proj in &projs {
        match proj {
            Proj::SourceColumn { index, name } => {
                columns.push(ResultColumn {
                    name: name.clone(),
                    column_type: source_columns[*index].column_type,
                });
                for (out, row) in out_rows.iter_mut().zip(rows) {
                    out.push(row[*index].clone());
                }
            }
            Proj::Expr { name, expr } => {
                // Evaluate the column for every row, then infer one type. A
                // subquery still present here is correlated (the rewrite pass
                // left it for the per-row bind): substitute the outer row's
                // values in, making it uncorrelated for that row.
                let correlated = expr_needs_binding(storage, eval_ctx.database_id, expr);
                let mut values = Vec::with_capacity(rows.len());
                for row in &row_sql {
                    let bound;
                    let expr = if correlated {
                        bound = substitute_correlated_in_expr(
                            storage,
                            expr,
                            &|name| resolver.resolve(name),
                            row,
                            eval_ctx,
                        )?;
                        &bound
                    } else {
                        expr
                    };
                    values.push(eval::eval(expr, row, resolver, eval_ctx)?);
                }
                let column_type = value::infer_type(&values);
                for (out, value) in out_rows.iter_mut().zip(&values) {
                    // Coerce each value to the inferred column type (e.g. all
                    // decimals to the widest scale) so the column is uniform.
                    out.push(value::sql_to_datum(value, &column_type, name)?);
                }
                columns.push(ResultColumn {
                    name: name.clone(),
                    column_type,
                });
            }
        }
    }
    Ok(RowSet {
        columns,
        rows: out_rows,
    })
}

pub(in crate::engine::relational) fn bare_column_name(expr: &Expr) -> Option<String> {
    match &expr.kind {
        // A qualified `t.col` reference outputs the bare column name.
        ExprKind::Column(name) => Some(name.value.rsplit('.').next().unwrap_or("").to_string()),
        _ => None,
    }
}

pub(in crate::engine::relational) fn bare_column_index(
    expr: &Expr,
    scope: &JoinScope,
) -> Option<usize> {
    match &expr.kind {
        ExprKind::Column(name) => scope.resolve(&name.value),
        _ => None,
    }
}
