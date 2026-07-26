use super::super::prelude::*;

#[cfg(test)]
thread_local! {
    /// Test hook: makes [`scan_plan`] decline everything, so a test can run one
    /// query down both paths and compare. Thread-local, not a `static`: a batch
    /// runs on one thread, and the suite runs its tests in parallel in a single
    /// binary — a global would force every other test's queries onto the
    /// collecting path for as long as this one was set.
    static FORCE_COLLECTING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(in crate::engine::relational) fn force_collecting() -> bool {
    FORCE_COLLECTING.with(|c| c.get())
}

/// Runs `f` with [`scan_select`]'s path disabled, so `f`'s queries take the
/// collecting path. Restores on drop, so a panicking `f` cannot leave the flag
/// set for the next test to run on this thread.
#[cfg(test)]
pub(crate) fn without_scan_path<R>(f: impl FnOnce() -> R) -> R {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            FORCE_COLLECTING.with(|c| c.set(false));
        }
    }
    FORCE_COLLECTING.with(|c| c.set(true));
    let _restore = Restore;
    f()
}

/// A `SELECT` that can be answered a row at a time: one base table, scanned,
/// filtered and projected without any stage that must see the whole input
/// first. Produced by [`scan_plan`], consumed by [`scan_select`].
pub(in crate::engine::relational) struct ScanPlan {
    /// The base table's database — the namespace the scan reads it from.
    db_id: u32,
    /// The base table's catalog name — what the scan reads.
    table: String,
    /// How to read it: the planner's choice, made once (see [`scan_plan`]).
    pub(in crate::engine::relational) access: plan::AccessPath,
    /// The schema columns this query reads at all — its projection plus the
    /// WHERE clause's — ascending and distinct. Everything below is expressed
    /// in *these* coordinates, not the table's: the storage layer decodes only
    /// these, so a scanned row has exactly this width.
    needed: Vec<usize>,
    /// Type of each needed column, parallel to `needed`.
    types: Vec<ColumnType>,
    /// Resolves the WHERE clause's column references against a scanned row.
    resolver: JoinScope,
    /// Output columns. Every type here is the schema's, which is what makes the
    /// shape work: a computed column's type comes from `infer_type` over every
    /// value in it, so it cannot be known until the last row has been seen.
    pub(in crate::engine::relational) columns: Vec<ResultColumn>,
    /// The scanned-row position each output column reads (an index into
    /// `needed`, not into the table's columns).
    picks: Vec<usize>,
    /// An index seek that is *covering*: every needed column's original value
    /// is stored in the index leaves (`INCLUDE`), so the scan answers from the
    /// index alone — no per-row base-table lookup. Never true for a table
    /// scan.
    pub(in crate::engine::relational) covering: bool,
}

/// Recognises the shape [`scan_select`] can run, or `None` for everything else
/// — which then takes the collecting path unchanged.
///
/// Every rejection here is a stage that cannot answer until it has the whole
/// input: `ORDER BY` sorts it, `DISTINCT` dedups it, an aggregate folds it, a
/// computed column types it, and a join/derived table/CTE/view is another query
/// underneath. An `IndexSeek` is excluded for the opposite reason — it reads
/// *less* than a scan, and reading the whole table to filter it down would
/// trade the planner's work for this one's.
pub(in crate::engine::relational) fn scan_plan(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Option<ScanPlan> {
    #[cfg(test)]
    if force_collecting() {
        return None;
    }
    if !select.ctes.is_empty()
        || select.distinct
        || !select.order_by.is_empty()
        || aggregate::is_aggregated(select)
    {
        return None;
    }
    // `TOP 0` wants no rows, so this path would never evaluate the WHERE — and
    // the engine reports an unresolvable column (207) or a non-boolean predicate
    // (4145) from that evaluation, having no separate binding pass. Reading a
    // table to discard all of it is not worth answering an invalid query with an
    // empty result set, so the degenerate case stays on the collecting path.
    if select.top == Some(0) {
        return None;
    }
    // An uncorrelated subquery is executed by the rewrite this path skips; a
    // correlated one runs a query per row. (A subquery in the SELECT list is
    // already excluded: it is not a bare column.) A user scalar function in the
    // WHERE needs the same rewrite, so decline it here too.
    if select
        .where_clause
        .as_ref()
        .is_some_and(|w| expr_needs_binding(storage, eval_ctx.database_id, w))
    {
        return None;
    }
    // Whether every output column *could* be a source column, which is a
    // property of the syntax alone. Deciding it before the catalog is read keeps
    // `SELECT id + 1 FROM t` from paying for a table definition, a schema and a
    // resolver it is only going to discard.
    if !select.items.iter().all(|item| {
        matches!(
            item,
            SelectItem::Wildcard
                | SelectItem::QualifiedWildcard(_)
                | SelectItem::Expr {
                    expr: Expr {
                        kind: ExprKind::Column(_),
                        ..
                    },
                    ..
                }
        )
    }) {
        return None;
    }
    let Some(TableRef::Table { name, alias }) = select.from.as_ref() else {
        return None;
    };
    // The `sys.*` virtual tables build their rows in Rust and have no cursor to
    // scan. They are matched by their full name *before* catalog resolution, so
    // this check has to come first for the same reason: `resolve_table` strips a
    // schema prefix, so it would answer `sys.tables` with a user table called
    // `tables`.
    if is_sys_view(&name.value) {
        return None;
    }
    let def = resolve_table(storage, eval_ctx.database_id, &name.value)?;
    // A view is its own SELECT, expanded as a derived table — and its TableDef
    // has no columns and a `root_page` of 0, so a wildcard over it would project
    // nothing and the scan would read the catalog root instead of the view. A
    // PROCEDURE/FUNCTION/TRIGGER has the same empty shape and must not stream as
    // an empty table: the collecting path rejects it (2809/208).
    if def.view_query.is_some() || def.is_procedure() || def.is_function() || def.is_trigger() {
        return None;
    }
    // If SELECT is denied, fall back to the collecting path, which resolves the
    // same table through `build_table_source` and raises the 229 there — keeping
    // the check on the one path the executor uses to touch the object.
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Select).ok()?;
    let schema = def.schema().ok()?;

    let qualifier = alias
        .as_ref()
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    let source: Vec<ResultColumn> = schema
        .columns
        .iter()
        .map(|c| ResultColumn {
            name: c.name.clone(),
            column_type: c.column_type,
        })
        .collect();
    let collations: Vec<Option<String>> =
        schema.columns.iter().map(|c| c.collation.clone()).collect();
    // The full-width scope, used to plan the projection and resolve the WHERE's
    // references. What the scan actually runs on is the pruned scope built
    // below, once the needed columns are known.
    let resolver = JoinScope {
        columns: source
            .iter()
            .map(|c| (Some(qualifier.clone()), c.name.clone()))
            .collect(),
        collations: collations.clone(),
    };

    // The projection plan, mirroring `project`'s: every item must resolve to a
    // source column, so the output's types are the schema's.
    let mut columns = Vec::new();
    let mut picks = Vec::new();
    for item in &select.items {
        match item {
            SelectItem::Wildcard => {
                for (index, column) in source.iter().enumerate() {
                    picks.push(index);
                    columns.push(column.clone());
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                let indices = resolver.indices_for_qualifier(&q.value);
                // Unbound (4104): leave the error to the collecting path.
                if indices.is_empty() {
                    return None;
                }
                for index in indices {
                    picks.push(index);
                    columns.push(source[index].clone());
                }
            }
            SelectItem::Expr { expr, alias } => {
                let index = bare_column_index(expr, &resolver)?;
                let name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| bare_column_name(expr))
                    .unwrap_or_default();
                picks.push(index);
                columns.push(ResultColumn {
                    name,
                    column_type: source[index].column_type,
                });
            }
            // Rejected before projection on both paths.
            SelectItem::Assign { .. } => return None,
        }
    }

    // Projection pruning. The query reads the columns it projects plus the ones
    // its WHERE names, and nothing else — so those are the only ones the storage
    // layer decodes, and a scanned row is exactly that wide. A character column
    // costs a `String` allocation to decode, so on a wide table this is most of
    // the per-row work.
    //
    // A WHERE reference that does not resolve is simply not collected: it is not
    // a column of this table, so there is nothing to decode for it, and `eval`
    // still reports it (207) against the same resolver as before.
    let mut needed = picks.clone();
    let mut where_columns = Vec::new();
    if let Some(predicate) = &select.where_clause {
        collect_column_refs(predicate, &mut where_columns);
    }
    needed.extend(
        where_columns
            .iter()
            .filter_map(|name| resolver.resolve(name)),
    );
    needed.sort_unstable();
    needed.dedup();

    // The same access path `build_table_source` would take (it passes no
    // `needed`, so its choice can differ only toward a covering index — and it
    // never reaches this shape). Choosing here, rather than declining a seek,
    // is what keeps this gate free for the queries it rejects: a decline would
    // have thrown away the definition, the schema and this choice, and
    // `build_table_source` would compute all three again. Chosen after
    // `needed` is known so a covering index can win its tie (see
    // [`plan::choose`]).
    // The row count is a statistic (one buffer-pool-cached page read),
    // fetched only when choose() can use it (it returns a scan outright
    // without a predicate or indexes).
    let row_count = if def.indexes.is_empty() || select.where_clause.is_none() {
        None
    } else {
        storage.rel_row_count(def.database_id, &def.name)
    };
    let access = plan::choose(
        &def,
        &schema,
        &select.where_clause,
        eval_ctx,
        Some(&needed),
        row_count,
    );

    // Everything downstream now speaks in the scanned row's coordinates.
    let position = |index: usize| {
        needed
            .binary_search(&index)
            .expect("a needed column is in `needed`")
    };
    let picks = picks.into_iter().map(position).collect();
    let types = needed.iter().map(|&i| source[i].column_type).collect();
    let resolver = JoinScope {
        columns: needed
            .iter()
            .map(|&i| (Some(qualifier.clone()), source[i].name.clone()))
            .collect(),
        collations: needed.iter().map(|&i| collations[i].clone()).collect(),
    };

    // A seek covers when every column the query reads is INCLUDEd in the
    // index — original values in the leaf, since the key bytes are one-way
    // collation sort keys and cannot serve.
    let covering = match &access {
        plan::AccessPath::IndexSeek {
            index_object_id, ..
        } => def
            .indexes
            .iter()
            .find(|i| i.object_id == *index_object_id)
            .is_some_and(|i| needed.iter().all(|c| i.include.contains(c))),
        plan::AccessPath::TableScan => false,
    };

    Some(ScanPlan {
        db_id: def.database_id,
        table: def.name,
        access,
        needed,
        types,
        resolver,
        columns,
        picks,
        covering,
    })
}

/// Every column name a predicate references, in no particular order (duplicates
/// included — the caller dedups after resolving).
///
/// Exhaustive by construction: no wildcard arm, so a new [`ExprKind`] is a
/// compile error here rather than a column silently left undecoded.
pub(in crate::engine::relational) fn collect_column_refs(expr: &Expr, out: &mut Vec<String>) {
    match &expr.kind {
        ExprKind::Column(name) => out.push(name.value.clone()),
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => {}
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => collect_column_refs(e, out),
        ExprKind::Binary { left, right, .. } => {
            collect_column_refs(left, out);
            collect_column_refs(right, out);
        }
        ExprKind::Like {
            expr: e, pattern, ..
        } => {
            collect_column_refs(e, out);
            collect_column_refs(pattern, out);
        }
        ExprKind::InList { expr: e, list, .. } => {
            collect_column_refs(e, out);
            for item in list {
                collect_column_refs(item, out);
            }
        }
        ExprKind::Between {
            expr: e, low, high, ..
        } => {
            collect_column_refs(e, out);
            collect_column_refs(low, out);
            collect_column_refs(high, out);
        }
        ExprKind::Function { args, .. } => {
            for arg in args {
                collect_column_refs(arg, out);
            }
        }
        ExprKind::Aggregate { arg, .. } => {
            if let Some(arg) = arg {
                collect_column_refs(arg, out);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_column_refs(operand, out);
            }
            for (when, then) in branches {
                collect_column_refs(when, out);
                collect_column_refs(then, out);
            }
            if let Some(else_result) = else_result {
                collect_column_refs(else_result, out);
            }
        }
        // The gate rejects a subquery before this runs.
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => {}
    }
}

/// The `sys.*` catalog views, which [`build_table_source`] answers by name
/// ahead of any catalog lookup.
pub(in crate::engine::relational) fn is_sys_view(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "sys.tables"
            | "sys.views"
            | "sys.sql_modules"
            | "sys.columns"
            | "sys.indexes"
            | "sys.check_constraints"
            | "sys.foreign_keys"
            | "sys.default_constraints"
            | "sys.databases"
            | "sys.dm_repl_replica_states"
            | "sys.dm_repl_slots"
            | "sys.configurations"
            | "sys.database_principals"
            | "sys.database_role_members"
            | "sys.database_permissions"
    )
}

/// Scans, filters and projects one base table a row at a time.
///
/// The collecting path builds the whole table into `Source.rows`, filters that
/// into a second vector, converts *every* row to `SqlValue` in `project`, and
/// projects into a third — four copies of the input alive at once, before TOP
/// has discarded any of them. Here a slice is the only input in hand, each row
/// is projected or dropped as it is read, and `TOP n` stops the scan rather
/// than truncating afterwards.
///
/// The result is still collected; what this drops is the *input's*
/// materialization, which is the part that has no upper bound. An index seek
/// keeps its input materialized — `rel_index_scan` has no cursor — so it gains
/// the per-row savings but not that one; a seek's candidate set is bounded by
/// the seek.
///
/// `TOP n` therefore stops the scan without evaluating the predicate on rows
/// past the nth kept one, where the collecting path evaluated every source row
/// before truncating. A predicate that errors on one of those rows (a divide by
/// zero, an overflow) now goes unraised — which is SQL Server's behaviour, whose
/// Top operator likewise stops asking its child for rows.
pub(in crate::engine::relational) fn scan_select(
    storage: &Storage,
    plan: &ScanPlan,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    let mut out = RowSet {
        columns: plan.columns.clone(),
        rows: Vec::new(),
    };
    scan_select_rows(storage, plan, select, eval_ctx, &mut |row| {
        out.rows.push(row);
    })?;
    Ok(out)
}

/// Rows per [`BatchEmitter::rows`] chunk on the streamed scan path: enough to
/// amortize the per-event cost, small enough that the statement's peak memory
/// is a chunk, not the result.
pub(in crate::engine::relational) const STREAM_CHUNK_ROWS: usize = 256;

/// The streamed shape of [`scan_select`]: opens the result set, then emits
/// kept rows in [`STREAM_CHUNK_ROWS`] chunks as the scan produces them, so the
/// client sees rows while the scan is still running. On a mid-scan error the
/// full chunks already emitted stand — the caller closes the set (see
/// [`BatchRun::abort_open_rowset`]) — and the partial chunk is dropped.
pub(in crate::engine::relational) fn scan_select_streamed(
    storage: &Storage,
    plan: &ScanPlan,
    select: &Select,
    eval_ctx: &EvalContext,
    run: &mut BatchRun<'_>,
) -> Result<u64, SqlError> {
    run.open_rowset(plan.columns.clone());
    let mut chunk: Vec<Vec<Datum>> = Vec::new();
    let kept = scan_select_rows(storage, plan, select, eval_ctx, &mut |row| {
        chunk.push(row);
        if chunk.len() >= STREAM_CHUNK_ROWS {
            run.rows(std::mem::take(&mut chunk));
        }
    })?;
    run.rows(chunk);
    Ok(kept)
}

/// Walks the plan's access path, filters, projects, and hands each kept row to
/// `sink`, stopping once `TOP` is satisfied. Returns the number of rows kept
/// (which `TOP` counts, matching the collecting path's truncation of the
/// filtered rows — `TOP 0` never reaches here, the gate declines it). Both
/// executions of the scan shape ride this walk: [`scan_select`] collects into
/// a `RowSet`, [`scan_select_streamed`] emits chunks as slices are read.
pub(in crate::engine::relational) fn scan_select_rows(
    storage: &Storage,
    plan: &ScanPlan,
    select: &Select,
    eval_ctx: &EvalContext,
    sink: &mut dyn FnMut(Vec<Datum>),
) -> Result<u64, SqlError> {
    #[cfg(test)]
    storage.count_scan_select();
    let types = &plan.types;
    let mut kept: u64 = 0;
    let enough = |kept: u64| select.top.is_some_and(|top| kept >= top);

    // One row: filter it, and project it or drop it. `Ok(false)` once TOP is
    // satisfied and the caller should stop reading.
    let mut take = |row: Vec<Datum>| -> Result<bool, SqlError> {
        check_cancelled()?;
        if let Some(predicate) = &select.where_clause {
            let sql_row = row_values(&row, types);
            if !where_keeps(predicate, &sql_row, &plan.resolver, eval_ctx)? {
                return Ok(true);
            }
        }
        sink(plan.picks.iter().map(|i| row[*i].clone()).collect());
        kept += 1;
        Ok(!enough(kept))
    };

    match &plan.access {
        plan::AccessPath::TableScan => {
            if let Some(snapshot) = current_snapshot() {
                // A versioned reader holds no table lock, so the sliced
                // cursor's between-slice contract does not hold for it; the
                // snapshot scan reads the table atomically and merges the
                // version store.
                let rows = storage
                    .rel_scan_snapshot(plan.db_id, &plan.table, Some(&plan.needed), snapshot)
                    .map_err(|err| map_storage_err(err, &plan.table))?;
                for row in rows {
                    if !take(row)? {
                        break;
                    }
                }
            } else {
                let mut cursor = ScanCursor::start();
                let mut slice: Vec<Vec<Datum>> = Vec::new();
                'scan: while !cursor.done() {
                    cursor = storage
                        .rel_scan_slice(
                            plan.db_id,
                            &plan.table,
                            cursor,
                            SCAN_SLICE_ROWS,
                            Some(&plan.needed),
                            &mut slice,
                        )
                        .map_err(|err| map_storage_err(err, &plan.table))?;
                    for row in slice.drain(..) {
                        if !take(row)? {
                            break 'scan;
                        }
                    }
                }
            }
        }
        plan::AccessPath::IndexSeek {
            index_object_id,
            lower,
            upper,
            ..
        } => {
            // The seek narrows the candidates; the predicate still re-checks
            // each one, so the result matches a full scan.
            let rows = storage
                .rel_index_scan(
                    plan.db_id,
                    &plan.table,
                    *index_object_id,
                    lower.clone(),
                    upper.clone(),
                    Some(&plan.needed),
                    plan.covering,
                    current_snapshot(),
                )
                .map_err(|err| map_storage_err(err, &plan.table))?;
            for row in rows {
                if !take(row)? {
                    break;
                }
            }
        }
    }
    Ok(kept)
}
