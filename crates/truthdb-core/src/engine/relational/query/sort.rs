use super::super::prelude::*;

/// Per-query sort memory budget: a sort whose rows exceed this spills to temp
/// extents (external merge sort) rather than growing without bound.
pub(in crate::engine::relational) const SORT_MEMORY_BUDGET: usize = 64 * 1024 * 1024;

/// A row paired with its evaluated ORDER BY key, as carried through the sort.
pub(in crate::engine::relational) type KeyedRow = (Vec<SqlValue>, Vec<Datum>);

#[cfg(test)]
thread_local! {
    /// Test-only override that forces the external-sort spill path on small
    /// inputs (execution runs on the calling thread in tests).
    static TEST_SORT_BUDGET: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// The active sort memory budget (overridable in tests).
pub(in crate::engine::relational) fn sort_budget() -> usize {
    #[cfg(test)]
    if let Some(budget) = TEST_SORT_BUDGET.with(std::cell::Cell::get) {
        return budget;
    }
    SORT_MEMORY_BUDGET
}

/// Forces (or clears) the sort spill budget for the current test thread.
#[cfg(test)]
pub(crate) fn set_test_sort_budget(budget: Option<usize>) {
    TEST_SORT_BUDGET.with(|cell| cell.set(budget));
}

/// The ORDER BY comparator for one pair of pre-evaluated key tuples: per item,
/// collation-aware for a character column, else value order (NULLs first);
/// `descending` reverses. No tie-break here — the caller adds stability.
pub(in crate::engine::relational) fn compare_sort_keys(
    a: &[SqlValue],
    b: &[SqlValue],
    order_by: &[OrderItem],
    collators: &[Option<collation::Collation>],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (col, item) in order_by.iter().enumerate() {
        let ord = match (&collators[col], &a[col], &b[col]) {
            (Some(coll), SqlValue::Str(x), SqlValue::Str(y)) => coll.compare(x, y),
            (Some(_), SqlValue::Null, SqlValue::Null) => Ordering::Equal,
            (Some(_), SqlValue::Null, _) => Ordering::Less,
            (Some(_), _, SqlValue::Null) => Ordering::Greater,
            _ => order_key_cmp(&a[col], &b[col]),
        };
        let ord = if item.descending { ord.reverse() } else { ord };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Builds the per-item collators (only a bare character column is collation-
/// ordered; everything else uses value order).
pub(in crate::engine::relational) fn sort_collators(
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
) -> Vec<Option<collation::Collation>> {
    order_by
        .iter()
        .map(|item| {
            let index = bare_column_index(&item.expr, resolver)?;
            let is_char = matches!(
                types.get(index),
                Some(ColumnType::VarChar { .. }) | Some(ColumnType::NVarChar { .. })
            );
            if !is_char {
                return None;
            }
            let name = collations
                .get(index)
                .cloned()
                .flatten()
                .unwrap_or_else(|| collation::DEFAULT_COLLATION.to_string());
            Some(collation::Collation::from_name(&name))
        })
        .collect()
}

/// The ORDER BY key of one row.
/// Evaluate an expression that may hold a subquery or user scalar function: fold
/// it against `row` (its columns resolved by `resolver`) before the pure
/// evaluator, mirroring the SELECT-list / WHERE rewrite. Lets a UDF appear in
/// ORDER BY / join-ON / CHECK etc. rather than reaching the pure evaluator and
/// raising 195.
pub(in crate::engine::relational) fn eval_maybe_bound(
    storage: &Storage,
    expr: &Expr,
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
    eval_ctx: &EvalContext,
) -> Result<SqlValue, SqlError> {
    if expr_needs_binding(storage, eval_ctx.database_id, expr) {
        let outer = |name: &str| resolver.resolve(name);
        let bound = substitute_correlated_in_expr(storage, expr, &outer, row, eval_ctx)?;
        eval::eval(&bound, row, resolver, eval_ctx)
    } else {
        eval::eval(expr, row, resolver, eval_ctx)
    }
}

pub(in crate::engine::relational) fn sort_key(
    storage: &Storage,
    row: &[Datum],
    order_by: &[OrderItem],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Vec<SqlValue>, SqlError> {
    let values = row_values(row, types);
    order_by
        .iter()
        .map(|item| eval_maybe_bound(storage, &item.expr, &values, resolver, eval_ctx))
        .collect()
}

/// A rough in-memory byte estimate for a row, for the sort budget.
pub(in crate::engine::relational) fn approx_row_bytes(row: &[Datum]) -> usize {
    let payload: usize = row
        .iter()
        .map(|d| match d {
            Datum::VarChar(s) | Datum::NVarChar(s) => s.len() + 16,
            Datum::VarBinary(b) => b.len() + 16,
            _ => 16,
        })
        .sum();
    payload + 24
}

/// Sorts the (already WHERE-filtered) source rows by ORDER BY. Fits-in-budget
/// inputs sort in memory (Rust's stable `sort_by`); a larger input spills
/// sorted runs to temp extents and k-way merges them (external merge sort),
/// bounding the sort's working memory instead of erroring or doubling memory.
pub(in crate::engine::relational) fn order_rows(
    storage: &Storage,
    rows: Vec<Vec<Datum>>,
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Vec<Vec<Datum>>, SqlError> {
    order_rows_budgeted(
        storage,
        rows,
        order_by,
        types,
        collations,
        resolver,
        eval_ctx,
        sort_budget(),
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::engine::relational) fn order_rows_budgeted<'a>(
    storage: &'a Storage,
    rows: Vec<Vec<Datum>>,
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
    budget: usize,
) -> Result<Vec<Vec<Datum>>, SqlError> {
    let collators = sort_collators(order_by, types, collations, resolver);
    let cmp = |a: &Vec<SqlValue>, b: &Vec<SqlValue>| compare_sort_keys(a, b, order_by, &collators);

    // Generate sorted runs, spilling a run to a `RowSpool` each time the
    // accumulated rows reach the budget. The final (in-memory) run is kept.
    let mut runs: Vec<crate::relstore::spill::RowSpool<'a>> = Vec::new();
    let mut current: Vec<KeyedRow> = Vec::new();
    let mut current_bytes = 0usize;
    for row in rows {
        check_cancelled()?;
        let key = sort_key(storage, &row, order_by, types, resolver, eval_ctx)?;
        current_bytes += approx_row_bytes(&row);
        current.push((key, row));
        if current_bytes >= budget {
            runs.push(sort_and_spill(storage, &mut current, &cmp)?);
            current_bytes = 0;
        }
    }
    // No spill: a plain stable in-memory sort.
    if runs.is_empty() {
        current.sort_by(|(a, _), (b, _)| cmp(a, b));
        return Ok(current.into_iter().map(|(_, row)| row).collect());
    }
    // Sort the final partial run and merge every run.
    current.sort_by(|(a, _), (b, _)| cmp(a, b));
    merge_runs(
        storage, &runs, current, order_by, types, resolver, eval_ctx, &collators,
    )
}

/// Stably sorts `run` in place and writes its rows (in sorted order) to a fresh
/// `RowSpool`, clearing `run`.
pub(in crate::engine::relational) fn sort_and_spill<'a>(
    storage: &'a Storage,
    run: &mut Vec<KeyedRow>,
    cmp: &impl Fn(&Vec<SqlValue>, &Vec<SqlValue>) -> std::cmp::Ordering,
) -> Result<crate::relstore::spill::RowSpool<'a>, SqlError> {
    run.sort_by(|(a, _), (b, _)| cmp(a, b));
    let mut spool = crate::relstore::spill::RowSpool::new(storage);
    for (_, row) in run.drain(..) {
        spool
            .write_row(&row)
            .map_err(|e| map_storage_err(e, "<sort spill>"))?;
    }
    spool
        .finish_writing()
        .map_err(|e| map_storage_err(e, "<sort spill>"))?;
    Ok(spool)
}

/// K-way merges the sorted spilled `runs` and the sorted in-memory `tail` run
/// into one sorted row vector. Keys are recomputed per row on read (cheap for
/// column refs); ties prefer the earlier run so the merge is globally stable
/// (spilled runs hold earlier input rows than the in-memory tail).
#[allow(clippy::too_many_arguments)]
pub(in crate::engine::relational) fn merge_runs(
    storage: &Storage,
    runs: &[crate::relstore::spill::RowSpool<'_>],
    tail: Vec<KeyedRow>,
    order_by: &[OrderItem],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
    collators: &[Option<collation::Collation>],
) -> Result<Vec<Vec<Datum>>, SqlError> {
    // One cursor per source: spilled-run readers first, then the in-memory tail.
    let mut readers: Vec<_> = runs.iter().map(|r| r.reader()).collect();
    let mut tail_iter = tail.into_iter();

    // Current head (key + row) of each source, in the same order.
    let source_count = readers.len() + 1;
    let mut heads: Vec<Option<(Vec<SqlValue>, Vec<Datum>)>> = Vec::with_capacity(source_count);
    for reader in &mut readers {
        heads.push(read_head(
            storage, reader, order_by, types, resolver, eval_ctx,
        )?);
    }
    heads.push(tail_iter.next());

    let total: usize = runs.iter().map(|r| r.row_count() as usize).sum::<usize>() + heads.len();
    let mut out: Vec<Vec<Datum>> = Vec::with_capacity(total);
    loop {
        // Pick the smallest head; on a key tie, the earliest source (lowest
        // index) wins, which preserves input order across runs.
        let mut best: Option<usize> = None;
        for (i, head) in heads.iter().enumerate() {
            let Some((key, _)) = head else { continue };
            match best {
                None => best = Some(i),
                Some(b) => {
                    let (bkey, _) = heads[b].as_ref().unwrap();
                    if compare_sort_keys(key, bkey, order_by, collators) == std::cmp::Ordering::Less
                    {
                        best = Some(i);
                    }
                }
            }
        }
        let Some(i) = best else { break };
        let (_, row) = heads[i].take().unwrap();
        out.push(row);
        // Advance the chosen source.
        heads[i] = if i < readers.len() {
            read_head(
                storage,
                &mut readers[i],
                order_by,
                types,
                resolver,
                eval_ctx,
            )?
        } else {
            tail_iter.next()
        };
    }
    Ok(out)
}

/// Reads the next row from a spool reader and pairs it with its ORDER BY key.
pub(in crate::engine::relational) fn read_head(
    storage: &Storage,
    reader: &mut crate::relstore::spill::RowSpoolReader,
    order_by: &[OrderItem],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Option<KeyedRow>, SqlError> {
    match reader
        .next_row()
        .map_err(|e| map_storage_err(e, "<sort spill>"))?
    {
        Some(row) => {
            let key = sort_key(storage, &row, order_by, types, resolver, eval_ctx)?;
            Ok(Some((key, row)))
        }
        None => Ok(None),
    }
}
