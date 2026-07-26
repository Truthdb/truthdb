use super::super::prelude::*;

/// Joins two sources. The PROBE side — the side driving output, walked exactly
/// once: left, or right for a RIGHT join — streams slice-by-slice; only the
/// BUILD side is materialized here, and the hash join grace-spills it past the
/// memory budget. The ON predicate (absent for CROSS) is evaluated against the
/// concatenated row; outer joins emit NULL-extended rows for unmatched sides.
pub(in crate::engine::relational) fn join_sources(
    storage: &Storage,
    left: Source,
    right: Source,
    kind: JoinKind,
    on: Option<&Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    let mut qualifiers = left.qualifiers.clone();
    qualifiers.extend(right.qualifiers.clone());
    let mut collations = left.collations.clone();
    collations.extend(right.collations.clone());
    let types: Vec<ColumnType> = columns.iter().map(|c| c.column_type).collect();
    let scope = JoinScope {
        columns: qualifiers
            .iter()
            .zip(&columns)
            .map(|(q, c)| (q.clone(), c.name.clone()))
            .collect(),
        collations: collations.clone(),
    };
    let left_nulls = vec![Datum::Null; left.columns.len()];
    let right_nulls = vec![Datum::Null; right.columns.len()];

    // A subquery or user scalar function in the ON predicate is folded against
    // the joined row before the pure evaluator runs (per candidate pair).
    let on_needs_binding =
        on.is_some_and(|pred| expr_needs_binding(storage, eval_ctx.database_id, pred));
    let concat = |l: &[Datum], r: &[Datum]| -> Vec<Datum> { l.iter().chain(r).cloned().collect() };
    let matches = |l: &[Datum], r: &[Datum]| -> Result<bool, SqlError> {
        match on {
            None => Ok(true),
            Some(pred) => {
                let vals = row_values(&concat(l, r), &types);
                let value = if on_needs_binding {
                    let outer = |name: &str| scope.resolve(name);
                    let bound =
                        substitute_correlated_in_expr(storage, pred, &outer, &vals, eval_ctx)?;
                    eval::eval(&bound, &vals, &scope, eval_ctx)?
                } else {
                    eval::eval(pred, &vals, &scope, eval_ctx)?
                };
                match value {
                    SqlValue::Bool(b) => Ok(b),
                    SqlValue::Null => Ok(false),
                    _ => Err(SqlError::new(
                        4145,
                        15,
                        1,
                        "An expression of non-boolean type specified in a context where a condition is expected, near 'ON'.",
                    )
                    .at(pred.span)),
                }
            }
        }
    };

    // Equijoin key columns (bare `left_col = right_col` conjuncts of a
    // hash-compatible type). When present on an INNER/LEFT/RIGHT/FULL join, a
    // hash join replaces the O(n·m) nested loop; the full ON predicate is still
    // re-checked on each hash candidate, so the result set and its order are
    // identical to the nested loop. (Like a real optimizer, the hash join
    // evaluates the ON predicate only on candidate pairs sharing a key, so a
    // side-effecting error in a residual conjunct — e.g. `1/b.z` — may be raised
    // on fewer rows than the loop would; the SQL result set is unaffected.)
    // CROSS and equi-key-less joins keep the loop.
    let equi = match on {
        Some(pred) => extract_equi_keys(pred, &left, &right),
        None => Vec::new(),
    };

    // The build side is the one NOT driving output: left for RIGHT, else
    // right. It is walked repeatedly (nested loop) or hashed whole, so it
    // materializes here (bounded by the grace-hash spill past the budget);
    // the probe side stays a stream.
    let build_left = matches!(kind, JoinKind::Right);
    let (probe, build) = if build_left {
        (right, left)
    } else {
        (left, right)
    };
    let build = MaterializedSource::from(build, storage)?;
    // LEFT/RIGHT/FULL null-extend unmatched probe rows; FULL also null-extends
    // unmatched build rows. Emission is oriented so output is always
    // [left columns .. right columns].
    let preserve_probe = matches!(kind, JoinKind::Left | JoinKind::Right | JoinKind::Full);
    let preserve_build = matches!(kind, JoinKind::Full);
    let emit_match = |p: &[Datum], b: &[Datum]| -> Vec<Datum> {
        if build_left {
            concat(b, p)
        } else {
            concat(p, b)
        }
    };
    let emit_probe_only = |p: &[Datum]| -> Vec<Datum> {
        if build_left {
            concat(&left_nulls, p)
        } else {
            concat(p, &right_nulls)
        }
    };
    let emit_build_only = |b: &[Datum]| -> Vec<Datum> {
        if build_left {
            concat(b, &right_nulls)
        } else {
            concat(&left_nulls, b)
        }
    };

    let mut rows = Vec::new();
    if !equi.is_empty() && !matches!(kind, JoinKind::Cross) {
        hash_join(
            storage,
            probe,
            &build,
            build_left,
            &equi,
            preserve_probe,
            preserve_build,
            &matches,
            &emit_match,
            &emit_probe_only,
            &emit_build_only,
            &mut rows,
        )?;
    } else {
        // Nested loop: stream the probe side, walking the whole build side
        // per probe row.
        let mut build_matched = vec![false; build.rows.len()];
        let mut probe_rows = probe.rows;
        while let Some(slice) = probe_rows.next_slice(storage)? {
            for p in &slice {
                check_cancelled()?;
                let mut matched = false;
                for (bi, b) in build.rows.iter().enumerate() {
                    if matches_oriented(p, b, build_left, &matches)? {
                        rows.push(emit_match(p, b));
                        matched = true;
                        build_matched[bi] = true;
                    }
                }
                if preserve_probe && !matched {
                    rows.push(emit_probe_only(p));
                }
            }
        }
        if preserve_build {
            for (bi, b) in build.rows.iter().enumerate() {
                if !build_matched[bi] {
                    rows.push(emit_build_only(b));
                }
            }
        }
    }
    Ok(Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    })
}

/// An equijoin key pair: `(left column index, right column index)` for a
/// `left_col = right_col` conjunct of the ON predicate.
pub(in crate::engine::relational) type EquiKey = (usize, usize);

/// Extracts the equijoin key pairs usable for a hash join from an ON predicate:
/// the top-level `AND` conjuncts that are `col = col` with one bare column
/// resolving uniquely to the left source, the other uniquely to the right, and
/// matching hash classes. A predicate with no such conjunct (a range/disjunction
/// join, an expression key, or a type-mismatched equality) yields an empty list
/// and the caller keeps the nested-loop join. Non-equi conjuncts are left for
/// the full-ON re-check on each hash candidate, so results are unchanged.
pub(in crate::engine::relational) fn extract_equi_keys(
    pred: &Expr,
    left: &Source,
    right: &Source,
) -> Vec<EquiKey> {
    let left_scope = left.scope();
    let right_scope = right.scope();
    // `Some(true)` = resolves uniquely to the left source, `Some(false)` = right,
    // `None` = neither, both, or not a bare column.
    let side_of = |expr: &Expr| -> Option<(bool, usize)> {
        let ExprKind::Column(name) = &expr.kind else {
            return None;
        };
        match (
            left_scope.resolve(&name.value),
            right_scope.resolve(&name.value),
        ) {
            (Some(i), None) => Some((true, i)),
            (None, Some(j)) => Some((false, j)),
            _ => None,
        }
    };
    let mut conjuncts = Vec::new();
    flatten_and(pred, &mut conjuncts);
    let mut keys = Vec::new();
    for conjunct in conjuncts {
        let ExprKind::Binary {
            op: ast::BinaryOp::Eq,
            left: le,
            right: re,
        } = &conjunct.kind
        else {
            continue;
        };
        let pair = match (side_of(le), side_of(re)) {
            (Some((true, i)), Some((false, j))) => (i, j),
            (Some((false, j)), Some((true, i))) => (i, j),
            _ => continue,
        };
        if hash::hash_class(left.columns[pair.0].column_type)
            == hash::hash_class(right.columns[pair.1].column_type)
        {
            keys.push(pair);
        }
    }
    keys
}

/// Collects the top-level `AND` conjuncts of an expression (flattening nested
/// `AND`s); any other expression is one conjunct.
pub(in crate::engine::relational) fn flatten_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let ExprKind::Binary {
        op: ast::BinaryOp::And,
        left,
        right,
    } = &expr.kind
    {
        flatten_and(left, out);
        flatten_and(right, out);
    } else {
        out.push(expr);
    }
}

/// Grace-hash join for any kind: partition both inputs by join-key hash into
/// temp extents (matching rows share a partition, since equal keys hash equally,
/// so per-partition matched/unmatched equals globally matched/unmatched), then
/// join each partition pair in memory — the build hash table is bounded to one
/// partition. The probe input streams straight into its partitions; each
/// partition then materializes only its build rows and streams its probe rows
/// back. NULL-keyed rows never match: the outer side's are null-extended
/// directly, the inner side's are dropped. Output order is by partition
/// (immaterial — a spilling join is not order-sensitive).
#[allow(clippy::too_many_arguments)]
pub(in crate::engine::relational) fn grace_hash_join(
    storage: &Storage,
    mut probe_rows: SourceRows,
    build: &MaterializedSource,
    build_left: bool,
    preserve_probe: bool,
    preserve_build: bool,
    probe_key: &impl Fn(&[Datum]) -> Vec<SqlValue>,
    build_key: &impl Fn(&[Datum]) -> Vec<SqlValue>,
    matches: &impl Fn(&[Datum], &[Datum]) -> Result<bool, SqlError>,
    emit_match: &impl Fn(&[Datum], &[Datum]) -> Vec<Datum>,
    emit_probe_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    emit_build_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    rows: &mut Vec<Vec<Datum>>,
) -> Result<(), SqlError> {
    use hash::{HashKey, key_has_null};
    use std::collections::HashMap;

    let build_bytes: usize = build.rows.iter().map(|r| approx_row_bytes(r)).sum();
    let partitions = (build_bytes / sort_budget() + 1).max(2);
    let partition_of = |key: &[SqlValue]| -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        HashKey(key.to_vec()).hash(&mut hasher);
        (hasher.finish() % partitions as u64) as usize
    };
    let spill_err = |e| map_storage_err(e, "<join spill>");

    let mut probe_parts: Vec<_> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    let mut build_parts: Vec<_> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    // Partition non-null-key rows; null-key rows can't match, so emit the outer
    // side's now and drop the rest. The probe side streams slice-by-slice.
    while let Some(slice) = probe_rows.next_slice(storage)? {
        for p in &slice {
            check_cancelled()?;
            let key = probe_key(p);
            if key_has_null(&key) {
                if preserve_probe {
                    rows.push(emit_probe_only(p));
                }
                continue;
            }
            probe_parts[partition_of(&key)]
                .write_row(p)
                .map_err(spill_err)?;
        }
    }
    for b in &build.rows {
        check_cancelled()?;
        let key = build_key(b);
        if key_has_null(&key) {
            if preserve_build {
                rows.push(emit_build_only(b));
            }
            continue;
        }
        build_parts[partition_of(&key)]
            .write_row(b)
            .map_err(spill_err)?;
    }
    for part in probe_parts.iter_mut().chain(build_parts.iter_mut()) {
        part.finish_writing().map_err(spill_err)?;
    }

    for part in 0..partitions {
        let mut b_rows: Vec<Vec<Datum>> =
            Vec::with_capacity(build_parts[part].row_count() as usize);
        let mut b_reader = build_parts[part].reader();
        while let Some(row) = b_reader.next_row().map_err(spill_err)? {
            b_rows.push(row);
        }
        let mut table: HashMap<HashKey, Vec<usize>> = HashMap::new();
        for (bi, b) in b_rows.iter().enumerate() {
            table.entry(HashKey(build_key(b))).or_default().push(bi);
        }
        let mut build_matched = vec![false; b_rows.len()];
        let mut p_reader = probe_parts[part].reader();
        while let Some(p) = p_reader.next_row().map_err(spill_err)? {
            let mut matched = false;
            if let Some(cands) = table.get(&HashKey(probe_key(&p))) {
                for &bi in cands {
                    let b = &b_rows[bi];
                    if matches_oriented(&p, b, build_left, matches)? {
                        rows.push(emit_match(&p, b));
                        matched = true;
                        build_matched[bi] = true;
                    }
                }
            }
            if preserve_probe && !matched {
                rows.push(emit_probe_only(&p));
            }
        }
        if preserve_build {
            for (bi, b) in b_rows.iter().enumerate() {
                if !build_matched[bi] {
                    rows.push(emit_build_only(b));
                }
            }
        }
    }
    Ok(())
}

/// Evaluates the ON predicate for a probe/build pair in the caller's left/right
/// orientation (`matches` always takes `(left, right)`).
pub(in crate::engine::relational) fn matches_oriented(
    probe: &[Datum],
    build: &[Datum],
    build_left: bool,
    matches: &impl Fn(&[Datum], &[Datum]) -> Result<bool, SqlError>,
) -> Result<bool, SqlError> {
    if build_left {
        matches(build, probe)
    } else {
        matches(probe, build)
    }
}

/// Hash join on the given equi-key columns. The build side is hashed by its
/// key tuple; the probe side streams and drives output, so row order matches
/// the nested loop exactly (unmatched build rows for FULL are null-extended at
/// the end, as the loop does). NULL key components never match (`x = NULL` is
/// UNKNOWN), so NULL-keyed rows are excluded from the table and treated as
/// unmatched. The full ON predicate is re-evaluated on every candidate, so
/// residual (non-equi) conjuncts and the 3VL of the equality are honored
/// identically to the nested loop. A build side past the memory budget spills
/// via [`grace_hash_join`].
#[allow(clippy::too_many_arguments)]
pub(in crate::engine::relational) fn hash_join(
    storage: &Storage,
    probe: Source,
    build: &MaterializedSource,
    build_left: bool,
    equi: &[EquiKey],
    preserve_probe: bool,
    preserve_build: bool,
    matches: &impl Fn(&[Datum], &[Datum]) -> Result<bool, SqlError>,
    emit_match: &impl Fn(&[Datum], &[Datum]) -> Vec<Datum>,
    emit_probe_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    emit_build_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    rows: &mut Vec<Vec<Datum>>,
) -> Result<(), SqlError> {
    use hash::{HashKey, key_has_null};
    use std::collections::HashMap;

    let Source {
        columns: probe_columns,
        collations: probe_collations,
        rows: mut probe_rows,
        ..
    } = probe;

    // The case sensitivity governing each equi-key pair — the combined collation
    // of its two columns, combined in left/right order (`combine` favors its
    // first operand when both sides are exact, and `matches` combines in that
    // same order). The hash key is only a *pre-filter*: the full ON predicate
    // (collation-aware `matches`) re-checks each candidate, so the buckets must
    // be a superset of true matches. Folding both sides' key strings by this
    // sensitivity ensures case-insensitive-equal keys share a bucket (an
    // unfolded, case-sensitive hash would put `'abc'` and `'ABC'` in different
    // buckets, and the CI `matches` would never be consulted → a lost match).
    let (left_collations, right_collations) = if build_left {
        (&build.collations, &probe_collations)
    } else {
        (&probe_collations, &build.collations)
    };
    let key_sens: Vec<CollationSensitivity> = equi
        .iter()
        .map(|&(i, j)| {
            CollationSensitivity::from_optional(left_collations.get(i).and_then(|c| c.as_deref()))
                .combine(CollationSensitivity::from_optional(
                    right_collations.get(j).and_then(|c| c.as_deref()),
                ))
        })
        .collect();
    // Each equi pair reoriented as (probe column, build column): the pairs are
    // (left, right), and the build side is left exactly for a RIGHT join.
    let key_cols: Vec<(usize, usize)> = equi
        .iter()
        .map(|&(i, j)| if build_left { (j, i) } else { (i, j) })
        .collect();
    let probe_key = |p: &[Datum]| -> Vec<SqlValue> {
        key_cols
            .iter()
            .zip(&key_sens)
            .map(|(&(pc, _), &sens)| {
                sens.fold_value(value::datum_to_sql(&p[pc], &probe_columns[pc].column_type))
            })
            .collect()
    };
    let build_key = |b: &[Datum]| -> Vec<SqlValue> {
        key_cols
            .iter()
            .zip(&key_sens)
            .map(|(&(_, bc), &sens)| {
                sens.fold_value(value::datum_to_sql(&b[bc], &build.columns[bc].column_type))
            })
            .collect()
    };

    // Grace-hash spill for a large build side (any kind): partition both sides
    // by join-key hash so each partition's build table fits the memory budget.
    let build_bytes: usize = build.rows.iter().map(|r| approx_row_bytes(r)).sum();
    if build_bytes > sort_budget() {
        return grace_hash_join(
            storage,
            probe_rows,
            build,
            build_left,
            preserve_probe,
            preserve_build,
            &probe_key,
            &build_key,
            matches,
            emit_match,
            emit_probe_only,
            emit_build_only,
            rows,
        );
    }

    let mut table: HashMap<HashKey, Vec<usize>> = HashMap::new();
    for (index, row) in build.rows.iter().enumerate() {
        check_cancelled()?;
        let key = build_key(row);
        if key_has_null(&key) {
            continue;
        }
        table.entry(HashKey(key)).or_default().push(index);
    }

    let mut build_matched = vec![false; build.rows.len()];
    while let Some(slice) = probe_rows.next_slice(storage)? {
        for p in &slice {
            check_cancelled()?;
            let key = probe_key(p);
            let mut matched = false;
            if !key_has_null(&key)
                && let Some(cands) = table.get(&HashKey(key))
            {
                for &bi in cands {
                    let b = &build.rows[bi];
                    if matches_oriented(p, b, build_left, matches)? {
                        rows.push(emit_match(p, b));
                        matched = true;
                        build_matched[bi] = true;
                    }
                }
            }
            if preserve_probe && !matched {
                rows.push(emit_probe_only(p));
            }
        }
    }
    if preserve_build {
        for (bi, b) in build.rows.iter().enumerate() {
            if !build_matched[bi] {
                rows.push(emit_build_only(b));
            }
        }
    }
    Ok(())
}
