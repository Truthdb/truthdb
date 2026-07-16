//! GROUP BY / HAVING / aggregate execution (Stage 8). Runs after WHERE and
//! before DISTINCT / ORDER BY / TOP.
//!
//! Aggregates and grouped columns in the SELECT list and HAVING are rewritten
//! to reference a synthetic "group row" of `[group keys..., aggregate
//! results...]`; the ordinary scalar evaluator then evaluates the rewritten
//! expressions against that row. A bare column that is neither a group key nor
//! inside an aggregate is rejected with error 8120.

use truthdb_sql::ast::{AggFunc, BinaryOp, Expr, ExprKind, Name, Select, SelectItem};
use truthdb_sql::collation::CollationSensitivity;
use truthdb_sql::error::SqlError;
use truthdb_sql::eval::{self, EvalContext};
use truthdb_sql::value::{SqlValue, order_key_cmp};

use crate::relstore::types::{ColumnType, Datum};

use super::value;
use super::{JoinScope, ResultColumn, RowSet, row_values};

/// True if the query aggregates: it has a GROUP BY, a HAVING, or any aggregate
/// in its SELECT list.
pub fn is_aggregated(select: &Select) -> bool {
    !select.group_by.is_empty()
        || select.having.is_some()
        || select.items.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => contains_aggregate(expr),
            SelectItem::Assign { value, .. } => contains_aggregate(value),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
        })
}

fn contains_aggregate(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Aggregate { .. } => true,
        ExprKind::Unary { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Cast { expr, .. } => contains_aggregate(expr),
        ExprKind::Binary { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        ExprKind::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        ExprKind::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        ExprKind::Function { args, .. } => args.iter().any(contains_aggregate),
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || branches
                    .iter()
                    .any(|(w, r)| contains_aggregate(w) || contains_aggregate(r))
                || else_result.as_deref().is_some_and(contains_aggregate)
        }
        _ => false,
    }
}

/// Resolver over the synthetic group row `[group keys..., aggregate results...]`.
/// Carries each grouping column's collation (aggregate-result slots default to
/// the case-insensitive database default) so HAVING / projection comparisons on
/// a `_CS`/`_BIN` grouping column stay case-sensitive.
struct SynthScope {
    names: Vec<String>,
    collations: Vec<CollationSensitivity>,
}

impl truthdb_sql::eval::ColumnResolver for SynthScope {
    fn resolve(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|n| n.eq_ignore_ascii_case(name))
    }

    fn collation(&self, index: usize) -> CollationSensitivity {
        self.collations
            .get(index)
            .copied()
            .unwrap_or(CollationSensitivity::default_collation())
    }
}

/// One aggregate to compute over each group.
struct AggSpec {
    func: AggFunc,
    distinct: bool,
    /// `None` only for `COUNT(*)`.
    arg: Option<Expr>,
}

/// Runs the grouped query over the (already WHERE-filtered) source rows and
/// returns the projected output rows (before DISTINCT/ORDER BY/TOP).
pub fn execute(
    storage: &crate::storage::Storage,
    select: &Select,
    rows: &[Vec<Datum>],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    // Rewrite SELECT items + HAVING against the group row, collecting aggregates.
    let mut aggs: Vec<AggSpec> = Vec::new();
    let mut out_names: Vec<String> = Vec::new();
    let mut out_exprs: Vec<Expr> = Vec::new();
    for item in &select.items {
        match item {
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                return Err(SqlError::new(
                    8120,
                    16,
                    1,
                    "'*' is not allowed in a query with GROUP BY or aggregates; list the grouped columns.",
                ));
            }
            SelectItem::Expr { expr, alias } => {
                out_names.push(output_name(expr, alias.as_ref()));
                out_exprs.push(rewrite(expr, &select.group_by, &mut aggs)?);
            }
            // Assignment SELECTs are rewritten to Expr items before execution.
            SelectItem::Assign { .. } => {
                unreachable!("assignment SELECT handled before aggregation")
            }
        }
    }
    let having = match &select.having {
        Some(h) => Some(rewrite(h, &select.group_by, &mut aggs)?),
        None => None,
    };

    // The synthetic resolver over [group keys..., aggregate results...], carrying
    // each grouping column's collation so HAVING / projection comparisons on a
    // `_CS`/`_BIN` grouping column stay case-sensitive (a bare name list would
    // report the case-insensitive default and re-merge groups `group_rows` kept
    // apart). Aggregate-result slots take the database default.
    let names: Vec<String> = (0..select.group_by.len())
        .map(|i| format!("$gk{i}"))
        .chain((0..aggs.len()).map(|j| format!("$agg{j}")))
        .collect();
    let collations: Vec<CollationSensitivity> = select
        .group_by
        .iter()
        .map(|expr| eval::key_collation(expr, resolver))
        .chain((0..aggs.len()).map(|_| CollationSensitivity::default_collation()))
        .collect();
    let synth = SynthScope { names, collations };

    // A GROUP BY over an input larger than the operator memory budget spills:
    // partition the rows by group-key hash (so every member of a group lands in
    // one partition) to temp extents, then aggregate each partition in bounded
    // memory. Without GROUP BY (one group) or within budget, aggregate directly.
    let budget = super::sort_budget();
    let input_bytes: usize = rows.iter().map(|r| super::approx_row_bytes(r)).sum();
    let out_rows = if select.group_by.is_empty() || input_bytes <= budget {
        aggregate_partition(
            storage, select, rows, types, resolver, eval_ctx, &aggs, &having, &out_exprs, &synth,
        )?
    } else {
        grace_hash_aggregate(
            storage,
            select,
            rows,
            types,
            resolver,
            eval_ctx,
            &aggs,
            &having,
            &out_exprs,
            &synth,
            input_bytes,
            budget,
        )?
    };

    build_rowset(out_names, out_rows)
}

/// Groups `rows`, computes each aggregate and HAVING, and projects the output
/// SQL-value rows. This is the in-memory aggregation used both directly and per
/// grace-hash partition.
#[allow(clippy::too_many_arguments)]
fn aggregate_partition(
    storage: &crate::storage::Storage,
    select: &Select,
    rows: &[Vec<Datum>],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
    aggs: &[AggSpec],
    having: &Option<Expr>,
    out_exprs: &[Expr],
    synth: &SynthScope,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    let groups = group_rows(select, rows, types, resolver, eval_ctx)?;
    let mut out_rows: Vec<Vec<SqlValue>> = Vec::new();
    for (keys, members) in &groups {
        let mut group_row = keys.clone();
        for spec in aggs {
            group_row.push(compute_aggregate(
                spec, rows, types, resolver, members, eval_ctx,
            )?);
        }
        if let Some(having) = having {
            // A subquery still present in HAVING is correlated to this
            // query's grouping columns (the rewrite pass left it): bind the
            // group row's values in, making it uncorrelated for this group.
            // A reference to a non-grouped column stays unresolved and errors
            // inside the subquery, as SQL Server rejects it too.
            let bound;
            let having = if crate::rel::expr_has_subquery(having) {
                // The synth scope's names are synthetic ($gk0, $agg0) — an
                // outer reference binds against the ORIGINAL group-by
                // expressions instead: a bare-column key at position i is
                // group_row[i]. Qualified and bare spellings match either way
                // (the qualifier can only name this query's own FROM, which
                // is where the grouping column came from).
                let resolve = |name: &str| -> Option<usize> {
                    let bare = name.rsplit_once('.').map(|(_, b)| b).unwrap_or(name);
                    select.group_by.iter().position(|key| match &key.kind {
                        truthdb_sql::ast::ExprKind::Column(n) => {
                            let key_bare =
                                n.value.rsplit_once('.').map(|(_, b)| b).unwrap_or(&n.value);
                            n.value.eq_ignore_ascii_case(name)
                                || key_bare.eq_ignore_ascii_case(bare)
                        }
                        _ => false,
                    })
                };
                bound = crate::rel::substitute_correlated_in_expr(
                    storage, having, &resolve, &group_row, eval_ctx,
                )?;
                &bound
            } else {
                having
            };
            match eval::eval(having, &group_row, synth, eval_ctx)? {
                SqlValue::Bool(true) => {}
                SqlValue::Bool(false) | SqlValue::Null => continue,
                _ => {
                    return Err(SqlError::new(
                        4145,
                        15,
                        1,
                        "An expression of non-boolean type specified in a context where a condition is expected, near 'HAVING'.",
                    ));
                }
            }
        }
        let mut row = Vec::with_capacity(out_exprs.len());
        for expr in out_exprs {
            row.push(eval::eval(expr, &group_row, synth, eval_ctx)?);
        }
        out_rows.push(row);
    }
    Ok(out_rows)
}

/// Grace-hash aggregation: partition rows by group-key hash into temp-extent
/// spools, then aggregate each partition independently and concatenate.
/// (Extreme group skew — one partition far larger than the budget — is not
/// re-partitioned; a first cut, documented.)
#[allow(clippy::too_many_arguments)]
fn grace_hash_aggregate(
    storage: &crate::storage::Storage,
    select: &Select,
    rows: &[Vec<Datum>],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
    aggs: &[AggSpec],
    having: &Option<Expr>,
    out_exprs: &[Expr],
    synth: &SynthScope,
    input_bytes: usize,
    budget: usize,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    let partitions = (input_bytes / budget + 1).max(2);
    let mut spools: Vec<crate::relstore::spill::RowSpool> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    for row in rows {
        let key = group_key(select, row, types, resolver, eval_ctx)?;
        let index = partition_index(&key, partitions);
        spools[index]
            .write_row(row)
            .map_err(|e| super::map_storage_err(e, "<agg spill>"))?;
    }
    let mut out_rows: Vec<Vec<SqlValue>> = Vec::new();
    for spool in &mut spools {
        spool
            .finish_writing()
            .map_err(|e| super::map_storage_err(e, "<agg spill>"))?;
    }
    for spool in &spools {
        let mut part_rows: Vec<Vec<Datum>> = Vec::with_capacity(spool.row_count() as usize);
        let mut reader = spool.reader();
        while let Some(row) = reader
            .next_row()
            .map_err(|e| super::map_storage_err(e, "<agg spill>"))?
        {
            part_rows.push(row);
        }
        out_rows.extend(aggregate_partition(
            storage, select, &part_rows, types, resolver, eval_ctx, aggs, having, out_exprs, synth,
        )?);
    }
    Ok(out_rows)
}

/// The GROUP BY key of one row (used to route it to a grace-hash partition).
/// The key is *folded* by each grouping column's collation — identically to the
/// in-memory `group_rows` — so case-insensitive-equal keys route to the same
/// partition and are not split across partitions on a spilling GROUP BY.
fn group_key(
    select: &Select,
    row: &[Datum],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Vec<SqlValue>, SqlError> {
    let sql_row = row_values(row, types);
    let key_sens: Vec<CollationSensitivity> = select
        .group_by
        .iter()
        .map(|expr| eval::key_collation(expr, resolver))
        .collect();
    let raw = select
        .group_by
        .iter()
        .map(|expr| eval::eval(expr, &sql_row, resolver, eval_ctx))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(super::hash::fold_hash_key(&raw, &key_sens))
}

/// The partition a group key routes to. Uses the same `HashKey` hashing as the
/// in-memory grouping, so equal group keys always share a partition.
fn partition_index(key: &[SqlValue], partitions: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    super::hash::HashKey(key.to_vec()).hash(&mut hasher);
    (hasher.finish() % partitions as u64) as usize
}

/// Groups the rows by the GROUP BY key expressions. With no GROUP BY the whole
/// (possibly empty) input is one group, so `SELECT COUNT(*)` over no rows still
/// yields one row. NULL keys group together.
#[allow(clippy::type_complexity)]
fn group_rows(
    select: &Select,
    rows: &[Vec<Datum>],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Vec<(Vec<SqlValue>, Vec<usize>)>, SqlError> {
    if select.group_by.is_empty() {
        return Ok(vec![(Vec::new(), (0..rows.len()).collect())]);
    }
    // Key each row and bucket by key in a hash table — O(n) instead of the old
    // O(n·groups) linear probe. `HashKey`'s equality matches the previous
    // `order_key_cmp`-based `keys_equal`, so groups are identical; a side `Vec`
    // preserves first-appearance order (the order the old scan produced).
    use super::hash::{HashKey, fold_hash_key};
    // The collation of each grouping column (a bare column keeps its collation;
    // any other expression takes the database default). Groups are bucketed by
    // the *folded* key so case-insensitive-equal keys collapse into one group,
    // while the stored key keeps the first row's original value for output —
    // `GROUP BY name` over `'ABC'`,`'abc'` returns one group labelled `'ABC'`.
    let key_sens: Vec<truthdb_sql::collation::CollationSensitivity> = select
        .group_by
        .iter()
        .map(|expr| eval::key_collation(expr, resolver))
        .collect();
    let mut index_of: std::collections::HashMap<HashKey, usize> = std::collections::HashMap::new();
    let mut groups: Vec<(Vec<SqlValue>, Vec<usize>)> = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        super::check_cancelled()?;
        let sql_row = row_values(row, types);
        let key = select
            .group_by
            .iter()
            .map(|expr| eval::eval(expr, &sql_row, resolver, eval_ctx))
            .collect::<Result<Vec<_>, _>>()?;
        let folded = fold_hash_key(&key, &key_sens);
        match index_of.get(&HashKey(folded.clone())) {
            Some(&pos) => groups[pos].1.push(index),
            None => {
                index_of.insert(HashKey(folded), groups.len());
                groups.push((key, vec![index]));
            }
        }
    }
    Ok(groups)
}

fn compute_aggregate(
    spec: &AggSpec,
    rows: &[Vec<Datum>],
    types: &[ColumnType],
    resolver: &JoinScope,
    members: &[usize],
    eval_ctx: &EvalContext,
) -> Result<SqlValue, SqlError> {
    // COUNT(*) counts rows including NULLs.
    let Some(arg) = &spec.arg else {
        return Ok(SqlValue::Int(members.len() as i64));
    };
    // The argument column's collation governs DISTINCT deduplication and MIN/MAX
    // for character values (case-insensitive by default), consistent with GROUP
    // BY and WHERE.
    let sensitivity = eval::key_collation(arg, resolver);
    let mut values: Vec<SqlValue> = Vec::new();
    for &index in members {
        let sql_row = row_values(&rows[index], types);
        let value = eval::eval(arg, &sql_row, resolver, eval_ctx)?;
        if !value.is_null() {
            values.push(value);
        }
    }
    if spec.distinct {
        // Dedup case-insensitively under a `_CI` collation: `COUNT(DISTINCT name)`
        // counts `'ABC'` and `'abc'` once. Strings compare by the collation;
        // everything else keeps `order_key_cmp`.
        let cmp = |a: &SqlValue, b: &SqlValue| collated_cmp(a, b, sensitivity);
        values.sort_by(&cmp);
        values.dedup_by(|a, b| cmp(a, b) == std::cmp::Ordering::Equal);
    }
    fold(spec.func, values, sensitivity)
}

/// Compares two aggregate values under a collation: character operands fold by
/// case sensitivity, all others fall back to `order_key_cmp`.
fn collated_cmp(
    a: &SqlValue,
    b: &SqlValue,
    sensitivity: CollationSensitivity,
) -> std::cmp::Ordering {
    match (a, b) {
        (SqlValue::Str(x), SqlValue::Str(y)) => sensitivity.compare_str(x, y),
        _ => order_key_cmp(a, b),
    }
}

fn fold(
    func: AggFunc,
    values: Vec<SqlValue>,
    sensitivity: CollationSensitivity,
) -> Result<SqlValue, SqlError> {
    match func {
        AggFunc::Count => Ok(SqlValue::Int(values.len() as i64)),
        AggFunc::Min | AggFunc::Max => {
            let want_max = func == AggFunc::Max;
            let mut best: Option<SqlValue> = None;
            for value in values {
                best = Some(match best {
                    None => value,
                    Some(current) => {
                        let ord = collated_cmp(&current, &value, sensitivity);
                        let take_new = if want_max {
                            ord == std::cmp::Ordering::Less
                        } else {
                            ord == std::cmp::Ordering::Greater
                        };
                        if take_new { value } else { current }
                    }
                });
            }
            Ok(best.unwrap_or(SqlValue::Null))
        }
        AggFunc::Sum | AggFunc::Avg => {
            if values.is_empty() {
                return Ok(SqlValue::Null);
            }
            let count = values.len() as i64;
            let mut sum: Option<SqlValue> = None;
            for value in values {
                // SUM/AVG accept only numeric types — a character/date operand
                // is error 8117, never string concatenation (which `arith`
                // would otherwise do for `Str + Str`).
                if !matches!(
                    value,
                    SqlValue::Int(_) | SqlValue::Decimal(_) | SqlValue::Float(_)
                ) {
                    let op = if func == AggFunc::Sum { "sum" } else { "avg" };
                    return Err(SqlError::new(
                        8117,
                        16,
                        1,
                        format!(
                            "Operand data type {} is invalid for the {op} operator.",
                            value.type_name()
                        ),
                    ));
                }
                sum = Some(match sum {
                    None => value,
                    Some(acc) => eval::arith(BinaryOp::Add, acc, value)?,
                });
            }
            let sum = sum.expect("non-empty");
            if func == AggFunc::Sum {
                Ok(sum)
            } else {
                // AVG divides by the count; integer AVG truncates (T-SQL).
                eval::arith(BinaryOp::Div, sum, SqlValue::Int(count))
            }
        }
    }
}

/// Rewrites `expr` so it evaluates against the synthetic group row: a whole
/// sub-expression equal to a GROUP BY key becomes `$gk{i}`; an aggregate
/// becomes `$agg{j}` (and is recorded); a bare column that is neither is
/// rejected (error 8120).
fn rewrite(expr: &Expr, group_by: &[Expr], aggs: &mut Vec<AggSpec>) -> Result<Expr, SqlError> {
    for (i, key) in group_by.iter().enumerate() {
        if same_expr(expr, key) {
            return Ok(synthetic_column(&format!("$gk{i}"), expr.span));
        }
    }
    let kind = match &expr.kind {
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => {
            let index = aggs.len();
            aggs.push(AggSpec {
                func: *func,
                distinct: *distinct,
                arg: arg.as_deref().cloned(),
            });
            return Ok(synthetic_column(&format!("$agg{index}"), expr.span));
        }
        ExprKind::Column(name) => {
            return Err(SqlError::new(
                8120,
                16,
                1,
                format!(
                    "Column '{}' is invalid in the select list because it is not contained in either an aggregate function or the GROUP BY clause.",
                    name.value
                ),
            ));
        }
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => expr.kind.clone(),
        // Subqueries are rewritten to literals before aggregation runs; clone
        // defensively (evaluation would reject any that slipped through).
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => {
            expr.kind.clone()
        }
        ExprKind::Unary { op, expr: inner } => ExprKind::Unary {
            op: *op,
            expr: Box::new(rewrite(inner, group_by, aggs)?),
        },
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => ExprKind::IsNull {
            expr: Box::new(rewrite(inner, group_by, aggs)?),
            negated: *negated,
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: Box::new(rewrite(left, group_by, aggs)?),
            right: Box::new(rewrite(right, group_by, aggs)?),
        },
        ExprKind::Cast {
            expr: inner,
            target,
        } => ExprKind::Cast {
            expr: Box::new(rewrite(inner, group_by, aggs)?),
            target: target.clone(),
        },
        ExprKind::Like {
            expr: inner,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: Box::new(rewrite(inner, group_by, aggs)?),
            pattern: Box::new(rewrite(pattern, group_by, aggs)?),
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => ExprKind::InList {
            expr: Box::new(rewrite(inner, group_by, aggs)?),
            list: list
                .iter()
                .map(|e| rewrite(e, group_by, aggs))
                .collect::<Result<_, _>>()?,
            negated: *negated,
        },
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: Box::new(rewrite(inner, group_by, aggs)?),
            low: Box::new(rewrite(low, group_by, aggs)?),
            high: Box::new(rewrite(high, group_by, aggs)?),
            negated: *negated,
        },
        ExprKind::Function { name, args } => ExprKind::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite(a, group_by, aggs))
                .collect::<Result<_, _>>()?,
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: match operand {
                Some(o) => Some(Box::new(rewrite(o, group_by, aggs)?)),
                None => None,
            },
            branches: branches
                .iter()
                .map(|(w, r)| Ok((rewrite(w, group_by, aggs)?, rewrite(r, group_by, aggs)?)))
                .collect::<Result<_, SqlError>>()?,
            else_result: match else_result {
                Some(e) => Some(Box::new(rewrite(e, group_by, aggs)?)),
                None => None,
            },
        },
    };
    Ok(Expr {
        kind,
        span: expr.span,
    })
}

fn synthetic_column(name: &str, span: truthdb_sql::lexer::Span) -> Expr {
    Expr {
        kind: ExprKind::Column(Name {
            value: name.to_string(),
            quoted: false,
            span,
        }),
        span,
    }
}

/// Structural expression equality, ignoring spans, for GROUP BY matching.
/// Covers the forms that appear in GROUP BY (columns, literals, arithmetic,
/// function calls); anything else compares unequal (a conservative miss just
/// forces the 8120 path).
fn same_expr(a: &Expr, b: &Expr) -> bool {
    match (&a.kind, &b.kind) {
        (ExprKind::Column(x), ExprKind::Column(y)) => x.value.eq_ignore_ascii_case(&y.value),
        (ExprKind::Int(x), ExprKind::Int(y)) => x == y,
        (ExprKind::Number(x), ExprKind::Number(y)) => x == y,
        (ExprKind::Str(x), ExprKind::Str(y)) => x == y,
        (ExprKind::Bool(x), ExprKind::Bool(y)) => x == y,
        (ExprKind::Null, ExprKind::Null) => true,
        (ExprKind::Unary { op: ox, expr: ex }, ExprKind::Unary { op: oy, expr: ey }) => {
            ox == oy && same_expr(ex, ey)
        }
        (
            ExprKind::Binary {
                op: ox,
                left: lx,
                right: rx,
            },
            ExprKind::Binary {
                op: oy,
                left: ly,
                right: ry,
            },
        ) => ox == oy && same_expr(lx, ly) && same_expr(rx, ry),
        (ExprKind::Function { name: nx, args: ax }, ExprKind::Function { name: ny, args: ay }) => {
            nx.eq_ignore_ascii_case(ny)
                && ax.len() == ay.len()
                && ax.iter().zip(ay).all(|(x, y)| same_expr(x, y))
        }
        (
            ExprKind::Cast {
                expr: ex,
                target: tx,
            },
            ExprKind::Cast {
                expr: ey,
                target: ty,
            },
        ) => tx == ty && same_expr(ex, ey),
        (
            ExprKind::IsNull {
                expr: ex,
                negated: nx,
            },
            ExprKind::IsNull {
                expr: ey,
                negated: ny,
            },
        ) => nx == ny && same_expr(ex, ey),
        (
            ExprKind::Like {
                expr: ex,
                pattern: px,
                escape: cx,
                negated: nx,
            },
            ExprKind::Like {
                expr: ey,
                pattern: py,
                escape: cy,
                negated: ny,
            },
        ) => cx == cy && nx == ny && same_expr(ex, ey) && same_expr(px, py),
        (
            ExprKind::InList {
                expr: ex,
                list: lx,
                negated: nx,
            },
            ExprKind::InList {
                expr: ey,
                list: ly,
                negated: ny,
            },
        ) => {
            nx == ny
                && same_expr(ex, ey)
                && lx.len() == ly.len()
                && lx.iter().zip(ly).all(|(a, b)| same_expr(a, b))
        }
        (
            ExprKind::Between {
                expr: ex,
                low: lox,
                high: hix,
                negated: nx,
            },
            ExprKind::Between {
                expr: ey,
                low: loy,
                high: hiy,
                negated: ny,
            },
        ) => nx == ny && same_expr(ex, ey) && same_expr(lox, loy) && same_expr(hix, hiy),
        (
            ExprKind::Case {
                operand: ox,
                branches: bx,
                else_result: elx,
            },
            ExprKind::Case {
                operand: oy,
                branches: by,
                else_result: ely,
            },
        ) => {
            same_opt(ox, oy)
                && bx.len() == by.len()
                && bx
                    .iter()
                    .zip(by)
                    .all(|((wx, rx), (wy, ry))| same_expr(wx, wy) && same_expr(rx, ry))
                && same_opt(elx, ely)
        }
        (ExprKind::GlobalVar(x), ExprKind::GlobalVar(y)) => x.eq_ignore_ascii_case(y),
        (
            ExprKind::Aggregate {
                func: fx,
                distinct: dx,
                arg: ax,
            },
            ExprKind::Aggregate {
                func: fy,
                distinct: dy,
                arg: ay,
            },
        ) => fx == fy && dx == dy && same_opt(ax, ay),
        _ => false,
    }
}

fn same_opt(a: &Option<Box<Expr>>, b: &Option<Box<Expr>>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => same_expr(x, y),
        (None, None) => true,
        _ => false,
    }
}

/// The output column name for a SELECT item: its alias, else the column name
/// for a bare column, else empty (SQL Server leaves computed columns unnamed).
fn output_name(expr: &Expr, alias: Option<&Name>) -> String {
    if let Some(alias) = alias {
        return alias.value.clone();
    }
    match &expr.kind {
        ExprKind::Column(name) => name.value.rsplit('.').next().unwrap_or("").to_string(),
        _ => String::new(),
    }
}

/// Builds a typed RowSet from evaluated output rows: each column's type is
/// inferred from its values, then every value is coerced to it. A coercion
/// failure (overflow/truncation) is propagated — matching the plain projection
/// path — rather than masked as NULL.
fn build_rowset(names: Vec<String>, rows: Vec<Vec<SqlValue>>) -> Result<RowSet, SqlError> {
    let width = names.len();
    let mut columns = Vec::with_capacity(width);
    for (index, name) in names.into_iter().enumerate() {
        let column_values: Vec<SqlValue> = rows.iter().map(|r| r[index].clone()).collect();
        let column_type = value::infer_type(&column_values);
        columns.push(ResultColumn { name, column_type });
    }
    let mut out_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut out = Vec::with_capacity(width);
        for (index, v) in row.into_iter().enumerate() {
            out.push(value::sql_to_datum(
                &v,
                &columns[index].column_type,
                &columns[index].name,
            )?);
        }
        out_rows.push(out);
    }
    Ok(RowSet {
        columns,
        rows: out_rows,
    })
}
