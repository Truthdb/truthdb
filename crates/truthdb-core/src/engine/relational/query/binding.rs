use super::super::prelude::*;

// ---- subquery resolution ------------------------------------------------

/// Returns a copy of a SELECT with every subquery in its expressions
/// (WHERE/HAVING/SELECT list/GROUP BY/ORDER BY) evaluated and replaced by a
/// precomputed literal. Subqueries in a FROM-clause join `ON` are not rewritten
/// here (they are rare and error at evaluation). Only uncorrelated subqueries
/// are supported; a correlated one references an outer column and fails to
/// resolve when executed independently.
pub(in crate::engine::relational) fn rewrite_select_subqueries(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<Select, SqlError> {
    // The columns this query exposes to a correlated subquery. A correlated
    // subquery in the WHERE or the SELECT list is left un-evaluated here (the
    // per-row loops bind the outer row), and one in HAVING likewise (the
    // per-group loop binds the group row). GROUP BY and ORDER BY do not
    // support correlation and evaluate as before.
    let self_scope = select
        .from
        .as_ref()
        .and_then(|from| from_column_names(storage, eval_ctx.database_id, from))
        .map(|columns| JoinScope {
            collations: Vec::new(),
            columns,
        });
    let items = select
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, alias } => Ok(SelectItem::Expr {
                expr: rewrite_subqueries(storage, expr, eval_ctx, self_scope.as_ref())?,
                alias: alias.clone(),
            }),
            other => Ok(other.clone()),
        })
        .collect::<Result<Vec<_>, SqlError>>()?;
    let where_clause = select
        .where_clause
        .as_ref()
        .map(|e| rewrite_subqueries(storage, e, eval_ctx, self_scope.as_ref()))
        .transpose()?;
    let having = select
        .having
        .as_ref()
        .map(|e| rewrite_subqueries(storage, e, eval_ctx, self_scope.as_ref()))
        .transpose()?;
    let group_by = select
        .group_by
        .iter()
        .map(|e| rewrite_subqueries(storage, e, eval_ctx, None))
        .collect::<Result<Vec<_>, SqlError>>()?;
    let order_by = select
        .order_by
        .iter()
        .map(|o| {
            Ok(OrderItem {
                expr: rewrite_subqueries(storage, &o.expr, eval_ctx, None)?,
                descending: o.descending,
            })
        })
        .collect::<Result<Vec<_>, SqlError>>()?;
    Ok(Select {
        ctes: select.ctes.clone(),
        top: select.top,
        distinct: select.distinct,
        items,
        from: select.from.clone(),
        where_clause,
        group_by,
        having,
        order_by,
        span: select.span,
    })
}

/// Recursively replaces each subquery node in an expression with its evaluated
/// result: a scalar `(SELECT ...)` -> a literal, `EXISTS (...)` -> a boolean,
/// `expr IN (SELECT ...)` -> an `InList` of the subquery's values.
pub(in crate::engine::relational) fn rewrite_subqueries(
    storage: &Storage,
    expr: &Expr,
    eval_ctx: &EvalContext,
    correlated_scope: Option<&JoinScope>,
) -> Result<Expr, SqlError> {
    let recur =
        |storage: &Storage, e: &Expr| rewrite_subqueries(storage, e, eval_ctx, correlated_scope);
    let recur_box = |storage: &Storage, e: &Expr| -> Result<Box<Expr>, SqlError> {
        Ok(Box::new(recur(storage, e)?))
    };
    let recur_opt =
        |storage: &Storage, e: &Option<Box<Expr>>| -> Result<Option<Box<Expr>>, SqlError> {
            e.as_ref().map(|e| recur_box(storage, e)).transpose()
        };
    // A subquery correlated with the enclosing query (`correlated_scope`) is left
    // in place — the per-row WHERE loop substitutes its outer references and runs
    // it once per outer row (`substitute_correlated_in_expr`).
    let leave_correlated = |select: &Select| {
        correlated_scope
            .is_some_and(|scope| is_correlated(storage, eval_ctx.database_id, select, scope))
    };
    let kind = match &expr.kind {
        ExprKind::Subquery(select) if leave_correlated(select) => expr.kind.clone(),
        ExprKind::Exists(select) if leave_correlated(select) => expr.kind.clone(),
        ExprKind::InSubquery { subquery, .. } if leave_correlated(subquery) => expr.kind.clone(),
        ExprKind::Subquery(select) => {
            ExprKind::Literal(eval_scalar_subquery(storage, select, eval_ctx)?)
        }
        ExprKind::Exists(select) => {
            let rowset = exec_select(storage, select, eval_ctx)?;
            ExprKind::Bool(!rowset.rows.is_empty())
        }
        ExprKind::InSubquery {
            expr: lhs,
            subquery,
            negated,
        } => {
            let lhs = recur_box(storage, lhs)?;
            let list = eval_in_subquery(storage, subquery, eval_ctx)?
                .into_iter()
                .map(|v| Expr {
                    kind: ExprKind::Literal(v),
                    span: expr.span,
                })
                .collect();
            ExprKind::InList {
                expr: lhs,
                list,
                negated: *negated,
            }
        }
        ExprKind::Unary { op, expr: e } => ExprKind::Unary {
            op: *op,
            expr: recur_box(storage, e)?,
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: recur_box(storage, left)?,
            right: recur_box(storage, right)?,
        },
        ExprKind::IsNull { expr: e, negated } => ExprKind::IsNull {
            expr: recur_box(storage, e)?,
            negated: *negated,
        },
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: recur_box(storage, e)?,
            pattern: recur_box(storage, pattern)?,
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: e,
            list,
            negated,
        } => ExprKind::InList {
            expr: recur_box(storage, e)?,
            list: list
                .iter()
                .map(|x| recur(storage, x))
                .collect::<Result<_, _>>()?,
            negated: *negated,
        },
        ExprKind::Between {
            expr: e,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: recur_box(storage, e)?,
            low: recur_box(storage, low)?,
            high: recur_box(storage, high)?,
            negated: *negated,
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: recur_opt(storage, operand)?,
            branches: branches
                .iter()
                .map(|(w, r)| Ok((recur(storage, w)?, recur(storage, r)?)))
                .collect::<Result<_, SqlError>>()?,
            else_result: recur_opt(storage, else_result)?,
        },
        ExprKind::Cast { expr: e, target } => ExprKind::Cast {
            expr: recur_box(storage, e)?,
            target: target.clone(),
        },
        ExprKind::Function { name, args } => ExprKind::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| recur(storage, a))
                .collect::<Result<_, _>>()?,
        },
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => ExprKind::Aggregate {
            func: *func,
            distinct: *distinct,
            arg: recur_opt(storage, arg)?,
        },
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => expr.kind.clone(),
    };
    Ok(Expr {
        kind,
        span: expr.span,
    })
}

/// Evaluates a scalar subquery to a single value: NULL for 0 rows, the value
/// for 1 row, error 512 for more than 1 row; error 116 if it is not exactly one
/// column wide.
pub(in crate::engine::relational) fn eval_scalar_subquery(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<SqlValue, SqlError> {
    let rowset = exec_select(storage, select, eval_ctx)?;
    if rowset.columns.len() != 1 {
        return Err(scalar_subquery_shape_err());
    }
    match rowset.rows.len() {
        0 => Ok(SqlValue::Null),
        1 => Ok(value::datum_to_sql(
            &rowset.rows[0][0],
            &rowset.columns[0].column_type,
        )),
        _ => Err(SqlError::new(
            512,
            16,
            1,
            "Subquery returned more than 1 value. This is not permitted when the subquery follows =, !=, <, <=, >, >= or when the subquery is used as an expression.",
        )),
    }
}

/// Evaluates an `IN (SELECT ...)` subquery to its list of values (one column,
/// else error 116).
pub(in crate::engine::relational) fn eval_in_subquery(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<Vec<SqlValue>, SqlError> {
    let rowset = exec_select(storage, select, eval_ctx)?;
    if rowset.columns.len() != 1 {
        return Err(scalar_subquery_shape_err());
    }
    let column_type = rowset.columns[0].column_type;
    Ok(rowset
        .rows
        .iter()
        .map(|r| value::datum_to_sql(&r[0], &column_type))
        .collect())
}

pub(in crate::engine::relational) fn scalar_subquery_shape_err() -> SqlError {
    SqlError::new(
        116,
        16,
        1,
        "Only one expression can be specified in the select list when the subquery is not introduced with EXISTS.",
    )
}

// ---- correlated subquery support ----------------------------------------
//
// A subquery that references an enclosing query's column is *correlated*. It is
// left un-evaluated by `rewrite_select_subqueries` (which only folds away
// uncorrelated subqueries) and instead re-run once per outer row: the per-row
// WHERE loop calls `substitute_correlated_in_expr`, which binds the outer row's
// values into the subquery (`substitute_subquery_outer_refs`) and evaluates it.
// This is the "correct, slow, honest" per-row apply. Supported for correlated
// subqueries in the WHERE clause over base-table / join subqueries; a correlated
// reference inside a derived-table / view subquery (whose inner scope cannot be
// read from the catalog) falls back to the prior behavior (invalid-column 207).

/// The `(qualifier, bare column name)` columns a FROM clause exposes, read from
/// the catalog WITHOUT materializing rows. `None` if the FROM has a derived
/// table or a view, whose output columns need binding to determine.
pub(in crate::engine::relational) fn from_column_names(
    storage: &Storage,
    db_id: u32,
    from: &TableRef,
) -> Option<Vec<(Option<String>, String)>> {
    match from {
        TableRef::Table { name, alias } => {
            let def = resolve_table(storage, db_id, &name.value)?;
            // A view defers to its expansion; a PROCEDURE/FUNCTION/TRIGGER must
            // not read as a zero-column table — bailing here routes to the
            // collecting path, which errors 2809/208.
            if def.is_view() || def.is_procedure() || def.is_function() || def.is_trigger() {
                return None;
            }
            let qualifier = alias
                .as_ref()
                .map(|a| a.value.clone())
                .unwrap_or_else(|| strip_schema(&name.value).to_string());
            Some(
                def.columns
                    .iter()
                    .map(|(cname, _, _)| (Some(qualifier.clone()), cname.clone()))
                    .collect(),
            )
        }
        TableRef::Join { left, right, .. } => {
            let mut cols = from_column_names(storage, db_id, left)?;
            cols.extend(from_column_names(storage, db_id, right)?);
            Some(cols)
        }
        TableRef::Derived { subquery, alias } => {
            let mut cols = Vec::new();
            for item in &subquery.items {
                match item {
                    SelectItem::Expr { expr, alias: a } => {
                        let name = a
                            .as_ref()
                            .map(|n| n.value.clone())
                            .or_else(|| bare_column_name(expr))?;
                        cols.push((Some(alias.value.clone()), name));
                    }
                    SelectItem::Wildcard => {
                        let inner = from_column_names(storage, db_id, subquery.from.as_ref()?)?;
                        cols.extend(
                            inner
                                .into_iter()
                                .map(|(_, n)| (Some(alias.value.clone()), n)),
                        );
                    }
                    SelectItem::QualifiedWildcard(q) => {
                        let inner = from_column_names(storage, db_id, subquery.from.as_ref()?)?;
                        cols.extend(
                            inner
                                .into_iter()
                                .filter(|(qu, _)| {
                                    qu.as_deref()
                                        .is_some_and(|x| x.eq_ignore_ascii_case(&q.value))
                                })
                                .map(|(_, n)| (Some(alias.value.clone()), n)),
                        );
                    }
                    SelectItem::Assign { .. } => return None,
                }
            }
            Some(cols)
        }
        // A TVF's output columns are its body SELECT's projection — only known
        // after the body is parsed and bound, like a view. Defer to expansion.
        TableRef::Function { .. } => None,
    }
}

/// The inner scope of a subquery (its own FROM columns), or `None` if it cannot
/// be determined from the catalog alone.
pub(in crate::engine::relational) fn subquery_inner_scope(
    storage: &Storage,
    db_id: u32,
    subquery: &Select,
) -> Option<JoinScope> {
    let columns = match &subquery.from {
        Some(from) => from_column_names(storage, db_id, from)?,
        None => Vec::new(),
    };
    Some(JoinScope {
        collations: Vec::new(),
        columns,
    })
}

/// True if `subquery` references a column that resolves in the enclosing `outer`
/// scope but not in its own FROM — i.e. it is correlated.
pub(in crate::engine::relational) fn is_correlated(
    storage: &Storage,
    db_id: u32,
    subquery: &Select,
    outer: &JoinScope,
) -> bool {
    let Some(inner) = subquery_inner_scope(storage, db_id, subquery) else {
        return false;
    };
    let mut correlated = false;
    select_column_refs(subquery, &mut |name| {
        // `matches_any` (not `resolve`) so an *ambiguous* inner column is treated
        // as inner (it errors in the subquery) rather than rebound to the outer.
        if !inner.matches_any(&name.value) && outer.resolve(&name.value).is_some() {
            correlated = true;
        }
    });
    // A correlated reference may live inside a derived table's body — its own
    // clauses resolve in the derived scope, so the walk above never sees it.
    correlated || from_has_correlated_derived(storage, db_id, subquery.from.as_ref(), outer)
}

/// Calls `f` on every column reference inside an AGGREGATE argument anywhere
/// in the select's own clauses (not descending into nested subqueries).
pub(in crate::engine::relational) fn select_aggregate_arg_refs(
    select: &Select,
    f: &mut impl FnMut(&Name),
) {
    fn walk(expr: &Expr, f: &mut impl FnMut(&Name)) {
        match &expr.kind {
            ExprKind::Aggregate { arg: Some(a), .. } => expr_column_refs(a, f),
            ExprKind::Aggregate { arg: None, .. } => {}
            ExprKind::Unary { expr: e, .. }
            | ExprKind::IsNull { expr: e, .. }
            | ExprKind::Cast { expr: e, .. } => walk(e, f),
            ExprKind::Binary { left, right, .. } => {
                walk(left, f);
                walk(right, f);
            }
            ExprKind::Like {
                expr: e, pattern, ..
            } => {
                walk(e, f);
                walk(pattern, f);
            }
            ExprKind::InList { expr: e, list, .. } => {
                walk(e, f);
                list.iter().for_each(|x| walk(x, f));
            }
            ExprKind::Between {
                expr: e, low, high, ..
            } => {
                walk(e, f);
                walk(low, f);
                walk(high, f);
            }
            ExprKind::Function { args, .. } => args.iter().for_each(|x| walk(x, f)),
            ExprKind::Case {
                operand,
                branches,
                else_result,
            } => {
                if let Some(o) = operand {
                    walk(o, f);
                }
                for (w, r) in branches {
                    walk(w, f);
                    walk(r, f);
                }
                if let Some(e) = else_result {
                    walk(e, f);
                }
            }
            _ => {}
        }
    }
    if let Some(w) = &select.where_clause {
        walk(w, f);
    }
    for item in &select.items {
        if let SelectItem::Expr { expr, .. } = item {
            walk(expr, f);
        }
    }
    if let Some(h) = &select.having {
        walk(h, f);
    }
}

/// Whether any derived-table body in a FROM tree is correlated to `outer`.
pub(in crate::engine::relational) fn from_has_correlated_derived(
    storage: &Storage,
    db_id: u32,
    from: Option<&TableRef>,
    outer: &JoinScope,
) -> bool {
    match from {
        None | Some(TableRef::Table { .. }) => false,
        Some(TableRef::Join { left, right, .. }) => {
            from_has_correlated_derived(storage, db_id, Some(left), outer)
                || from_has_correlated_derived(storage, db_id, Some(right), outer)
        }
        Some(TableRef::Derived { subquery, .. }) => is_correlated(storage, db_id, subquery, outer),
        // A TVF's literal/non-outer arguments do not correlate it to the outer
        // FROM (APPLY, with outer-referencing args, is out of scope).
        Some(TableRef::Function { .. }) => false,
    }
}

/// Calls `f` on every column reference in a select's own clauses (WHERE, SELECT
/// items, HAVING, GROUP BY, ORDER BY), not descending into nested subqueries
/// (which resolve in their own scope).
pub(in crate::engine::relational) fn select_column_refs(
    select: &Select,
    f: &mut impl FnMut(&Name),
) {
    if let Some(w) = &select.where_clause {
        expr_column_refs(w, f);
    }
    for item in &select.items {
        match item {
            SelectItem::Expr { expr, .. } => expr_column_refs(expr, f),
            SelectItem::Assign { value, .. } => expr_column_refs(value, f),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {}
        }
    }
    if let Some(h) = &select.having {
        expr_column_refs(h, f);
    }
    for e in &select.group_by {
        expr_column_refs(e, f);
    }
    for o in &select.order_by {
        expr_column_refs(&o.expr, f);
    }
}

/// Calls `f` on every column reference in an expression, not descending into
/// nested subquery bodies.
pub(in crate::engine::relational) fn expr_column_refs(expr: &Expr, f: &mut impl FnMut(&Name)) {
    match &expr.kind {
        ExprKind::Column(name) => f(name),
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_)
        | ExprKind::Subquery(_)
        | ExprKind::Exists(_) => {}
        // The IN operand is at this scope; the subquery body is not.
        ExprKind::InSubquery { expr: e, .. } => expr_column_refs(e, f),
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => expr_column_refs(e, f),
        ExprKind::Binary { left, right, .. } => {
            expr_column_refs(left, f);
            expr_column_refs(right, f);
        }
        ExprKind::Like {
            expr: e, pattern, ..
        } => {
            expr_column_refs(e, f);
            expr_column_refs(pattern, f);
        }
        ExprKind::InList { expr: e, list, .. } => {
            expr_column_refs(e, f);
            list.iter().for_each(|x| expr_column_refs(x, f));
        }
        ExprKind::Between {
            expr: e, low, high, ..
        } => {
            expr_column_refs(e, f);
            expr_column_refs(low, f);
            expr_column_refs(high, f);
        }
        ExprKind::Function { args, .. } => args.iter().for_each(|a| expr_column_refs(a, f)),
        ExprKind::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                expr_column_refs(a, f);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(o) = operand {
                expr_column_refs(o, f);
            }
            for (w, r) in branches {
                expr_column_refs(w, f);
                expr_column_refs(r, f);
            }
            if let Some(e) = else_result {
                expr_column_refs(e, f);
            }
        }
    }
}

/// Rewrites ORDER BY so a SELECT-list alias resolves on the path that sorts the
/// *source* rows.
///
/// ORDER BY is the one clause where a SELECT alias is in scope, but this path
/// sorts base rows against a [`JoinScope`], which knows only base columns — so
/// `SELECT v AS vv FROM a ORDER BY vv` would not resolve, and an alias over a
/// computed expression (`SELECT v * 2 AS dbl ... ORDER BY dbl`) has no base
/// column to fall back on at all. Each unqualified name matching an alias is
/// replaced by that alias's expression, which the existing sort machinery then
/// evaluates against the base row. (The grouped/DISTINCT path projects first and
/// resolves ORDER BY against the output names, so it already sees aliases.)
///
/// An alias shadows a base column of the same name, as in SQL Server. A
/// qualified name (`a.v`) always means that table's column and is never
/// rewritten. `map_expr_columns` does not rescan what it substitutes, so a
/// self-referential alias (`SELECT v + 1 AS v ... ORDER BY v`) substitutes once
/// instead of recursing forever. Ordinals (`ORDER BY 1`) are integers, not
/// column references, so they are untouched.
pub(in crate::engine::relational) fn order_by_with_aliases(
    order_by: &[OrderItem],
    items: &[SelectItem],
    scope: &JoinScope,
) -> Result<Vec<OrderItem>, SqlError> {
    let aliases: Vec<(&str, &Expr)> = items
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr {
                expr,
                alias: Some(name),
            } => Some((name.value.as_str(), expr)),
            _ => None,
        })
        .collect();
    let outputs = output_exprs(items, scope);
    order_by
        .iter()
        .map(|item| {
            // A bare integer is a 1-based output-column ordinal, not a value.
            let expr = if let ExprKind::Int(n) = &item.expr.kind {
                usize::try_from(*n)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|i| outputs.get(i).cloned())
                    .ok_or_else(|| {
                        SqlError::new(
                            108,
                            16,
                            1,
                            format!("The ORDER BY position number {n} is out of range."),
                        )
                    })?
            } else {
                map_expr_columns(&item.expr, &|name: &Name| {
                    if name.value.contains('.') {
                        return None;
                    }
                    aliases
                        .iter()
                        .find(|(alias, _)| name.eq_ignore_case(alias))
                        .map(|(_, expr)| (*expr).clone())
                })
            };
            Ok(OrderItem {
                expr,
                descending: item.descending,
            })
        })
        .collect()
}

/// The select list as one source-evaluable expression per *output* column, so a
/// positional `ORDER BY <n>` can name what it points at. A wildcard expands to
/// its source columns, each referenced by qualifier where it has one — `a.v`
/// rather than `v` — so a join with a repeated column name stays unambiguous.
pub(in crate::engine::relational) fn output_exprs(
    items: &[SelectItem],
    scope: &JoinScope,
) -> Vec<Expr> {
    // Synthetic: built from the scope, so it resolves by construction and its
    // span is never surfaced in an error.
    let synthetic = Span::new(0, 0);
    let column_expr = |index: usize| {
        let (qualifier, column) = &scope.columns[index];
        let value = match qualifier {
            Some(q) => format!("{q}.{column}"),
            None => column.clone(),
        };
        Expr {
            span: synthetic,
            kind: ExprKind::Column(Name {
                value,
                quoted: false,
                span: synthetic,
            }),
        }
    };
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => out.extend((0..scope.columns.len()).map(column_expr)),
            SelectItem::QualifiedWildcard(qualifier) => out.extend(
                scope
                    .indices_for_qualifier(&qualifier.value)
                    .into_iter()
                    .map(column_expr),
            ),
            SelectItem::Expr { expr, .. } => out.push(expr.clone()),
            // An assignment SELECT produces no result columns to order by.
            SelectItem::Assign { .. } => {}
        }
    }
    out
}

/// Replaces every column reference in an expression via `f` (a replacement, or
/// `None` to keep), not descending into nested subquery bodies (but mapping an
/// `IN (SELECT)` operand, which is at this scope).
pub(in crate::engine::relational) fn map_expr_columns(
    expr: &Expr,
    f: &impl Fn(&Name) -> Option<Expr>,
) -> Expr {
    let map = |e: &Expr| map_expr_columns(e, f);
    let map_box = |e: &Expr| Box::new(map_expr_columns(e, f));
    let kind = match &expr.kind {
        ExprKind::Column(name) => match f(name) {
            Some(replacement) => return replacement,
            None => expr.kind.clone(),
        },
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_)
        | ExprKind::Subquery(_)
        | ExprKind::Exists(_) => expr.kind.clone(),
        ExprKind::InSubquery {
            expr: e,
            subquery,
            negated,
        } => ExprKind::InSubquery {
            expr: map_box(e),
            subquery: subquery.clone(),
            negated: *negated,
        },
        ExprKind::Unary { op, expr: e } => ExprKind::Unary {
            op: *op,
            expr: map_box(e),
        },
        ExprKind::IsNull { expr: e, negated } => ExprKind::IsNull {
            expr: map_box(e),
            negated: *negated,
        },
        ExprKind::Cast { expr: e, target } => ExprKind::Cast {
            expr: map_box(e),
            target: target.clone(),
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: map_box(left),
            right: map_box(right),
        },
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: map_box(e),
            pattern: map_box(pattern),
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: e,
            list,
            negated,
        } => ExprKind::InList {
            expr: map_box(e),
            list: list.iter().map(map).collect(),
            negated: *negated,
        },
        ExprKind::Between {
            expr: e,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: map_box(e),
            low: map_box(low),
            high: map_box(high),
            negated: *negated,
        },
        ExprKind::Function { name, args } => ExprKind::Function {
            name: name.clone(),
            args: args.iter().map(map).collect(),
        },
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => ExprKind::Aggregate {
            func: *func,
            distinct: *distinct,
            arg: arg.as_ref().map(|a| map_box(a)),
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: operand.as_ref().map(|o| map_box(o)),
            branches: branches.iter().map(|(w, r)| (map(w), map(r))).collect(),
            else_result: else_result.as_ref().map(|e| map_box(e)),
        },
    };
    Expr {
        kind,
        span: expr.span,
    }
}

/// A copy of `subquery` with references to the enclosing query's columns (per
/// `outer`) replaced by the current outer row's literal values — making a
/// correlated subquery uncorrelated for that row. `None` if the inner scope
/// cannot be determined; the caller then runs the subquery unchanged.
pub(in crate::engine::relational) fn substitute_subquery_outer_refs(
    storage: &Storage,
    db_id: u32,
    subquery: &Select,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
) -> Option<Select> {
    let inner = subquery_inner_scope(storage, db_id, subquery)?;
    let substitute = |name: &Name| -> Option<Expr> {
        if inner.matches_any(&name.value) {
            return None; // the subquery's own column wins (even if ambiguous)
        }
        let index = outer(&name.value)?;
        Some(Expr {
            kind: ExprKind::Literal(outer_row.get(index)?.clone()),
            span: name.span,
        })
    };
    // An outer reference INSIDE an aggregate argument has outer-aggregate
    // semantics in SQL Server (the aggregate computes over the OUTER group);
    // substituting a per-row literal would silently compute something else.
    // Bail — the subquery runs unchanged and errors cleanly.
    let mut outer_in_agg = false;
    select_aggregate_arg_refs(subquery, &mut |name| {
        if !inner.matches_any(&name.value) && outer(&name.value).is_some() {
            outer_in_agg = true;
        }
    });
    if outer_in_agg {
        return None;
    }
    let mut out = subquery.clone();
    out.where_clause = out
        .where_clause
        .as_ref()
        .map(|e| map_expr_columns(e, &substitute));
    out.items = out
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, alias } => SelectItem::Expr {
                expr: map_expr_columns(expr, &substitute),
                alias: alias.clone(),
            },
            other => other.clone(),
        })
        .collect();
    out.having = out
        .having
        .as_ref()
        .map(|e| map_expr_columns(e, &substitute));
    out.group_by = out
        .group_by
        .iter()
        .map(|e| map_expr_columns(e, &substitute))
        .collect();
    out.order_by = out
        .order_by
        .iter()
        .map(|o| OrderItem {
            expr: map_expr_columns(&o.expr, &substitute),
            descending: o.descending,
        })
        .collect();
    // A correlated reference INSIDE a derived table's body lives in `from`,
    // not in any expression above — descend and substitute there too. The
    // recursive call's own inner-scope check handles shadowing.
    if let Some(from) = out.from.as_mut() {
        substitute_from_outer_refs(storage, db_id, from, outer, outer_row)?;
    }
    Some(out)
}

/// Substitutes outer references inside every derived-table body of a FROM
/// tree. `None` when any derived body's scope cannot be determined.
pub(in crate::engine::relational) fn substitute_from_outer_refs(
    storage: &Storage,
    db_id: u32,
    from: &mut TableRef,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
) -> Option<()> {
    match from {
        TableRef::Table { .. } => Some(()),
        TableRef::Join { left, right, .. } => {
            substitute_from_outer_refs(storage, db_id, left, outer, outer_row)?;
            substitute_from_outer_refs(storage, db_id, right, outer, outer_row)
        }
        TableRef::Derived { subquery, .. } => {
            **subquery =
                substitute_subquery_outer_refs(storage, db_id, subquery, outer, outer_row)?;
            Some(())
        }
        // A TVF's body lives in the catalog (bound at expansion, not here) and
        // its literal arguments carry no outer references.
        TableRef::Function { .. } => Some(()),
    }
}

/// Evaluates each correlated subquery in `expr` against `outer_row` (binding the
/// enclosing query's columns per `outer`) and replaces it with a literal —
/// producing a subquery-free predicate for that outer row.
/// A [`ColumnResolver`] over a bare name→index closure (the `outer` resolver the
/// correlated-substitution pass carries), so a user scalar function's arguments
/// can be evaluated against the current row.
pub(in crate::engine::relational) struct FnResolver<'a>(
    pub(super) &'a dyn Fn(&str) -> Option<usize>,
);

impl truthdb_sql::eval::ColumnResolver for FnResolver<'_> {
    fn resolve(&self, name: &str) -> Option<usize> {
        (self.0)(name)
    }
}

/// Resolves a function-call name to a user-defined SCALAR function, or `None`
/// (an unknown name, a built-in, or a table-valued function). Schema-qualified
/// (`dbo.f`) and bare names both resolve; a bare name that shadows a built-in
/// takes the user function (a documented minor divergence from SQL Server, which
/// requires schema-qualified UDF calls).
pub(in crate::engine::relational) fn resolve_scalar_function(
    storage: &Storage,
    db_id: u32,
    name: &str,
) -> Option<TableDef> {
    // A bare (unqualified) name that matches a built-in always binds to the
    // built-in — a same-named UDF is reached only by its schema-qualified name
    // (`dbo.abs`), as SQL Server requires. Without this a UDF named like a
    // built-in would silently hijack every unqualified call to that name.
    if !name.contains('.') && truthdb_sql::functions::is_builtin_function(name) {
        return None;
    }
    let def = resolve_table(storage, db_id, name)?;
    match def.function.as_ref()?.returns {
        FunctionReturns::Scalar { .. } => Some(def),
        // A table-valued function is not a scalar call (it resolves in FROM).
        FunctionReturns::InlineTable { .. } | FunctionReturns::MultiStatementTable { .. } => None,
    }
}

/// True if an expression contains a call to a user-defined scalar function —
/// which, like a subquery, cannot be evaluated by the pure eval crate and must
/// be rewritten to a literal per row first.
pub(in crate::engine::relational) fn expr_has_user_function(
    storage: &Storage,
    db_id: u32,
    expr: &Expr,
) -> bool {
    let has = |e: &Expr| expr_has_user_function(storage, db_id, e);
    match &expr.kind {
        ExprKind::Function { name, args } => {
            resolve_scalar_function(storage, db_id, name).is_some() || args.iter().any(has)
        }
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_)
        | ExprKind::Subquery(_)
        | ExprKind::Exists(_)
        | ExprKind::InSubquery { .. } => false,
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => has(e),
        ExprKind::Binary { left, right, .. } => has(left) || has(right),
        ExprKind::Like {
            expr: e, pattern, ..
        } => has(e) || has(pattern),
        ExprKind::InList { expr: e, list, .. } => has(e) || list.iter().any(has),
        ExprKind::Between {
            expr: e, low, high, ..
        } => has(e) || has(low) || has(high),
        ExprKind::Aggregate { arg, .. } => arg.as_deref().is_some_and(has),
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            operand.as_deref().is_some_and(has)
                || branches.iter().any(|(w, r)| has(w) || has(r))
                || else_result.as_deref().is_some_and(has)
        }
    }
}

/// True if an expression needs the per-row storage-aware rewrite (a subquery or
/// a user scalar function) before the pure evaluator can run on it.
pub(in crate::engine::relational) fn expr_needs_binding(
    storage: &Storage,
    db_id: u32,
    expr: &Expr,
) -> bool {
    expr_has_subquery(expr) || expr_has_user_function(storage, db_id, expr)
}

pub(in crate::engine::relational) fn substitute_correlated_in_expr(
    storage: &Storage,
    expr: &Expr,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
    eval_ctx: &EvalContext,
) -> Result<Expr, SqlError> {
    let recur = |e: &Expr| substitute_correlated_in_expr(storage, e, outer, outer_row, eval_ctx);
    let recur_box = |e: &Expr| -> Result<Box<Expr>, SqlError> { Ok(Box::new(recur(e)?)) };
    let bind = |sq: &Select| -> Select {
        substitute_subquery_outer_refs(storage, eval_ctx.database_id, sq, outer, outer_row)
            .unwrap_or_else(|| sq.clone())
    };
    let kind = match &expr.kind {
        ExprKind::Subquery(sq) => {
            ExprKind::Literal(eval_scalar_subquery(storage, &bind(sq), eval_ctx)?)
        }
        ExprKind::Exists(sq) => {
            let rowset = exec_select(storage, &bind(sq), eval_ctx)?;
            ExprKind::Bool(!rowset.rows.is_empty())
        }
        ExprKind::InSubquery {
            expr: lhs,
            subquery,
            negated,
        } => {
            let lhs = recur_box(lhs)?;
            let list = eval_in_subquery(storage, &bind(subquery), eval_ctx)?
                .into_iter()
                .map(|v| Expr {
                    kind: ExprKind::Literal(v),
                    span: expr.span,
                })
                .collect();
            ExprKind::InList {
                expr: lhs,
                list,
                negated: *negated,
            }
        }
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => expr.kind.clone(),
        ExprKind::Unary { op, expr: e } => ExprKind::Unary {
            op: *op,
            expr: recur_box(e)?,
        },
        ExprKind::IsNull { expr: e, negated } => ExprKind::IsNull {
            expr: recur_box(e)?,
            negated: *negated,
        },
        ExprKind::Cast { expr: e, target } => ExprKind::Cast {
            expr: recur_box(e)?,
            target: target.clone(),
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: recur_box(left)?,
            right: recur_box(right)?,
        },
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: recur_box(e)?,
            pattern: recur_box(pattern)?,
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: e,
            list,
            negated,
        } => ExprKind::InList {
            expr: recur_box(e)?,
            list: list.iter().map(&recur).collect::<Result<_, _>>()?,
            negated: *negated,
        },
        ExprKind::Between {
            expr: e,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: recur_box(e)?,
            low: recur_box(low)?,
            high: recur_box(high)?,
            negated: *negated,
        },
        ExprKind::Function { name, args } => {
            let args = args.iter().map(&recur).collect::<Result<Vec<_>, _>>()?;
            // A call that resolves to a user scalar function runs its body once
            // for this row (its arguments evaluated against the row) and folds to
            // the returned value — the same rewrite-to-literal discipline
            // subqueries use, keeping scalar evaluation free of storage access.
            if let Some(def) = resolve_scalar_function(storage, eval_ctx.database_id, name) {
                let resolver = FnResolver(outer);
                let values = args
                    .iter()
                    .map(|a| eval::eval(a, outer_row, &resolver, eval_ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                ExprKind::Literal(run_user_scalar_function(storage, &def, &values, eval_ctx)?)
            } else {
                ExprKind::Function {
                    name: name.clone(),
                    args,
                }
            }
        }
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => ExprKind::Aggregate {
            func: *func,
            distinct: *distinct,
            arg: match arg {
                Some(a) => Some(recur_box(a)?),
                None => None,
            },
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: match operand {
                Some(o) => Some(recur_box(o)?),
                None => None,
            },
            branches: branches
                .iter()
                .map(|(w, r)| Ok((recur(w)?, recur(r)?)))
                .collect::<Result<_, SqlError>>()?,
            else_result: match else_result {
                Some(e) => Some(recur_box(e)?),
                None => None,
            },
        },
    };
    Ok(Expr {
        kind,
        span: expr.span,
    })
}
