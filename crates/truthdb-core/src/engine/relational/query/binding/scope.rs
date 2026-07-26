use super::*;

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
