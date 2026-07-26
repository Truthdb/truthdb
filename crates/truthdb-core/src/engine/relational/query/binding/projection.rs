use super::*;

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
