use super::super::prelude::*;

// ---- common table expressions -------------------------------------------

/// Inlines a SELECT's `WITH` common table expressions: each FROM reference to a
/// CTE name becomes a derived table over the CTE's query. CTEs are expanded in
/// order, so a later CTE may reference an earlier one; non-recursive (a self- or
/// forward-reference is left as a base-table name and errors at bind). Returns a
/// CTE-free SELECT.
pub(in crate::engine::relational) type CteMap = std::collections::HashMap<String, Select>;

pub(in crate::engine::relational) fn expand_ctes(select: &Select) -> Select {
    expand_select_ctes(select, &CteMap::new())
}

/// A copy of `select` with every CTE reference — at this level and nested inside
/// its subqueries — replaced by a derived table. `outer` is the enclosing CTE
/// scope; this select's own `WITH` layers on top of it (so a nested `WITH` sees
/// enclosing CTEs and is itself inlined). The result carries no `ctes` at any
/// level, so lock analysis, which walks the expanded tree without re-expanding,
/// still sees every base table the executor reads.
pub(in crate::engine::relational) fn expand_select_ctes(select: &Select, outer: &CteMap) -> Select {
    let mut resolved = outer.clone();
    for cte in &select.ctes {
        let body = expand_select_ctes(&cte.query, &resolved);
        resolved.insert(cte.name.value.to_ascii_lowercase(), body);
    }
    let resolved = &resolved;
    let mut out = select.clone();
    out.ctes = Vec::new();
    out.from = out
        .from
        .as_ref()
        .map(|from| expand_from_ctes(from, resolved));
    out.items = out
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, alias } => SelectItem::Expr {
                expr: expand_expr_ctes(expr, resolved),
                alias: alias.clone(),
            },
            // Inline CTE references inside an assignment value too, so lock
            // analysis (which expands the original assignment SELECT) sees the
            // real base tables behind a CTE used only in the value expression.
            SelectItem::Assign { target, value } => SelectItem::Assign {
                target: target.clone(),
                value: expand_expr_ctes(value, resolved),
            },
            other => other.clone(),
        })
        .collect();
    out.where_clause = out
        .where_clause
        .as_ref()
        .map(|e| expand_expr_ctes(e, resolved));
    out.having = out.having.as_ref().map(|e| expand_expr_ctes(e, resolved));
    out.group_by = out
        .group_by
        .iter()
        .map(|e| expand_expr_ctes(e, resolved))
        .collect();
    out.order_by = out
        .order_by
        .iter()
        .map(|o| OrderItem {
            expr: expand_expr_ctes(&o.expr, resolved),
            descending: o.descending,
        })
        .collect();
    out
}

/// Replaces CTE references in a FROM tree with derived tables (recursing into
/// joins — including the `ON` predicate's subqueries — and nested derived
/// tables, which may also reference the CTEs).
pub(in crate::engine::relational) fn expand_from_ctes(
    tref: &TableRef,
    resolved: &CteMap,
) -> TableRef {
    match tref {
        TableRef::Table { name, alias } => {
            // Only an unqualified reference can name a CTE (CTE names are not
            // schema-qualified); `dbo.s` must resolve to a base table.
            let cte = (!name.value.contains('.'))
                .then(|| resolved.get(&name.value.to_ascii_lowercase()))
                .flatten();
            match cte {
                Some(body) => TableRef::Derived {
                    subquery: Box::new(body.clone()),
                    // The exposed name is the alias, else the CTE reference name.
                    alias: alias.clone().unwrap_or_else(|| name.clone()),
                },
                None => tref.clone(),
            }
        }
        TableRef::Join {
            left,
            right,
            kind,
            on,
        } => TableRef::Join {
            left: Box::new(expand_from_ctes(left, resolved)),
            right: Box::new(expand_from_ctes(right, resolved)),
            kind: *kind,
            on: on.as_ref().map(|e| expand_expr_ctes(e, resolved)),
        },
        TableRef::Derived { subquery, alias } => TableRef::Derived {
            subquery: Box::new(expand_select_ctes(subquery, resolved)),
            alias: alias.clone(),
        },
        // A TVF name is never a CTE (CTE names are unqualified); only its
        // arguments may reference one.
        TableRef::Function { name, args, alias } => TableRef::Function {
            name: name.clone(),
            args: args.iter().map(|a| expand_expr_ctes(a, resolved)).collect(),
            alias: alias.clone(),
        },
    }
}

/// Replaces CTE references inside a subquery embedded in an expression (so a CTE
/// is visible to WHERE/SELECT/HAVING subqueries, not only the FROM clause).
pub(in crate::engine::relational) fn expand_expr_ctes(expr: &Expr, resolved: &CteMap) -> Expr {
    let recur = |e: &Expr| Box::new(expand_expr_ctes(e, resolved));
    let recur_opt = |e: &Option<Box<Expr>>| e.as_ref().map(|e| recur(e));
    let kind = match &expr.kind {
        ExprKind::Subquery(s) => ExprKind::Subquery(Box::new(expand_select_ctes(s, resolved))),
        ExprKind::Exists(s) => ExprKind::Exists(Box::new(expand_select_ctes(s, resolved))),
        ExprKind::InSubquery {
            expr: e,
            subquery,
            negated,
        } => ExprKind::InSubquery {
            expr: recur(e),
            subquery: Box::new(expand_select_ctes(subquery, resolved)),
            negated: *negated,
        },
        ExprKind::Unary { op, expr: e } => ExprKind::Unary {
            op: *op,
            expr: recur(e),
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: recur(left),
            right: recur(right),
        },
        ExprKind::IsNull { expr: e, negated } => ExprKind::IsNull {
            expr: recur(e),
            negated: *negated,
        },
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: recur(e),
            pattern: recur(pattern),
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: e,
            list,
            negated,
        } => ExprKind::InList {
            expr: recur(e),
            list: list.iter().map(|x| expand_expr_ctes(x, resolved)).collect(),
            negated: *negated,
        },
        ExprKind::Between {
            expr: e,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: recur(e),
            low: recur(low),
            high: recur(high),
            negated: *negated,
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: recur_opt(operand),
            branches: branches
                .iter()
                .map(|(w, r)| (expand_expr_ctes(w, resolved), expand_expr_ctes(r, resolved)))
                .collect(),
            else_result: recur_opt(else_result),
        },
        ExprKind::Cast { expr: e, target } => ExprKind::Cast {
            expr: recur(e),
            target: target.clone(),
        },
        ExprKind::Function { name, args } => ExprKind::Function {
            name: name.clone(),
            args: args.iter().map(|a| expand_expr_ctes(a, resolved)).collect(),
        },
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => ExprKind::Aggregate {
            func: *func,
            distinct: *distinct,
            arg: recur_opt(arg),
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
    Expr {
        kind,
        span: expr.span,
    }
}
