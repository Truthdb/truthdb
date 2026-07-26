use super::*;

/// Evaluates each correlated subquery in `expr` against `outer_row` (binding the
/// enclosing query's columns per `outer`) and replaces it with a literal —
/// producing a subquery-free predicate for that outer row.
/// A [`ColumnResolver`] over a bare name→index closure (the `outer` resolver the
/// correlated-substitution pass carries), so a user scalar function's arguments
/// can be evaluated against the current row.
pub(in crate::engine::relational) struct FnResolver<'a>(
    pub(in crate::engine::relational::query) &'a dyn Fn(&str) -> Option<usize>,
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
