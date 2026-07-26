use super::*;

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
