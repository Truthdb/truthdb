use std::cmp::Ordering;

use crate::ast::{Expr, ExprKind, Name, UnaryOp};
use crate::collation::CollationSensitivity;
use crate::decimal::Decimal;
use crate::error::{SqlError, SqlResult};
use crate::value::{self, SqlValue};

use super::arithmetic::three_valued;
use super::casts::cast_value;
use super::comparison::{
    comparison_sensitivity, eval_binary, eval_number_literal, operand_sensitivity,
};
use super::context::{ColumnResolver, EvalContext, Resolution};
use super::functions::eval_call;

/// Maximum expression-evaluation recursion depth. A long operator chain
/// (`1 OR 1 OR ...`) recurses ~1 frame per operator, so — like the parser's
/// node budget — eval must bound its own depth to fail cleanly (error 191)
/// instead of overflowing the stack. Generous for real queries; the frame is
/// kept small (heavy arms delegate to out-of-line helpers) so this is safe.
const MAX_EVAL_DEPTH: usize = 500;
/// Evaluates `expr` against `row`, resolving columns via `resolver`.
pub fn eval(
    expr: &Expr,
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
    ctx: &EvalContext,
) -> SqlResult<SqlValue> {
    eval_at(expr, row, resolver, ctx, 0)
}

pub(super) fn eval_at(
    expr: &Expr,
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
    ctx: &EvalContext,
    depth: usize,
) -> SqlResult<SqlValue> {
    if depth > MAX_EVAL_DEPTH {
        return Err(SqlError::message_only(
            191,
            "Some part of your SQL statement is nested too deeply. Rewrite the query or break it into smaller queries.",
        ));
    }
    match &expr.kind {
        ExprKind::Null => Ok(SqlValue::Null),
        ExprKind::Int(v) => Ok(SqlValue::Int(*v)),
        ExprKind::Number(text) => eval_number_literal(text),
        ExprKind::Str(s) => Ok(SqlValue::Str(s.clone())),
        ExprKind::Bool(b) => Ok(SqlValue::Bool(*b)),
        // A precomputed value (a rewritten subquery).
        ExprKind::Literal(value) => Ok(value.clone()),
        // Subqueries must be rewritten to literals by the executor before
        // evaluation; reaching here means one appeared in an unsupported
        // context (e.g. a join ON clause).
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => Err(
            SqlError::message_only(1015, "A subquery is not supported in this context."),
        ),
        ExprKind::Column(name) => eval_column(name, row, resolver),
        ExprKind::GlobalVar(name) => eval_global_var(name, ctx),
        ExprKind::LocalVar(name) => ctx.variables.get(name).cloned().ok_or_else(|| {
            SqlError::message_only(
                137,
                format!("Must declare the scalar variable \"@{name}\"."),
            )
        }),
        ExprKind::Unary { op, expr: inner } => {
            let value = eval_at(inner, row, resolver, ctx, depth + 1)?;
            eval_unary(*op, value)
        }
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => {
            let value = eval_at(inner, row, resolver, ctx, depth + 1)?;
            Ok(SqlValue::Bool(value.is_null() != *negated))
        }
        ExprKind::Binary { op, left, right } => {
            let l = eval_at(left, row, resolver, ctx, depth + 1)?;
            let r = eval_at(right, row, resolver, ctx, depth + 1)?;
            let sensitivity = comparison_sensitivity(left, right, resolver);
            eval_binary(*op, l, r, sensitivity)
        }
        ExprKind::Like {
            expr,
            pattern,
            escape,
            negated,
        } => eval_like_expr(expr, pattern, *escape, *negated, row, resolver, ctx, depth),
        ExprKind::InList {
            expr,
            list,
            negated,
        } => eval_in_expr(expr, list, *negated, row, resolver, ctx, depth),
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => eval_between_expr(expr, low, high, *negated, row, resolver, ctx, depth),
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => eval_case_expr(
            operand.as_deref(),
            branches,
            else_result.as_deref(),
            row,
            resolver,
            ctx,
            depth,
        ),
        ExprKind::Cast { expr, target } => {
            let v = eval_at(expr, row, resolver, ctx, depth + 1)?;
            cast_value(v, target)
        }
        ExprKind::Function { name, args } => eval_call(name, args, row, resolver, ctx, depth),
        // Aggregates are resolved by the grouping executor, never per row. One
        // reaching scalar eval means it appeared where aggregates are not
        // allowed (e.g. WHERE) — SQL Server error 147.
        ExprKind::Aggregate { .. } => Err(SqlError::message_only(
            147,
            "An aggregate may not appear in the WHERE clause or a non-grouped context.",
        )),
    }
}

#[inline(never)]
fn eval_column(
    name: &Name,
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
) -> SqlResult<SqlValue> {
    match resolver.resolve_detail(&name.value) {
        Resolution::Found(index) => Ok(row[index].clone()),
        Resolution::Ambiguous => Err(SqlError::ambiguous_column(&name.value).at(name.span)),
        Resolution::NotFound => Err(SqlError::invalid_column(&name.value).at(name.span)),
    }
}

#[inline(never)]
fn eval_global_var(name: &str, ctx: &EvalContext) -> SqlResult<SqlValue> {
    match name {
        "trancount" => Ok(SqlValue::Int(ctx.trancount as i64)),
        "spid" => Ok(SqlValue::Int(ctx.spid as i64)),
        // Lead with TruthDB's own identity, then a SQL-Server-shaped version
        // token so tooling that scrapes @@VERSION for a version number keeps
        // working.
        "version" => Ok(SqlValue::Str(
            "TruthDB - 16.0.1000.6\n\tMicrosoft SQL Server 2022 compatible edition".to_string(),
        )),
        "rowcount" => Ok(SqlValue::Int(ctx.rowcount)),
        "fetch_status" => Ok(SqlValue::Int(ctx.fetch_status as i64)),
        "error" => Ok(SqlValue::Int(ctx.last_error as i64)),
        "nestlevel" => Ok(SqlValue::Int(ctx.nestlevel as i64)),
        "identity" => Ok(SqlValue::Int(0)),
        other => Err(SqlError::message_only(
            102,
            format!("Incorrect syntax near '@@{other}'."),
        )),
    }
}

#[inline(never)]
fn eval_unary(op: UnaryOp, value: SqlValue) -> SqlResult<SqlValue> {
    match op {
        UnaryOp::Neg => match value {
            SqlValue::Null => Ok(SqlValue::Null),
            SqlValue::Int(v) => Ok(SqlValue::Int(v.wrapping_neg())),
            SqlValue::Float(v) => Ok(SqlValue::Float(-v)),
            SqlValue::Decimal(d) => Ok(SqlValue::Decimal(Box::new(Decimal::new(
                -d.value,
                d.precision,
                d.scale,
            )))),
            other => Err(SqlError::conversion(format!(
                "operator '-' is not valid on {}",
                other.type_name()
            ))),
        },
        UnaryOp::Not => Ok(three_valued(value::not(value.as_predicate()))),
    }
}

// Compound-expression handlers are kept out of line so `eval_at`'s frame — the
// one that recurses down a long operator chain — stays small.

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn eval_like_expr<R: ColumnResolver>(
    expr: &Expr,
    pattern: &Expr,
    escape: Option<char>,
    negated: bool,
    row: &[SqlValue],
    resolver: &R,
    ctx: &EvalContext,
    depth: usize,
) -> SqlResult<SqlValue> {
    let value = eval_at(expr, row, resolver, ctx, depth + 1)?;
    let pat = eval_at(pattern, row, resolver, ctx, depth + 1)?;
    if value.is_null() || pat.is_null() {
        return Ok(SqlValue::Null);
    }
    let (SqlValue::Str(text), SqlValue::Str(pattern)) = (&value, &pat) else {
        return Err(SqlError::conversion(
            "LIKE requires character-string operands".to_string(),
        ));
    };
    // Under a case-insensitive collation, fold both sides before matching (LIKE
    // wildcards are ASCII and survive lowercasing); the LHS column's collation
    // governs. `_CS`/`_BIN` columns match exactly.
    let sensitivity =
        operand_sensitivity(expr, resolver).unwrap_or(CollationSensitivity::default_collation());
    let matched =
        crate::like::like_match(&sensitivity.fold(text), &sensitivity.fold(pattern), escape);
    Ok(SqlValue::Bool(matched != negated))
}

#[inline(never)]
fn eval_in_expr<R: ColumnResolver>(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    row: &[SqlValue],
    resolver: &R,
    ctx: &EvalContext,
    depth: usize,
) -> SqlResult<SqlValue> {
    // An empty list is definite regardless of the operand: `x IN ()` is FALSE
    // and `x NOT IN ()` is TRUE, even when `x` is NULL (the comparison set is
    // empty, so there is nothing unknown). Only reachable via an IN-subquery
    // that returned no rows — a written value list always has at least one item.
    if list.is_empty() {
        return Ok(SqlValue::Bool(negated));
    }
    let value = eval_at(expr, row, resolver, ctx, depth + 1)?;
    if value.is_null() {
        return Ok(SqlValue::Null);
    }
    // The operand's column collation governs the string equality; a list of
    // literals contributes nothing.
    let sensitivity =
        operand_sensitivity(expr, resolver).unwrap_or(CollationSensitivity::default_collation());
    // `x IN (list)` is `x=a OR x=b OR ...` under three-valued logic.
    let mut any_unknown = false;
    for item in list {
        let candidate = eval_at(item, row, resolver, ctx, depth + 1)?;
        match value.compare_collated(&candidate, sensitivity)? {
            Some(std::cmp::Ordering::Equal) => return Ok(SqlValue::Bool(!negated)),
            None => any_unknown = true,
            _ => {}
        }
    }
    if any_unknown {
        Ok(SqlValue::Null)
    } else {
        Ok(SqlValue::Bool(negated))
    }
}

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn eval_between_expr<R: ColumnResolver>(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    row: &[SqlValue],
    resolver: &R,
    ctx: &EvalContext,
    depth: usize,
) -> SqlResult<SqlValue> {
    let value = eval_at(expr, row, resolver, ctx, depth + 1)?;
    let lo = eval_at(low, row, resolver, ctx, depth + 1)?;
    let hi = eval_at(high, row, resolver, ctx, depth + 1)?;
    // `x BETWEEN a AND b` is `x>=a AND x<=b` (three-valued); the operand's column
    // collation governs the string comparison.
    let sensitivity =
        operand_sensitivity(expr, resolver).unwrap_or(CollationSensitivity::default_collation());
    let ge = value
        .compare_collated(&lo, sensitivity)?
        .map(|o| o != Ordering::Less);
    let le = value
        .compare_collated(&hi, sensitivity)?
        .map(|o| o != Ordering::Greater);
    let within = value::and(ge, le);
    Ok(three_valued(if negated {
        value::not(within)
    } else {
        within
    }))
}

#[inline(never)]
fn eval_case_expr<R: ColumnResolver>(
    operand: Option<&Expr>,
    branches: &[(Expr, Expr)],
    else_result: Option<&Expr>,
    row: &[SqlValue],
    resolver: &R,
    ctx: &EvalContext,
    depth: usize,
) -> SqlResult<SqlValue> {
    let operand_value = match operand {
        Some(o) => Some(eval_at(o, row, resolver, ctx, depth + 1)?),
        None => None,
    };
    for (cond, result) in branches {
        let matched = match &operand_value {
            // Simple CASE: operand = WHEN value (NULL never matches).
            Some(ov) => {
                let cv = eval_at(cond, row, resolver, ctx, depth + 1)?;
                matches!(ov.compare(&cv)?, Some(Ordering::Equal))
            }
            // Searched CASE: WHEN is a boolean predicate.
            None => matches!(
                eval_at(cond, row, resolver, ctx, depth + 1)?,
                SqlValue::Bool(true)
            ),
        };
        if matched {
            return eval_at(result, row, resolver, ctx, depth + 1);
        }
    }
    match else_result {
        Some(e) => eval_at(e, row, resolver, ctx, depth + 1),
        None => Ok(SqlValue::Null),
    }
}
