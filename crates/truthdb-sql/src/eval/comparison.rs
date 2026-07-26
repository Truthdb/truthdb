use crate::ast::{BinaryOp, Expr, ExprKind};
use crate::collation::CollationSensitivity;
use crate::decimal::Decimal;
use crate::error::{SqlError, SqlResult};
use crate::value::{self, SqlValue};

use super::arithmetic::{arithmetic, three_valued};
use super::context::ColumnResolver;

// The literal-parsing and arithmetic helpers are kept out of line so the
// deep-recursing `eval` frame stays small: a long operator chain recurses ~1
// `eval` frame per operator, and folding these (with their format!/parse
// temporaries) into `eval` would blow a 2 MiB stack (see the node-budget
// rationale in the parser).

/// A literal with a decimal point is DECIMAL/NUMERIC; one with an exponent is
/// FLOAT (SQL Server literal typing).
#[inline(never)]
pub(super) fn eval_number_literal(text: &str) -> SqlResult<SqlValue> {
    if text.contains(['e', 'E']) {
        text.parse::<f64>()
            .map(SqlValue::Float)
            .map_err(|_| SqlError::conversion(format!("cannot parse float literal '{text}'")))
    } else {
        Decimal::parse(text)
            .map(|d| SqlValue::Decimal(Box::new(d)))
            .ok_or_else(|| SqlError::conversion(format!("cannot parse numeric literal '{text}'")))
    }
}

#[inline(never)]
pub(super) fn eval_binary(
    op: BinaryOp,
    l: SqlValue,
    r: SqlValue,
    sensitivity: CollationSensitivity,
) -> SqlResult<SqlValue> {
    use BinaryOp::*;
    match op {
        And => Ok(three_valued(value::and(l.as_predicate(), r.as_predicate()))),
        Or => Ok(three_valued(value::or(l.as_predicate(), r.as_predicate()))),
        Eq | Ne | Lt | Le | Gt | Ge => {
            let ordering = l.compare_collated(&r, sensitivity)?;
            Ok(three_valued(ordering.map(|ord| compare_matches(op, ord))))
        }
        Add | Sub | Mul | Div | Mod => arithmetic(op, l, r),
    }
}

/// The case sensitivity governing a comparison of `left` and `right`. A column
/// operand contributes its column collation (implicit precedence); a literal or
/// computed expression contributes nothing (coercible-default, which yields). If
/// any participating column is case-sensitive the comparison is case-sensitive;
/// otherwise it is case-insensitive (the database default). Ignored for
/// non-string operands.
pub(super) fn comparison_sensitivity(
    left: &Expr,
    right: &Expr,
    resolver: &impl ColumnResolver,
) -> CollationSensitivity {
    match (
        operand_sensitivity(left, resolver),
        operand_sensitivity(right, resolver),
    ) {
        (Some(a), Some(b)) => a.combine(b),
        (Some(a), None) | (None, Some(a)) => a,
        (None, None) => CollationSensitivity::default_collation(),
    }
}

/// The implicit collation of `expr` if it is a resolvable column reference, else
/// `None` (a literal or computed expression is coercible-default and yields to a
/// column's collation).
pub(super) fn operand_sensitivity(
    expr: &Expr,
    resolver: &impl ColumnResolver,
) -> Option<CollationSensitivity> {
    if let ExprKind::Column(name) = &expr.kind {
        return resolver.resolve(&name.value).map(|i| resolver.collation(i));
    }
    None
}

/// The collation governing `expr` when it is used as a grouping / join / DISTINCT
/// key column: its column collation if it is a column reference, else the
/// database default (case-insensitive). Used by the hash operators to fold key
/// strings so case-insensitive-equal keys share a bucket.
pub fn key_collation(expr: &Expr, resolver: &impl ColumnResolver) -> CollationSensitivity {
    operand_sensitivity(expr, resolver).unwrap_or(CollationSensitivity::default_collation())
}

fn compare_matches(op: BinaryOp, ord: std::cmp::Ordering) -> bool {
    use std::cmp::Ordering::*;
    match op {
        BinaryOp::Eq => ord == Equal,
        BinaryOp::Ne => ord != Equal,
        BinaryOp::Lt => ord == Less,
        BinaryOp::Le => ord != Greater,
        BinaryOp::Gt => ord == Greater,
        BinaryOp::Ge => ord != Less,
        _ => unreachable!("not a comparison op"),
    }
}
