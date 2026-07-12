//! Expression evaluation over a bound row of [`SqlValue`]s.
//!
//! Column references are resolved to indices before evaluation (by the
//! binder in the storage crate), so `eval` takes the row as a slice and a
//! resolver mapping a column [`Name`] to its index. Arithmetic and
//! comparisons follow three-valued logic (see [`value`](crate::value)).

use crate::ast::{BinaryOp, Expr, ExprKind, UnaryOp};
use crate::decimal::Decimal;
use crate::error::{SqlError, SqlResult};
use crate::value::{self, Numeric, SqlValue};

/// Resolves a column name to its position in the row, case-insensitively.
pub trait ColumnResolver {
    fn resolve(&self, name: &str) -> Option<usize>;
}

impl ColumnResolver for [String] {
    fn resolve(&self, name: &str) -> Option<usize> {
        self.iter().position(|c| c.eq_ignore_ascii_case(name))
    }
}

impl ColumnResolver for Vec<String> {
    fn resolve(&self, name: &str) -> Option<usize> {
        self.as_slice().resolve(name)
    }
}

/// Maximum expression-evaluation recursion depth. A long operator chain
/// (`1 OR 1 OR ...`) recurses ~1 frame per operator, so — like the parser's
/// node budget — eval must bound its own depth to fail cleanly (error 191)
/// instead of overflowing the stack. Generous for real queries; the frame is
/// kept small (heavy arms delegate to out-of-line helpers) so this is safe.
const MAX_EVAL_DEPTH: usize = 500;

/// Evaluates `expr` against `row`, resolving columns via `resolver`.
pub fn eval(expr: &Expr, row: &[SqlValue], resolver: &impl ColumnResolver) -> SqlResult<SqlValue> {
    eval_at(expr, row, resolver, 0)
}

fn eval_at(
    expr: &Expr,
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
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
        ExprKind::Column(name) => eval_column(name, row, resolver),
        ExprKind::Unary { op, expr: inner } => {
            let value = eval_at(inner, row, resolver, depth + 1)?;
            eval_unary(*op, value)
        }
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => {
            let value = eval_at(inner, row, resolver, depth + 1)?;
            Ok(SqlValue::Bool(value.is_null() != *negated))
        }
        ExprKind::Binary { op, left, right } => {
            let l = eval_at(left, row, resolver, depth + 1)?;
            let r = eval_at(right, row, resolver, depth + 1)?;
            eval_binary(*op, l, r)
        }
    }
}

#[inline(never)]
fn eval_column(
    name: &crate::ast::Name,
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
) -> SqlResult<SqlValue> {
    let index = resolver
        .resolve(&name.value)
        .ok_or_else(|| SqlError::invalid_column(&name.value).at(name.span))?;
    Ok(row[index].clone())
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

// The literal-parsing and arithmetic helpers are kept out of line so the
// deep-recursing `eval` frame stays small: a long operator chain recurses ~1
// `eval` frame per operator, and folding these (with their format!/parse
// temporaries) into `eval` would blow a 2 MiB stack (see the node-budget
// rationale in the parser).

/// A literal with a decimal point is DECIMAL/NUMERIC; one with an exponent is
/// FLOAT (SQL Server literal typing).
#[inline(never)]
fn eval_number_literal(text: &str) -> SqlResult<SqlValue> {
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
fn eval_binary(op: BinaryOp, l: SqlValue, r: SqlValue) -> SqlResult<SqlValue> {
    use BinaryOp::*;
    match op {
        And => Ok(three_valued(value::and(l.as_predicate(), r.as_predicate()))),
        Or => Ok(three_valued(value::or(l.as_predicate(), r.as_predicate()))),
        Eq | Ne | Lt | Le | Gt | Ge => {
            let ordering = l.compare(&r)?;
            Ok(three_valued(ordering.map(|ord| compare_matches(op, ord))))
        }
        Add | Sub | Mul | Div | Mod => arithmetic(op, l, r),
    }
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

fn arithmetic(op: BinaryOp, l: SqlValue, r: SqlValue) -> SqlResult<SqlValue> {
    if l.is_null() || r.is_null() {
        return Ok(SqlValue::Null);
    }
    // `+` over two character operands is concatenation, not addition.
    if op == BinaryOp::Add {
        if let (SqlValue::Str(a), SqlValue::Str(b)) = (&l, &r) {
            return Ok(SqlValue::Str(format!("{a}{b}")));
        }
    }
    let a = coerce_numeric(&l)?;
    let b = coerce_numeric(&r)?;
    numeric_arithmetic(op, a, b)
}

/// A value as a number for arithmetic; a character operand is parsed (int, then
/// decimal), matching SQL Server's implicit conversion.
fn coerce_numeric(value: &SqlValue) -> SqlResult<Numeric> {
    if let Some(n) = value.as_numeric() {
        return Ok(n);
    }
    if let SqlValue::Str(s) = value {
        if let Ok(v) = s.trim().parse::<i64>() {
            return Ok(Numeric::Int(v));
        }
        if let Some(d) = Decimal::parse(s) {
            return Ok(Numeric::Decimal(d));
        }
    }
    Err(SqlError::conversion(format!(
        "operator is not valid on operand of type {}",
        value.type_name()
    )))
}

/// Promotes two numerics (float > decimal > int) and applies the operator.
fn numeric_arithmetic(op: BinaryOp, a: Numeric, b: Numeric) -> SqlResult<SqlValue> {
    use Numeric::*;
    match (a, b) {
        (Float(_), _) | (_, Float(_)) => float_arithmetic(op, num_to_f64(a), num_to_f64(b)),
        (Decimal(x), Decimal(y)) => decimal_arithmetic(op, x, y),
        (Decimal(x), Int(y)) => decimal_arithmetic(op, x, crate::decimal::Decimal::from_i64(y)),
        (Int(x), Decimal(y)) => decimal_arithmetic(op, crate::decimal::Decimal::from_i64(x), y),
        (Int(x), Int(y)) => int_arithmetic(op, x, y),
    }
}

fn num_to_f64(n: Numeric) -> f64 {
    match n {
        Numeric::Int(v) => v as f64,
        Numeric::Float(v) => v,
        Numeric::Decimal(d) => d.to_f64(),
    }
}

fn float_arithmetic(op: BinaryOp, a: f64, b: f64) -> SqlResult<SqlValue> {
    let value = match op {
        BinaryOp::Add => a + b,
        BinaryOp::Sub => a - b,
        BinaryOp::Mul => a * b,
        BinaryOp::Div => {
            if b == 0.0 {
                return Err(SqlError::divide_by_zero());
            }
            a / b
        }
        BinaryOp::Mod => {
            if b == 0.0 {
                return Err(SqlError::divide_by_zero());
            }
            a % b
        }
        _ => unreachable!(),
    };
    Ok(SqlValue::Float(value))
}

fn decimal_arithmetic(op: BinaryOp, a: Decimal, b: Decimal) -> SqlResult<SqlValue> {
    let overflow = || SqlError::new(8115, 16, 2, "Arithmetic overflow error.");
    let result = match op {
        BinaryOp::Add => a.add(b).map_err(|_| overflow())?,
        BinaryOp::Sub => a.sub(b).map_err(|_| overflow())?,
        BinaryOp::Mul => a.mul(b).map_err(|_| overflow())?,
        BinaryOp::Div => match a.div(b).map_err(|_| overflow())? {
            Some(d) => d,
            None => return Err(SqlError::divide_by_zero()),
        },
        BinaryOp::Mod => {
            if b.is_zero() {
                return Err(SqlError::divide_by_zero());
            }
            let scale = a.scale.max(b.scale);
            let (Some(x), Some(y)) = (a.rescaled(scale), b.rescaled(scale)) else {
                return Err(overflow());
            };
            Decimal::new(x % y, a.precision.max(b.precision), scale)
        }
        _ => unreachable!(),
    };
    Ok(SqlValue::Decimal(Box::new(result)))
}

fn int_arithmetic(op: BinaryOp, a: i64, b: i64) -> SqlResult<SqlValue> {
    let checked = match op {
        BinaryOp::Add => a.checked_add(b),
        BinaryOp::Sub => a.checked_sub(b),
        BinaryOp::Mul => a.checked_mul(b),
        BinaryOp::Div => {
            if b == 0 {
                return Err(SqlError::divide_by_zero());
            }
            a.checked_div(b)
        }
        BinaryOp::Mod => {
            if b == 0 {
                return Err(SqlError::divide_by_zero());
            }
            a.checked_rem(b)
        }
        _ => unreachable!(),
    };
    checked
        .map(SqlValue::Int)
        .ok_or_else(|| SqlError::new(8115, 16, 2, "Arithmetic overflow error."))
}

/// Wraps a three-valued result as a SQL boolean value (UNKNOWN -> NULL).
fn three_valued(v: value::ThreeValued) -> SqlValue {
    match v {
        Some(b) => SqlValue::Bool(b),
        None => SqlValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    fn eval_predicate(sql: &str, columns: &[&str], row: &[SqlValue]) -> SqlValue {
        // Parse `SELECT <expr>` and evaluate the single item.
        let statements = Parser::parse_str(&format!("SELECT {sql}")).expect("parse");
        let select = match &statements[0] {
            crate::ast::Statement::Select(s) => s,
            _ => panic!("expected select"),
        };
        let expr = match &select.items[0] {
            crate::ast::SelectItem::Expr { expr, .. } => expr,
            _ => panic!("expected expr"),
        };
        let names: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        eval(expr, row, &names).expect("eval")
    }

    #[test]
    fn arithmetic_and_precedence() {
        assert_eq!(eval_predicate("1 + 2 * 3", &[], &[]), SqlValue::Int(7));
        assert_eq!(eval_predicate("(1 + 2) * 3", &[], &[]), SqlValue::Int(9));
        assert_eq!(eval_predicate("7 / 2", &[], &[]), SqlValue::Int(3));
        assert_eq!(eval_predicate("7 % 3", &[], &[]), SqlValue::Int(1));
        assert_eq!(eval_predicate("-5 + 2", &[], &[]), SqlValue::Int(-3));
    }

    #[test]
    fn null_arithmetic_is_null() {
        assert_eq!(eval_predicate("1 + NULL", &[], &[]), SqlValue::Null);
    }

    #[test]
    fn divide_by_zero_errors() {
        let statements = Parser::parse_str("SELECT 1 / 0").unwrap();
        let crate::ast::Statement::Select(select) = &statements[0] else {
            panic!()
        };
        let crate::ast::SelectItem::Expr { expr, .. } = &select.items[0] else {
            panic!()
        };
        let empty: Vec<String> = Vec::new();
        assert_eq!(eval(expr, &[], &empty).unwrap_err().number, 8134);
    }

    #[test]
    fn three_valued_comparisons() {
        assert_eq!(eval_predicate("1 = 1", &[], &[]), SqlValue::Bool(true));
        assert_eq!(eval_predicate("1 = 2", &[], &[]), SqlValue::Bool(false));
        assert_eq!(eval_predicate("1 = NULL", &[], &[]), SqlValue::Null);
        assert_eq!(eval_predicate("NULL <> 1", &[], &[]), SqlValue::Null);
    }

    #[test]
    fn is_null_is_two_valued() {
        assert_eq!(
            eval_predicate("x IS NULL", &["x"], &[SqlValue::Null]),
            SqlValue::Bool(true)
        );
        assert_eq!(
            eval_predicate("x IS NOT NULL", &["x"], &[SqlValue::Null]),
            SqlValue::Bool(false)
        );
        assert_eq!(
            eval_predicate("x IS NULL", &["x"], &[SqlValue::Int(5)]),
            SqlValue::Bool(false)
        );
    }

    #[test]
    fn boolean_connectives_over_null() {
        // NULL AND FALSE = FALSE; NULL AND TRUE = NULL; NULL OR TRUE = TRUE.
        assert_eq!(
            eval_predicate("x = 1 AND 1 = 2", &["x"], &[SqlValue::Null]),
            SqlValue::Bool(false)
        );
        assert_eq!(
            eval_predicate("x = 1 AND 1 = 1", &["x"], &[SqlValue::Null]),
            SqlValue::Null
        );
        assert_eq!(
            eval_predicate("x = 1 OR 1 = 1", &["x"], &[SqlValue::Null]),
            SqlValue::Bool(true)
        );
        assert_eq!(
            eval_predicate("NOT (x = 1)", &["x"], &[SqlValue::Null]),
            SqlValue::Null
        );
    }

    #[test]
    fn large_valid_chain_evaluates_without_overflow() {
        // A left-leaning OR chain within the eval depth budget evaluates,
        // recursing down its spine without overflowing the stack.
        let sql = format!("1{}", " OR 1".repeat(400));
        assert_eq!(eval_predicate(&sql, &[], &[]), SqlValue::Bool(true));
    }

    #[test]
    fn over_deep_chain_errors_not_overflow() {
        // Past the depth budget eval fails cleanly (191), never overflowing.
        let sql = format!("1{}", " OR 1".repeat(700));
        let statements = Parser::parse_str(&format!("SELECT {sql}")).unwrap();
        let crate::ast::Statement::Select(select) = &statements[0] else {
            panic!()
        };
        let crate::ast::SelectItem::Expr { expr, .. } = &select.items[0] else {
            panic!()
        };
        let empty: Vec<String> = Vec::new();
        assert_eq!(eval(expr, &[], &empty).unwrap_err().number, 191);
    }

    #[test]
    fn column_reference_resolution() {
        assert_eq!(
            eval_predicate("price * 2", &["price"], &[SqlValue::Int(50)]),
            SqlValue::Int(100)
        );
        // Case-insensitive.
        assert_eq!(
            eval_predicate("PRICE + 1", &["price"], &[SqlValue::Int(9)]),
            SqlValue::Int(10)
        );
    }
}
