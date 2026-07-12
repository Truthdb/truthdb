//! Expression evaluation over a bound row of [`SqlValue`]s.
//!
//! Column references are resolved to indices before evaluation (by the
//! binder in the storage crate), so `eval` takes the row as a slice and a
//! resolver mapping a column [`Name`] to its index. Arithmetic and
//! comparisons follow three-valued logic (see [`value`](crate::value)).

use crate::ast::{BinaryOp, Expr, ExprKind, UnaryOp};
use crate::error::{SqlError, SqlResult};
use crate::value::{self, SqlValue};

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

/// Evaluates `expr` against `row`, resolving columns via `resolver`.
pub fn eval(expr: &Expr, row: &[SqlValue], resolver: &impl ColumnResolver) -> SqlResult<SqlValue> {
    match &expr.kind {
        ExprKind::Null => Ok(SqlValue::Null),
        ExprKind::Int(v) => Ok(SqlValue::Int(*v)),
        ExprKind::Number(text) => text
            .parse::<f64>()
            .map(SqlValue::Float)
            .map_err(|_| SqlError::conversion(format!("cannot parse numeric literal '{text}'"))),
        ExprKind::Str(s) => Ok(SqlValue::Str(s.clone())),
        ExprKind::Bool(b) => Ok(SqlValue::Bool(*b)),
        ExprKind::Column(name) => {
            let index = resolver
                .resolve(&name.value)
                .ok_or_else(|| SqlError::invalid_column(&name.value).at(name.span))?;
            Ok(row[index].clone())
        }
        ExprKind::Unary { op, expr: inner } => {
            let value = eval(inner, row, resolver)?;
            match op {
                UnaryOp::Neg => match value {
                    SqlValue::Null => Ok(SqlValue::Null),
                    SqlValue::Int(v) => Ok(SqlValue::Int(v.wrapping_neg())),
                    SqlValue::Float(v) => Ok(SqlValue::Float(-v)),
                    other => Err(SqlError::conversion(format!(
                        "operator '-' is not valid on {}",
                        other.type_name()
                    ))),
                },
                UnaryOp::Not => Ok(three_valued(value::not(value.as_predicate()))),
            }
        }
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => {
            let value = eval(inner, row, resolver)?;
            let is_null = value.is_null();
            Ok(SqlValue::Bool(is_null != *negated))
        }
        ExprKind::Binary { op, left, right } => {
            let l = eval(left, row, resolver)?;
            let r = eval(right, row, resolver)?;
            eval_binary(*op, l, r)
        }
    }
}

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
    // Integer arithmetic stays integer (T-SQL: int / int truncates); any
    // float operand promotes the whole expression to float.
    match (&l, &r) {
        (SqlValue::Int(a), SqlValue::Int(b)) => int_arithmetic(op, *a, *b),
        _ => {
            let (Some(a), Some(b)) = (as_f64(&l), as_f64(&r)) else {
                return Err(SqlError::conversion(format!(
                    "arithmetic operand type clash: {} vs {}",
                    l.type_name(),
                    r.type_name()
                )));
            };
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
    }
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

fn as_f64(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Int(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v),
        _ => None,
    }
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
        // A left-leaning OR chain near the parser's node budget (~1801 nodes)
        // must evaluate — recursing ~900 deep down its spine — without
        // overflowing the stack.
        let sql = format!("1{}", " OR 1".repeat(900));
        assert_eq!(eval_predicate(&sql, &[], &[]), SqlValue::Bool(true));
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
