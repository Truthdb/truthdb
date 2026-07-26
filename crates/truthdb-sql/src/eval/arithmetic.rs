use crate::ast::BinaryOp;
use crate::decimal::Decimal;
use crate::error::{SqlError, SqlResult};
use crate::value::{self, Numeric, SqlValue};

/// Arithmetic on two values with SQL Server numeric promotion (NULL-
/// propagating). Exposed for aggregate folding (SUM/AVG) in the executor.
pub fn arith(op: BinaryOp, left: SqlValue, right: SqlValue) -> SqlResult<SqlValue> {
    arithmetic(op, left, right)
}

pub(super) fn arithmetic(op: BinaryOp, l: SqlValue, r: SqlValue) -> SqlResult<SqlValue> {
    if l.is_null() || r.is_null() {
        return Ok(SqlValue::Null);
    }
    // `+` over two character operands is concatenation, not addition.
    if op == BinaryOp::Add
        && let (SqlValue::Str(a), SqlValue::Str(b)) = (&l, &r)
    {
        return Ok(SqlValue::Str(format!("{a}{b}")));
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
            // SQL Server: precision = min(p1-s1, p2-s2) + max(s1, s2).
            let int_digits = a
                .precision
                .saturating_sub(a.scale)
                .min(b.precision.saturating_sub(b.scale));
            let precision = (int_digits as u16 + scale as u16).clamp(1, 38) as u8;
            Decimal::new(x % y, precision, scale)
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
pub(super) fn three_valued(v: value::ThreeValued) -> SqlValue {
    match v {
        Some(b) => SqlValue::Bool(b),
        None => SqlValue::Null,
    }
}
