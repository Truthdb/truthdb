//! Expression evaluation over a bound row of [`SqlValue`]s.
//!
//! Column references are resolved to indices before evaluation (by the
//! binder in the storage crate), so `eval` takes the row as a slice and a
//! resolver mapping a column [`Name`] to its index. Arithmetic and
//! comparisons follow three-valued logic (see [`value`](crate::value)).

use std::cmp::Ordering;

use crate::ast::{BinaryOp, DataType, Expr, ExprKind, Name, UnaryOp};
use crate::decimal::Decimal;
use crate::error::{SqlError, SqlResult};
use crate::functions;
use crate::value::{self, Numeric, SqlValue};

/// The outcome of resolving a column name, distinguishing a missing column from
/// one that is ambiguous (matches more than one source column) so each maps to
/// the correct SQL Server error (208-family 207 vs 209).
pub enum Resolution {
    Found(usize),
    NotFound,
    Ambiguous,
}

/// Resolves a column name to its position in the row, case-insensitively.
pub trait ColumnResolver {
    fn resolve(&self, name: &str) -> Option<usize>;

    /// Like [`ColumnResolver::resolve`] but distinguishes not-found from
    /// ambiguous. The default cannot detect ambiguity (a `None` from `resolve`
    /// is reported as not-found); a multi-source resolver overrides it.
    fn resolve_detail(&self, name: &str) -> Resolution {
        match self.resolve(name) {
            Some(index) => Resolution::Found(index),
            None => Resolution::NotFound,
        }
    }
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

/// Session context available to expression evaluation: `@@`-variables, the
/// batch's `@`-variables, and (in later stages) the current time /
/// SCOPE_IDENTITY. `Default` is a no-transaction, no-variable context, used
/// where no session is in scope.
#[derive(Debug, Clone, Default)]
pub struct EvalContext {
    pub trancount: i32,
    /// Declared batch variables (name without `@`, lowercased) to their current
    /// value. Present but NULL for a declared-but-unset variable; absent means
    /// undeclared.
    pub variables: std::collections::HashMap<String, SqlValue>,
    /// The connection's current database name — `DB_NAME()`.
    pub database: String,
    /// The authenticated login name — `SUSER_SNAME()`.
    pub login: String,
    /// The session process id — `@@SPID`.
    pub spid: i32,
    /// The last identity value inserted in this scope — `SCOPE_IDENTITY()`.
    /// `None` until an identity INSERT runs.
    pub scope_identity: Option<i64>,
}

/// Evaluates `expr` against `row`, resolving columns via `resolver`.
pub fn eval(
    expr: &Expr,
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
    ctx: &EvalContext,
) -> SqlResult<SqlValue> {
    eval_at(expr, row, resolver, ctx, 0)
}

fn eval_at(
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
            eval_binary(*op, l, r)
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
        "error" | "rowcount" | "identity" => Ok(SqlValue::Int(0)),
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
    let matched = crate::like::like_match(text, pattern, escape);
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
    // `x IN (list)` is `x=a OR x=b OR ...` under three-valued logic.
    let mut any_unknown = false;
    for item in list {
        let candidate = eval_at(item, row, resolver, ctx, depth + 1)?;
        match value.compare(&candidate)? {
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
    // `x BETWEEN a AND b` is `x>=a AND x<=b` (three-valued).
    let ge = value.compare(&lo)?.map(|o| o != Ordering::Less);
    let le = value.compare(&hi)?.map(|o| o != Ordering::Greater);
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

#[inline(never)]
fn eval_call<R: ColumnResolver>(
    name: &str,
    args: &[Expr],
    row: &[SqlValue],
    resolver: &R,
    ctx: &EvalContext,
    depth: usize,
) -> SqlResult<SqlValue> {
    // Session-context functions read the EvalContext, which the pure
    // functions::eval_function does not receive.
    if let Some(value) = eval_session_function(name, args) {
        return Ok(value(ctx));
    }
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_at(arg, row, resolver, ctx, depth + 1)?);
    }
    functions::eval_function(name, values)
}

/// Session-identity intrinsics that resolve against the [`EvalContext`] rather
/// than their arguments. Returns a closure so the (cheap) context read happens
/// only when the name matches. With a single database, `DB_NAME()` and
/// `DB_NAME(id)` both report the current database.
fn eval_session_function(name: &str, args: &[Expr]) -> Option<fn(&EvalContext) -> SqlValue> {
    match name.to_ascii_uppercase().as_str() {
        "DB_NAME" if args.len() <= 1 => Some(|ctx| SqlValue::Str(ctx.database.clone())),
        "SUSER_SNAME" | "SUSER_NAME" if args.is_empty() => {
            Some(|ctx| SqlValue::Str(ctx.login.clone()))
        }
        // SQL Server returns NUMERIC(38,0); NULL when no identity has been
        // generated in the scope yet.
        "SCOPE_IDENTITY" if args.is_empty() => Some(|ctx| match ctx.scope_identity {
            Some(value) => SqlValue::Decimal(Box::new(Decimal::new(value as i128, 38, 0))),
            None => SqlValue::Null,
        }),
        _ => None,
    }
}

/// CAST/CONVERT: converts a value to a target [`DataType`], producing a value
/// of that type. Numeric overflow is 8115; a failed parse is 241.
#[inline(never)]
fn cast_value(value: SqlValue, target: &DataType) -> SqlResult<SqlValue> {
    if value.is_null() {
        return Ok(SqlValue::Null);
    }
    let overflow = || {
        SqlError::new(
            8115,
            16,
            2,
            format!(
                "Arithmetic overflow error converting to data type {}.",
                type_label(target)
            ),
        )
    };
    let cfail = |t: &str| {
        SqlError::message_only(
            241,
            format!("Conversion failed when converting to data type {t}."),
        )
    };
    match target {
        DataType::TinyInt => cast_int(&value, 0, u8::MAX as i64, overflow),
        DataType::SmallInt => cast_int(&value, i16::MIN as i64, i16::MAX as i64, overflow),
        DataType::Int => cast_int(&value, i32::MIN as i64, i32::MAX as i64, overflow),
        DataType::BigInt => cast_int(&value, i64::MIN, i64::MAX, overflow),
        DataType::Bit => Ok(SqlValue::Bool(
            cast_to_i64(&value).ok_or_else(|| cfail("bit"))? != 0,
        )),
        DataType::Real => Ok(SqlValue::Float(
            cast_to_f64(&value).ok_or_else(|| cfail("real"))? as f32 as f64,
        )),
        DataType::Float => Ok(SqlValue::Float(
            cast_to_f64(&value).ok_or_else(|| cfail("float"))?,
        )),
        DataType::Decimal { precision, scale } => {
            let d = cast_to_decimal(&value).ok_or_else(|| cfail("decimal"))?;
            d.coerce(*precision, *scale)
                .map(|d| SqlValue::Decimal(Box::new(d)))
                .map_err(|_| overflow())
        }
        DataType::VarChar(n) | DataType::NVarChar(n) => {
            // CAST to a char type truncates silently.
            let s: String = cast_to_string(&value).chars().take(*n as usize).collect();
            Ok(SqlValue::Str(s))
        }
        DataType::Date => match &value {
            SqlValue::Date(d) => Ok(SqlValue::Date(*d)),
            SqlValue::DateTime2(d, _) => Ok(SqlValue::Date(*d)),
            SqlValue::Str(s) => crate::temporal::parse_date(s)
                .map(SqlValue::Date)
                .ok_or_else(|| cfail("date")),
            _ => Err(cfail("date")),
        },
        DataType::Time => match &value {
            SqlValue::Time(t) => Ok(SqlValue::Time(*t)),
            SqlValue::DateTime2(_, t) => Ok(SqlValue::Time(*t)),
            SqlValue::Str(s) => crate::temporal::parse_time(s)
                .map(SqlValue::Time)
                .ok_or_else(|| cfail("time")),
            _ => Err(cfail("time")),
        },
        DataType::DateTime2 => match &value {
            SqlValue::DateTime2(d, t) => Ok(SqlValue::DateTime2(*d, *t)),
            SqlValue::Date(d) => Ok(SqlValue::DateTime2(*d, 0)),
            SqlValue::Str(s) => crate::temporal::parse_datetime2(s)
                .map(|(d, t)| SqlValue::DateTime2(d, t))
                .ok_or_else(|| cfail("datetime2")),
            _ => Err(cfail("datetime2")),
        },
        DataType::UniqueIdentifier => match &value {
            SqlValue::Guid(b) => Ok(SqlValue::Guid(*b)),
            SqlValue::Str(s) => crate::guid::parse(s)
                .map(SqlValue::Guid)
                .ok_or_else(|| cfail("uniqueidentifier")),
            _ => Err(cfail("uniqueidentifier")),
        },
        DataType::VarBinary(n) => match &value {
            SqlValue::Binary(b) => Ok(SqlValue::Binary(
                b.iter().take(*n as usize).copied().collect(),
            )),
            _ => Err(cfail("varbinary")),
        },
    }
}

fn type_label(target: &DataType) -> &'static str {
    match target {
        DataType::TinyInt => "tinyint",
        DataType::SmallInt => "smallint",
        DataType::Int => "int",
        DataType::BigInt => "bigint",
        DataType::Bit => "bit",
        DataType::Real => "real",
        DataType::Float => "float",
        DataType::Decimal { .. } => "decimal",
        DataType::Date => "date",
        DataType::Time => "time",
        DataType::DateTime2 => "datetime2",
        DataType::UniqueIdentifier => "uniqueidentifier",
        DataType::VarChar(_) => "varchar",
        DataType::NVarChar(_) => "nvarchar",
        DataType::VarBinary(_) => "varbinary",
    }
}

fn cast_int(
    value: &SqlValue,
    min: i64,
    max: i64,
    overflow: impl Fn() -> SqlError,
) -> SqlResult<SqlValue> {
    let v = cast_to_i64(value).ok_or_else(|| {
        SqlError::message_only(
            245,
            format!(
                "Conversion failed converting {} to an integer.",
                value.type_name()
            ),
        )
    })?;
    if v < min || v > max {
        return Err(overflow());
    }
    Ok(SqlValue::Int(v))
}

fn cast_to_i64(value: &SqlValue) -> Option<i64> {
    match value {
        SqlValue::Int(v) => Some(*v),
        SqlValue::Bool(b) => Some(*b as i64),
        // CAST to an integer type truncates toward zero (SQL Server); a float
        // out of i64 range fails rather than saturating.
        SqlValue::Float(f) => {
            let t = f.trunc();
            (t.is_finite() && t >= i64::MIN as f64 && t <= i64::MAX as f64).then_some(t as i64)
        }
        SqlValue::Decimal(d) => i64::try_from(d.truncated_to_int()).ok(),
        SqlValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn cast_to_f64(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Int(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v),
        SqlValue::Bool(b) => Some(*b as i64 as f64),
        SqlValue::Decimal(d) => Some(d.to_f64()),
        SqlValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn cast_to_decimal(value: &SqlValue) -> Option<Decimal> {
    match value {
        SqlValue::Decimal(d) => Some(**d),
        SqlValue::Int(v) => Some(Decimal::from_i64(*v)),
        SqlValue::Bool(b) => Some(Decimal::from_i64(*b as i64)),
        SqlValue::Str(s) => Decimal::parse(s),
        SqlValue::Float(f) => Decimal::parse(&format!("{f}")),
        _ => None,
    }
}

fn cast_to_string(value: &SqlValue) -> String {
    match value {
        SqlValue::Str(s) => s.clone(),
        SqlValue::Int(v) => v.to_string(),
        SqlValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Float(f) => format!("{f}"),
        SqlValue::Decimal(d) => d.render(),
        SqlValue::Date(days) => crate::temporal::render_date(*days),
        SqlValue::Time(t) => crate::temporal::render_time(*t),
        SqlValue::DateTime2(d, t) => crate::temporal::render_datetime2(*d, *t),
        SqlValue::Guid(b) => crate::guid::render(b),
        SqlValue::Binary(_) => String::new(),
        SqlValue::Null => String::new(),
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

/// Arithmetic on two values with SQL Server numeric promotion (NULL-
/// propagating). Exposed for aggregate folding (SUM/AVG) in the executor.
pub fn arith(op: BinaryOp, left: SqlValue, right: SqlValue) -> SqlResult<SqlValue> {
    arithmetic(op, left, right)
}

fn arithmetic(op: BinaryOp, l: SqlValue, r: SqlValue) -> SqlResult<SqlValue> {
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
        eval(expr, row, &names, &EvalContext::default()).expect("eval")
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
    fn session_identity_intrinsics() {
        let ctx = EvalContext {
            database: "truthdb".to_string(),
            login: "sa".to_string(),
            spid: 53,
            ..EvalContext::default()
        };
        let eval_ctx = |sql: &str| {
            let statements = Parser::parse_str(&format!("SELECT {sql}")).expect("parse");
            let crate::ast::Statement::Select(select) = &statements[0] else {
                panic!("expected select")
            };
            let crate::ast::SelectItem::Expr { expr, .. } = &select.items[0] else {
                panic!("expected expr")
            };
            let no_columns: Vec<String> = Vec::new();
            eval(expr, &[], &no_columns, &ctx).expect("eval")
        };
        assert_eq!(eval_ctx("DB_NAME()"), SqlValue::Str("truthdb".to_string()));
        assert_eq!(eval_ctx("DB_NAME(1)"), SqlValue::Str("truthdb".to_string()));
        assert_eq!(eval_ctx("SUSER_SNAME()"), SqlValue::Str("sa".to_string()));
        assert_eq!(eval_ctx("SUSER_NAME()"), SqlValue::Str("sa".to_string()));
        assert_eq!(eval_ctx("@@SPID"), SqlValue::Int(53));
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
        assert_eq!(
            eval(expr, &[], &empty, &EvalContext::default())
                .unwrap_err()
                .number,
            8134
        );
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
        assert_eq!(
            eval(expr, &[], &empty, &EvalContext::default())
                .unwrap_err()
                .number,
            191
        );
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
