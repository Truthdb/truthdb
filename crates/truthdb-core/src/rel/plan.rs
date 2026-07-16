//! Single-table access-path selection (Stage 7): choose a sargable index seek
//! over a full table scan. The chosen path only *narrows* the candidate set —
//! the executor re-applies the whole WHERE afterwards — so seek bounds may
//! over-fetch and results are always identical to a scan. That property keeps
//! the planner simple and the A/B (index vs no-index) results equal.

use truthdb_sql::ast::{BinaryOp, Expr, ExprKind};
use truthdb_sql::eval::{self, EvalContext};

use crate::relstore::catalog::{IndexDef, TableDef};
use crate::relstore::index;
use crate::relstore::row::{Column, Schema};
use crate::relstore::types::{ColumnType, Datum};

use super::value::sql_to_datum;

/// The access path chosen for a single-table read.
pub enum AccessPath {
    /// Full clustered/heap scan.
    TableScan,
    /// Seek an index over `[lower, upper]` (either bound `None` = open), then
    /// look up each row by its locator.
    IndexSeek {
        index_object_id: u32,
        index_name: String,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        /// Seek predicate descriptions, for `SHOWPLAN`.
        predicates: Vec<String>,
        unique: bool,
    },
}

/// A sargable atom `column <op> constant`.
struct Sarg {
    column: usize,
    op: BinaryOp,
    value: Datum,
}

/// Inclusive `(lower, upper)` seek bounds (either side `None` = open).
type Bounds = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Chooses the access path for a table read given its WHERE clause. Prefers
/// the index matching the most equality columns (a fully-matched UNIQUE index
/// — a unique seek — wins outright), else a range seek, else a scan. Among
/// equality-equal candidates, one whose `INCLUDE` list covers every column
/// the query reads (`needed`, when the caller knows it) wins: it answers
/// from its leaves with no per-row base-table lookup. Coverage never
/// outranks a better equality match or a fully-matched UNIQUE seek — a
/// single-row lookup beats a covering scan of many.
pub fn choose(
    def: &TableDef,
    schema: &Schema,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
    needed: Option<&[usize]>,
) -> AccessPath {
    let Some(predicate) = where_clause else {
        return AccessPath::TableScan;
    };
    if def.indexes.is_empty() {
        return AccessPath::TableScan;
    }
    let mut sargs = Vec::new();
    collect_sargs(predicate, schema, eval_ctx, &mut sargs);
    if sargs.is_empty() {
        return AccessPath::TableScan;
    }
    let mut best: Option<(u32, AccessPath)> = None;
    for index in &def.indexes {
        if let Some((mut score, path)) = try_index(index, schema, &sargs) {
            let covers =
                needed.is_some_and(|needed| needed.iter().all(|c| index.include.contains(c)));
            if covers {
                // Between the range bonus (+1) and an extra equality column
                // (+10): an avoided lookup per row outranks a somewhat
                // narrower range, never a more selective seek.
                score += 5;
            }
            if best.as_ref().is_none_or(|(s, _)| score > *s) {
                best = Some((score, path));
            }
        }
    }
    best.map(|(_, path)| path).unwrap_or(AccessPath::TableScan)
}

/// Renders the plan as `SHOWPLAN_TEXT` rows. A covering seek (every needed
/// column INCLUDEd in the leaf) answers from the index alone, so it has no
/// Key Lookup line.
pub fn plan_text(path: &AccessPath, table: &str, covering: bool) -> Vec<String> {
    match path {
        AccessPath::TableScan => vec![format!("Table Scan({table})")],
        AccessPath::IndexSeek {
            index_name,
            predicates,
            unique,
            ..
        } => {
            let kind = match (covering, *unique) {
                (true, _) => "Index Seek (covering)",
                (false, true) => "Index Seek (unique)",
                (false, false) => "Index Seek",
            };
            let mut lines = vec![format!(
                "{kind}({table}.{index_name}), SEEK: {}",
                predicates.join(", ")
            )];
            if !covering {
                lines.push(format!("Key Lookup({table})"));
            }
            lines
        }
    }
}

fn collect_sargs(expr: &Expr, schema: &Schema, eval_ctx: &EvalContext, out: &mut Vec<Sarg>) {
    match &expr.kind {
        ExprKind::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            collect_sargs(left, schema, eval_ctx, out);
            collect_sargs(right, schema, eval_ctx, out);
        }
        ExprKind::Binary { op, left, right } if is_seekable(*op) => {
            if let Some(sarg) = as_sarg(*op, left, right, schema, eval_ctx) {
                out.push(sarg);
            }
        }
        _ => {}
    }
}

fn is_seekable(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    )
}

fn flip(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Le => BinaryOp::Ge,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Ge => BinaryOp::Le,
        other => other,
    }
}

fn as_sarg(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    schema: &Schema,
    eval_ctx: &EvalContext,
) -> Option<Sarg> {
    if let ExprKind::Column(name) = &left.kind
        && let Some(column) = resolve_column(schema, &name.value)
        && let Some(value) = eval_const(
            right,
            &schema.columns[column].column_type,
            &name.value,
            eval_ctx,
        )
    {
        return Some(Sarg { column, op, value });
    }
    if let ExprKind::Column(name) = &right.kind
        && let Some(column) = resolve_column(schema, &name.value)
        && let Some(value) = eval_const(
            left,
            &schema.columns[column].column_type,
            &name.value,
            eval_ctx,
        )
    {
        return Some(Sarg {
            column,
            op: flip(op),
            value,
        });
    }
    None
}

/// Whether a column may back an index *range* seek. A seek is correct only when
/// the index's key byte order matches the WHERE filter's compare order for that
/// column. The filter now compares strings by the column's collation (Stage 5)
/// and the index key is folded the same way (`fold_key_datum`), so VARCHAR keys
/// (UTF-8 bytes of the folded string) match the filter order — equality seeks
/// (folded literal vs folded key) and VARCHAR ranges stay correct, including
/// case-insensitively. NVARCHAR index keys are UTF-16BE, whose byte order
/// diverges from the filter's order at supplementary-plane characters (surrogate
/// pairs) regardless of folding — so an NVARCHAR *range* bound could exclude a
/// matching row the filter keeps, and only NVARCHAR ranges are excluded here.
/// (Equality seeks stay correct for NVARCHAR: they are exact folded-key matches.)
///
/// Locale-specific ordering (Swedish å-after-z) would still need collation sort
/// keys; case-folding alone gives correct *equality*, not linguistic order.
fn range_seekable_column(column: &Column) -> bool {
    !matches!(column.column_type, ColumnType::NVarChar { .. })
}

/// Evaluates a would-be constant operand. Returns None if it references a
/// column, is NULL, or does not coerce to the column type (all non-sargable —
/// execution's own filter handles them).
fn eval_const(
    expr: &Expr,
    column_type: &crate::relstore::types::ColumnType,
    column_name: &str,
    eval_ctx: &EvalContext,
) -> Option<Datum> {
    let empty: Vec<String> = Vec::new();
    let value = eval::eval(expr, &[], &empty, eval_ctx).ok()?;
    if value.is_null() {
        return None;
    }
    sql_to_datum(&value, column_type, column_name).ok()
}

fn resolve_column(schema: &Schema, name: &str) -> Option<usize> {
    schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
}

fn try_index(index: &IndexDef, schema: &Schema, sargs: &[Sarg]) -> Option<(u32, AccessPath)> {
    // Equality prefix over the leading index columns.
    let mut eq_values: Vec<Datum> = Vec::new();
    let mut predicates: Vec<String> = Vec::new();
    let mut i = 0;
    while i < index.columns.len() {
        let (col, _) = index.columns[i];
        if let Some(sarg) = sargs
            .iter()
            .find(|s| s.column == col && s.op == BinaryOp::Eq)
        {
            predicates.push(format!(
                "{} = {}",
                schema.columns[col].name,
                datum_display(&sarg.value)
            ));
            eq_values.push(sarg.value.clone());
            i += 1;
        } else {
            break;
        }
    }
    // Optional single range on the next column. Ascending only (a descending
    // range would need reversed bounds) and never NVARCHAR (UTF-16BE key order
    // can diverge from the filter's code-point order); equality prefixes above
    // are exact matches and stay correct for all types.
    let mut lower_extra: Option<Datum> = None;
    let mut upper_extra: Option<Datum> = None;
    if i < index.columns.len() {
        let (col, ascending) = index.columns[i];
        if ascending && range_seekable_column(&schema.columns[col]) {
            for sarg in sargs.iter().filter(|s| s.column == col) {
                match sarg.op {
                    BinaryOp::Gt | BinaryOp::Ge => {
                        lower_extra = Some(sarg.value.clone());
                        predicates.push(format!(
                            "{} {} {}",
                            schema.columns[col].name,
                            op_text(sarg.op),
                            datum_display(&sarg.value)
                        ));
                    }
                    BinaryOp::Lt | BinaryOp::Le => {
                        upper_extra = Some(sarg.value.clone());
                        predicates.push(format!(
                            "{} {} {}",
                            schema.columns[col].name,
                            op_text(sarg.op),
                            datum_display(&sarg.value)
                        ));
                    }
                    _ => {}
                }
            }
        }
    }
    if eq_values.is_empty() && lower_extra.is_none() && upper_extra.is_none() {
        return None;
    }
    let (lower, upper) = build_bounds(index, schema, &eq_values, &lower_extra, &upper_extra)?;
    let unique_full = index.unique && eq_values.len() == index.columns.len();
    let score = (eq_values.len() as u32) * 10
        + if unique_full { 1000 } else { 0 }
        + u32::from(lower_extra.is_some() || upper_extra.is_some());
    Some((
        score,
        AccessPath::IndexSeek {
            index_object_id: index.object_id,
            index_name: index.name.clone(),
            lower,
            upper,
            predicates,
            unique: index.unique,
        },
    ))
}

fn build_bounds(
    index: &IndexDef,
    schema: &Schema,
    eq_values: &[Datum],
    lower_extra: &Option<Datum>,
    upper_extra: &Option<Datum>,
) -> Option<Bounds> {
    let cols = &index.columns;
    // Per-column collations (by schema index) so a seek literal folds exactly as
    // the stored index key did — a case-insensitive character seek matches.
    let collations: Vec<Option<String>> =
        schema.columns.iter().map(|c| c.collation.clone()).collect();
    let mut lower_vals = eq_values.to_vec();
    if let Some(value) = lower_extra {
        lower_vals.push(value.clone());
    }
    let lower = if lower_vals.is_empty() {
        None
    } else {
        Some(index::encode_index_prefix(&lower_vals, cols, &collations).ok()?)
    };
    let upper = if let Some(value) = upper_extra {
        let mut upper_vals = eq_values.to_vec();
        upper_vals.push(value.clone());
        let encoded = index::encode_index_prefix(&upper_vals, cols, &collations).ok()?;
        index::prefix_upper_bound(&encoded)
    } else if !eq_values.is_empty() {
        let encoded = index::encode_index_prefix(eq_values, cols, &collations).ok()?;
        index::prefix_upper_bound(&encoded)
    } else {
        None
    };
    Some((lower, upper))
}

fn op_text(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        _ => "?",
    }
}

fn datum_display(datum: &Datum) -> String {
    match datum {
        Datum::TinyInt(v) => v.to_string(),
        Datum::SmallInt(v) => v.to_string(),
        Datum::Int(v) => v.to_string(),
        Datum::BigInt(v) => v.to_string(),
        Datum::VarChar(s) | Datum::NVarChar(s) => format!("'{s}'"),
        other => format!("{other:?}"),
    }
}
