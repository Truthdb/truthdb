use super::prelude::*;

// ---- SELECT -------------------------------------------------------------

/// Rows a table scan reads per slice before dropping the storage lock and
/// letting another session in. Large enough that the per-slice overhead (a lock
/// acquisition and a catalog lookup) is noise against decoding the rows, small
/// enough that a big scan yields often.
pub(super) const SCAN_SLICE_ROWS: usize = 1024;

pub(super) struct Source {
    pub(super) columns: Vec<ResultColumn>,
    /// Per-column table qualifier (alias or table name; `None` = virtual/
    /// constant source), parallel to `columns`. Drives multi-table resolution.
    pub(super) qualifiers: Vec<Option<String>>,
    /// Per-column collation names (parallel to `columns`; `None` = database
    /// default). Used by ORDER BY on character columns.
    pub(super) collations: Vec<Option<String>>,
    /// Rows of typed values (real-table Datums; virtual sources build them).
    pub(super) rows: SourceRows,
}

/// A source's rows: already whole, or pulled slice-by-slice from a base-table
/// scan as the consumer iterates (Stage 8 streaming scans, the input side). A
/// consumer that filters or folds row-at-a-time holds one slice, not the
/// table; one that needs the whole input calls [`SourceRows::materialize`].
pub(super) enum SourceRows {
    Materialized(Vec<Vec<Datum>>),
    Scan(ScanStream),
}

/// A base-table scan not yet read: full-width rows, [`SCAN_SLICE_ROWS`] at a
/// time on the resumable cursor. The storage lock is taken per slice, as
/// everywhere since #96; under every isolation level that takes read locks
/// the table's S lock spans the whole batch, so lazy pulling changes no
/// isolation semantics — and READ UNCOMMITTED took no read lock before
/// either (the cursor's per-page object check safe-stops on a recycled
/// page, as the B+ tree layer documents).
pub(super) struct ScanStream {
    db_id: u32,
    table: String,
    cursor: ScanCursor,
}

impl ScanStream {
    fn next_slice(&mut self, storage: &Storage) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        let mut slice = Vec::new();
        while !self.cursor.done() && slice.is_empty() {
            check_cancelled()?;
            self.cursor = storage
                .rel_scan_slice(
                    self.db_id,
                    &self.table,
                    self.cursor,
                    SCAN_SLICE_ROWS,
                    None,
                    &mut slice,
                )
                .map_err(|err| map_storage_err(err, &self.table))?;
        }
        Ok(if slice.is_empty() { None } else { Some(slice) })
    }
}

/// A [`Source`] with its rows fully in hand — the join operators' BUILD side,
/// which is walked repeatedly (nested loop) or hashed whole (the grace-hash
/// spill bounds it past the memory budget). The probe side never takes this
/// form: it streams via [`SourceRows::next_slice`].
pub(super) struct MaterializedSource {
    columns: Vec<ResultColumn>,
    collations: Vec<Option<String>>,
    rows: Vec<Vec<Datum>>,
}

impl MaterializedSource {
    fn from(source: Source, storage: &Storage) -> Result<Self, SqlError> {
        let Source {
            columns,
            collations,
            rows,
            ..
        } = source;
        Ok(MaterializedSource {
            columns,
            collations,
            rows: rows.materialize(storage)?,
        })
    }
}

impl SourceRows {
    /// Pulls the next batch of rows, for a consumer that walks the source
    /// exactly once (a join's probe side). A scan hands over its next slice; a
    /// materialized source hands everything in one batch.
    fn next_slice(&mut self, storage: &Storage) -> Result<Option<Vec<Vec<Datum>>>, SqlError> {
        match self {
            SourceRows::Materialized(rows) => {
                if rows.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(std::mem::take(rows)))
                }
            }
            SourceRows::Scan(stream) => stream.next_slice(storage),
        }
    }

    /// The whole input, for consumers that need it at once. A scan drains its
    /// remaining slices; a materialized source hands its rows over as-is.
    fn materialize(self, storage: &Storage) -> Result<Vec<Vec<Datum>>, SqlError> {
        match self {
            SourceRows::Materialized(rows) => Ok(rows),
            SourceRows::Scan(mut stream) => {
                #[cfg(test)]
                storage.count_scan_materialization();
                let mut rows = Vec::new();
                while let Some(mut slice) = stream.next_slice(storage)? {
                    rows.append(&mut slice);
                }
                Ok(rows)
            }
        }
    }
}

impl Source {
    fn types(&self) -> Vec<ColumnType> {
        self.columns.iter().map(|c| c.column_type).collect()
    }

    fn scope(&self) -> JoinScope {
        JoinScope {
            columns: self
                .qualifiers
                .iter()
                .zip(&self.columns)
                .map(|(qualifier, column)| (qualifier.clone(), column.name.clone()))
                .collect(),
            collations: self.collations.clone(),
        }
    }
}

/// Resolves column references against a (possibly multi-table) row source. A
/// dotted `t.col` matches by qualifier + name; a bare `col` matches a unique
/// column (ambiguous or unknown → `None`, surfaced by eval as an invalid-
/// column error).
pub(super) struct JoinScope {
    /// (qualifier, bare column name) per source column.
    columns: Vec<(Option<String>, String)>,
    /// Per-column collation names, parallel to `columns` (`None` = database
    /// default). Empty for correlation-only scopes that never drive comparison.
    collations: Vec<Option<String>>,
}

/// Resolver over an output RowSet's columns. Output columns are unqualified,
/// so a qualified `t.col` reference (e.g. in a grouped query's ORDER BY)
/// resolves by its bare name.
///
/// It does not carry per-column collation, so an *embedded equality* in an
/// ORDER BY expression over a `_CS`/`_BIN` output column (e.g.
/// `ORDER BY CASE WHEN code = 'ABC' THEN 0 ELSE 1 END`) folds case
/// (case-insensitive default). The sort key itself is collation-correct — the
/// non-aggregated path orders via `sort_collators` (real per-column collation)
/// and the aggregated/DISTINCT path via `order_key_cmp` — so this only affects a
/// nested `=` inside an ORDER BY expression on a case-sensitive column: a narrow,
/// documented limitation.
pub(super) struct OutputScope {
    names: Vec<String>,
}

impl truthdb_sql::eval::ColumnResolver for OutputScope {
    fn resolve(&self, name: &str) -> Option<usize> {
        let bare = name.rsplit('.').next().unwrap_or(name);
        self.names.iter().position(|n| n.eq_ignore_ascii_case(bare))
    }
}

impl JoinScope {
    /// True if any column matches `name` — even ambiguously (>1 match), where
    /// [`ColumnResolver::resolve`] returns `None`. Correlation analysis uses this
    /// to tell "the inner scope has this name (bind/error here)" from "the name
    /// is absent (it is an outer reference)": an ambiguous inner column must NOT
    /// be rebound to a same-named outer column.
    fn matches_any(&self, name: &str) -> bool {
        self.columns.iter().any(|(qualifier, column)| {
            if let Some((q, c)) = name.rsplit_once('.') {
                qualifier
                    .as_deref()
                    .is_some_and(|qq| qq.eq_ignore_ascii_case(q))
                    && column.eq_ignore_ascii_case(c)
            } else {
                column.eq_ignore_ascii_case(name)
            }
        })
    }

    /// Source-column indices belonging to a table qualifier (for `t.*`).
    fn indices_for_qualifier(&self, qualifier: &str) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, (q, _))| {
                q.as_deref()
                    .is_some_and(|q| q.eq_ignore_ascii_case(qualifier))
            })
            .map(|(index, _)| index)
            .collect()
    }
}

impl truthdb_sql::eval::ColumnResolver for JoinScope {
    fn resolve(&self, name: &str) -> Option<usize> {
        match self.resolve_detail(name) {
            truthdb_sql::eval::Resolution::Found(index) => Some(index),
            // Ambiguous and not-found both fail to bind a single column.
            _ => None,
        }
    }

    fn resolve_detail(&self, name: &str) -> truthdb_sql::eval::Resolution {
        use truthdb_sql::eval::Resolution;
        let matches = |q: &Option<String>, c: &str| -> bool {
            if let Some((qualifier, column)) = name.rsplit_once('.') {
                q.as_deref()
                    .is_some_and(|q| q.eq_ignore_ascii_case(qualifier))
                    && c.eq_ignore_ascii_case(column)
            } else {
                c.eq_ignore_ascii_case(name)
            }
        };
        let mut found = None;
        for (index, (qualifier, column)) in self.columns.iter().enumerate() {
            if matches(qualifier, column) {
                if found.is_some() {
                    return Resolution::Ambiguous; // more than one match
                }
                found = Some(index);
            }
        }
        match found {
            Some(index) => Resolution::Found(index),
            None => Resolution::NotFound,
        }
    }

    fn collation(&self, index: usize) -> truthdb_sql::collation::CollationSensitivity {
        truthdb_sql::collation::CollationSensitivity::from_optional(
            self.collations.get(index).and_then(|c| c.as_deref()),
        )
    }
}

/// SqlValues of a row, for expression evaluation. `types` (parallel to `row`)
/// restores each value's exact type (e.g. a DECIMAL's scale).
pub(super) fn row_values(row: &[Datum], types: &[ColumnType]) -> Vec<SqlValue> {
    row.iter()
        .zip(types)
        .map(|(d, t)| value::datum_to_sql(d, t))
        .collect()
}

// ---- common table expressions -------------------------------------------

/// Inlines a SELECT's `WITH` common table expressions: each FROM reference to a
/// CTE name becomes a derived table over the CTE's query. CTEs are expanded in
/// order, so a later CTE may reference an earlier one; non-recursive (a self- or
/// forward-reference is left as a base-table name and errors at bind). Returns a
/// CTE-free SELECT.
pub(super) type CteMap = std::collections::HashMap<String, Select>;

pub(super) fn expand_ctes(select: &Select) -> Select {
    expand_select_ctes(select, &CteMap::new())
}

/// A copy of `select` with every CTE reference — at this level and nested inside
/// its subqueries — replaced by a derived table. `outer` is the enclosing CTE
/// scope; this select's own `WITH` layers on top of it (so a nested `WITH` sees
/// enclosing CTEs and is itself inlined). The result carries no `ctes` at any
/// level, so lock analysis, which walks the expanded tree without re-expanding,
/// still sees every base table the executor reads.
pub(super) fn expand_select_ctes(select: &Select, outer: &CteMap) -> Select {
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
pub(super) fn expand_from_ctes(tref: &TableRef, resolved: &CteMap) -> TableRef {
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
pub(super) fn expand_expr_ctes(expr: &Expr, resolved: &CteMap) -> Expr {
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

// ---- subquery resolution ------------------------------------------------

/// Returns a copy of a SELECT with every subquery in its expressions
/// (WHERE/HAVING/SELECT list/GROUP BY/ORDER BY) evaluated and replaced by a
/// precomputed literal. Subqueries in a FROM-clause join `ON` are not rewritten
/// here (they are rare and error at evaluation). Only uncorrelated subqueries
/// are supported; a correlated one references an outer column and fails to
/// resolve when executed independently.
pub(super) fn rewrite_select_subqueries(
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
pub(super) fn rewrite_subqueries(
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
pub(super) fn eval_scalar_subquery(
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
pub(super) fn eval_in_subquery(
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

pub(super) fn scalar_subquery_shape_err() -> SqlError {
    SqlError::new(
        116,
        16,
        1,
        "Only one expression can be specified in the select list when the subquery is not introduced with EXISTS.",
    )
}

// ---- correlated subquery support ----------------------------------------
//
// A subquery that references an enclosing query's column is *correlated*. It is
// left un-evaluated by `rewrite_select_subqueries` (which only folds away
// uncorrelated subqueries) and instead re-run once per outer row: the per-row
// WHERE loop calls `substitute_correlated_in_expr`, which binds the outer row's
// values into the subquery (`substitute_subquery_outer_refs`) and evaluates it.
// This is the "correct, slow, honest" per-row apply. Supported for correlated
// subqueries in the WHERE clause over base-table / join subqueries; a correlated
// reference inside a derived-table / view subquery (whose inner scope cannot be
// read from the catalog) falls back to the prior behavior (invalid-column 207).

/// The `(qualifier, bare column name)` columns a FROM clause exposes, read from
/// the catalog WITHOUT materializing rows. `None` if the FROM has a derived
/// table or a view, whose output columns need binding to determine.
pub(super) fn from_column_names(
    storage: &Storage,
    db_id: u32,
    from: &TableRef,
) -> Option<Vec<(Option<String>, String)>> {
    match from {
        TableRef::Table { name, alias } => {
            let def = resolve_table(storage, db_id, &name.value)?;
            // A view defers to its expansion; a PROCEDURE/FUNCTION/TRIGGER must
            // not read as a zero-column table — bailing here routes to the
            // collecting path, which errors 2809/208.
            if def.is_view() || def.is_procedure() || def.is_function() || def.is_trigger() {
                return None;
            }
            let qualifier = alias
                .as_ref()
                .map(|a| a.value.clone())
                .unwrap_or_else(|| strip_schema(&name.value).to_string());
            Some(
                def.columns
                    .iter()
                    .map(|(cname, _, _)| (Some(qualifier.clone()), cname.clone()))
                    .collect(),
            )
        }
        TableRef::Join { left, right, .. } => {
            let mut cols = from_column_names(storage, db_id, left)?;
            cols.extend(from_column_names(storage, db_id, right)?);
            Some(cols)
        }
        TableRef::Derived { subquery, alias } => {
            let mut cols = Vec::new();
            for item in &subquery.items {
                match item {
                    SelectItem::Expr { expr, alias: a } => {
                        let name = a
                            .as_ref()
                            .map(|n| n.value.clone())
                            .or_else(|| bare_column_name(expr))?;
                        cols.push((Some(alias.value.clone()), name));
                    }
                    SelectItem::Wildcard => {
                        let inner = from_column_names(storage, db_id, subquery.from.as_ref()?)?;
                        cols.extend(
                            inner
                                .into_iter()
                                .map(|(_, n)| (Some(alias.value.clone()), n)),
                        );
                    }
                    SelectItem::QualifiedWildcard(q) => {
                        let inner = from_column_names(storage, db_id, subquery.from.as_ref()?)?;
                        cols.extend(
                            inner
                                .into_iter()
                                .filter(|(qu, _)| {
                                    qu.as_deref()
                                        .is_some_and(|x| x.eq_ignore_ascii_case(&q.value))
                                })
                                .map(|(_, n)| (Some(alias.value.clone()), n)),
                        );
                    }
                    SelectItem::Assign { .. } => return None,
                }
            }
            Some(cols)
        }
        // A TVF's output columns are its body SELECT's projection — only known
        // after the body is parsed and bound, like a view. Defer to expansion.
        TableRef::Function { .. } => None,
    }
}

/// The inner scope of a subquery (its own FROM columns), or `None` if it cannot
/// be determined from the catalog alone.
pub(super) fn subquery_inner_scope(
    storage: &Storage,
    db_id: u32,
    subquery: &Select,
) -> Option<JoinScope> {
    let columns = match &subquery.from {
        Some(from) => from_column_names(storage, db_id, from)?,
        None => Vec::new(),
    };
    Some(JoinScope {
        collations: Vec::new(),
        columns,
    })
}

/// True if `subquery` references a column that resolves in the enclosing `outer`
/// scope but not in its own FROM — i.e. it is correlated.
pub(super) fn is_correlated(
    storage: &Storage,
    db_id: u32,
    subquery: &Select,
    outer: &JoinScope,
) -> bool {
    let Some(inner) = subquery_inner_scope(storage, db_id, subquery) else {
        return false;
    };
    let mut correlated = false;
    select_column_refs(subquery, &mut |name| {
        // `matches_any` (not `resolve`) so an *ambiguous* inner column is treated
        // as inner (it errors in the subquery) rather than rebound to the outer.
        if !inner.matches_any(&name.value) && outer.resolve(&name.value).is_some() {
            correlated = true;
        }
    });
    // A correlated reference may live inside a derived table's body — its own
    // clauses resolve in the derived scope, so the walk above never sees it.
    correlated || from_has_correlated_derived(storage, db_id, subquery.from.as_ref(), outer)
}

/// Calls `f` on every column reference inside an AGGREGATE argument anywhere
/// in the select's own clauses (not descending into nested subqueries).
pub(super) fn select_aggregate_arg_refs(select: &Select, f: &mut impl FnMut(&Name)) {
    fn walk(expr: &Expr, f: &mut impl FnMut(&Name)) {
        match &expr.kind {
            ExprKind::Aggregate { arg: Some(a), .. } => expr_column_refs(a, f),
            ExprKind::Aggregate { arg: None, .. } => {}
            ExprKind::Unary { expr: e, .. }
            | ExprKind::IsNull { expr: e, .. }
            | ExprKind::Cast { expr: e, .. } => walk(e, f),
            ExprKind::Binary { left, right, .. } => {
                walk(left, f);
                walk(right, f);
            }
            ExprKind::Like {
                expr: e, pattern, ..
            } => {
                walk(e, f);
                walk(pattern, f);
            }
            ExprKind::InList { expr: e, list, .. } => {
                walk(e, f);
                list.iter().for_each(|x| walk(x, f));
            }
            ExprKind::Between {
                expr: e, low, high, ..
            } => {
                walk(e, f);
                walk(low, f);
                walk(high, f);
            }
            ExprKind::Function { args, .. } => args.iter().for_each(|x| walk(x, f)),
            ExprKind::Case {
                operand,
                branches,
                else_result,
            } => {
                if let Some(o) = operand {
                    walk(o, f);
                }
                for (w, r) in branches {
                    walk(w, f);
                    walk(r, f);
                }
                if let Some(e) = else_result {
                    walk(e, f);
                }
            }
            _ => {}
        }
    }
    if let Some(w) = &select.where_clause {
        walk(w, f);
    }
    for item in &select.items {
        if let SelectItem::Expr { expr, .. } = item {
            walk(expr, f);
        }
    }
    if let Some(h) = &select.having {
        walk(h, f);
    }
}

/// Whether any derived-table body in a FROM tree is correlated to `outer`.
pub(super) fn from_has_correlated_derived(
    storage: &Storage,
    db_id: u32,
    from: Option<&TableRef>,
    outer: &JoinScope,
) -> bool {
    match from {
        None | Some(TableRef::Table { .. }) => false,
        Some(TableRef::Join { left, right, .. }) => {
            from_has_correlated_derived(storage, db_id, Some(left), outer)
                || from_has_correlated_derived(storage, db_id, Some(right), outer)
        }
        Some(TableRef::Derived { subquery, .. }) => is_correlated(storage, db_id, subquery, outer),
        // A TVF's literal/non-outer arguments do not correlate it to the outer
        // FROM (APPLY, with outer-referencing args, is out of scope).
        Some(TableRef::Function { .. }) => false,
    }
}

/// Calls `f` on every column reference in a select's own clauses (WHERE, SELECT
/// items, HAVING, GROUP BY, ORDER BY), not descending into nested subqueries
/// (which resolve in their own scope).
pub(super) fn select_column_refs(select: &Select, f: &mut impl FnMut(&Name)) {
    if let Some(w) = &select.where_clause {
        expr_column_refs(w, f);
    }
    for item in &select.items {
        match item {
            SelectItem::Expr { expr, .. } => expr_column_refs(expr, f),
            SelectItem::Assign { value, .. } => expr_column_refs(value, f),
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {}
        }
    }
    if let Some(h) = &select.having {
        expr_column_refs(h, f);
    }
    for e in &select.group_by {
        expr_column_refs(e, f);
    }
    for o in &select.order_by {
        expr_column_refs(&o.expr, f);
    }
}

/// Calls `f` on every column reference in an expression, not descending into
/// nested subquery bodies.
pub(super) fn expr_column_refs(expr: &Expr, f: &mut impl FnMut(&Name)) {
    match &expr.kind {
        ExprKind::Column(name) => f(name),
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_)
        | ExprKind::Subquery(_)
        | ExprKind::Exists(_) => {}
        // The IN operand is at this scope; the subquery body is not.
        ExprKind::InSubquery { expr: e, .. } => expr_column_refs(e, f),
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => expr_column_refs(e, f),
        ExprKind::Binary { left, right, .. } => {
            expr_column_refs(left, f);
            expr_column_refs(right, f);
        }
        ExprKind::Like {
            expr: e, pattern, ..
        } => {
            expr_column_refs(e, f);
            expr_column_refs(pattern, f);
        }
        ExprKind::InList { expr: e, list, .. } => {
            expr_column_refs(e, f);
            list.iter().for_each(|x| expr_column_refs(x, f));
        }
        ExprKind::Between {
            expr: e, low, high, ..
        } => {
            expr_column_refs(e, f);
            expr_column_refs(low, f);
            expr_column_refs(high, f);
        }
        ExprKind::Function { args, .. } => args.iter().for_each(|a| expr_column_refs(a, f)),
        ExprKind::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                expr_column_refs(a, f);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(o) = operand {
                expr_column_refs(o, f);
            }
            for (w, r) in branches {
                expr_column_refs(w, f);
                expr_column_refs(r, f);
            }
            if let Some(e) = else_result {
                expr_column_refs(e, f);
            }
        }
    }
}

/// Rewrites ORDER BY so a SELECT-list alias resolves on the path that sorts the
/// *source* rows.
///
/// ORDER BY is the one clause where a SELECT alias is in scope, but this path
/// sorts base rows against a [`JoinScope`], which knows only base columns — so
/// `SELECT v AS vv FROM a ORDER BY vv` would not resolve, and an alias over a
/// computed expression (`SELECT v * 2 AS dbl ... ORDER BY dbl`) has no base
/// column to fall back on at all. Each unqualified name matching an alias is
/// replaced by that alias's expression, which the existing sort machinery then
/// evaluates against the base row. (The grouped/DISTINCT path projects first and
/// resolves ORDER BY against the output names, so it already sees aliases.)
///
/// An alias shadows a base column of the same name, as in SQL Server. A
/// qualified name (`a.v`) always means that table's column and is never
/// rewritten. `map_expr_columns` does not rescan what it substitutes, so a
/// self-referential alias (`SELECT v + 1 AS v ... ORDER BY v`) substitutes once
/// instead of recursing forever. Ordinals (`ORDER BY 1`) are integers, not
/// column references, so they are untouched.
pub(super) fn order_by_with_aliases(
    order_by: &[OrderItem],
    items: &[SelectItem],
    scope: &JoinScope,
) -> Result<Vec<OrderItem>, SqlError> {
    let aliases: Vec<(&str, &Expr)> = items
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr {
                expr,
                alias: Some(name),
            } => Some((name.value.as_str(), expr)),
            _ => None,
        })
        .collect();
    let outputs = output_exprs(items, scope);
    order_by
        .iter()
        .map(|item| {
            // A bare integer is a 1-based output-column ordinal, not a value.
            let expr = if let ExprKind::Int(n) = &item.expr.kind {
                usize::try_from(*n)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|i| outputs.get(i).cloned())
                    .ok_or_else(|| {
                        SqlError::new(
                            108,
                            16,
                            1,
                            format!("The ORDER BY position number {n} is out of range."),
                        )
                    })?
            } else {
                map_expr_columns(&item.expr, &|name: &Name| {
                    if name.value.contains('.') {
                        return None;
                    }
                    aliases
                        .iter()
                        .find(|(alias, _)| name.eq_ignore_case(alias))
                        .map(|(_, expr)| (*expr).clone())
                })
            };
            Ok(OrderItem {
                expr,
                descending: item.descending,
            })
        })
        .collect()
}

/// The select list as one source-evaluable expression per *output* column, so a
/// positional `ORDER BY <n>` can name what it points at. A wildcard expands to
/// its source columns, each referenced by qualifier where it has one — `a.v`
/// rather than `v` — so a join with a repeated column name stays unambiguous.
pub(super) fn output_exprs(items: &[SelectItem], scope: &JoinScope) -> Vec<Expr> {
    // Synthetic: built from the scope, so it resolves by construction and its
    // span is never surfaced in an error.
    let synthetic = Span::new(0, 0);
    let column_expr = |index: usize| {
        let (qualifier, column) = &scope.columns[index];
        let value = match qualifier {
            Some(q) => format!("{q}.{column}"),
            None => column.clone(),
        };
        Expr {
            span: synthetic,
            kind: ExprKind::Column(Name {
                value,
                quoted: false,
                span: synthetic,
            }),
        }
    };
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => out.extend((0..scope.columns.len()).map(column_expr)),
            SelectItem::QualifiedWildcard(qualifier) => out.extend(
                scope
                    .indices_for_qualifier(&qualifier.value)
                    .into_iter()
                    .map(column_expr),
            ),
            SelectItem::Expr { expr, .. } => out.push(expr.clone()),
            // An assignment SELECT produces no result columns to order by.
            SelectItem::Assign { .. } => {}
        }
    }
    out
}

/// Replaces every column reference in an expression via `f` (a replacement, or
/// `None` to keep), not descending into nested subquery bodies (but mapping an
/// `IN (SELECT)` operand, which is at this scope).
pub(super) fn map_expr_columns(expr: &Expr, f: &impl Fn(&Name) -> Option<Expr>) -> Expr {
    let map = |e: &Expr| map_expr_columns(e, f);
    let map_box = |e: &Expr| Box::new(map_expr_columns(e, f));
    let kind = match &expr.kind {
        ExprKind::Column(name) => match f(name) {
            Some(replacement) => return replacement,
            None => expr.kind.clone(),
        },
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_)
        | ExprKind::Subquery(_)
        | ExprKind::Exists(_) => expr.kind.clone(),
        ExprKind::InSubquery {
            expr: e,
            subquery,
            negated,
        } => ExprKind::InSubquery {
            expr: map_box(e),
            subquery: subquery.clone(),
            negated: *negated,
        },
        ExprKind::Unary { op, expr: e } => ExprKind::Unary {
            op: *op,
            expr: map_box(e),
        },
        ExprKind::IsNull { expr: e, negated } => ExprKind::IsNull {
            expr: map_box(e),
            negated: *negated,
        },
        ExprKind::Cast { expr: e, target } => ExprKind::Cast {
            expr: map_box(e),
            target: target.clone(),
        },
        ExprKind::Binary { op, left, right } => ExprKind::Binary {
            op: *op,
            left: map_box(left),
            right: map_box(right),
        },
        ExprKind::Like {
            expr: e,
            pattern,
            escape,
            negated,
        } => ExprKind::Like {
            expr: map_box(e),
            pattern: map_box(pattern),
            escape: *escape,
            negated: *negated,
        },
        ExprKind::InList {
            expr: e,
            list,
            negated,
        } => ExprKind::InList {
            expr: map_box(e),
            list: list.iter().map(map).collect(),
            negated: *negated,
        },
        ExprKind::Between {
            expr: e,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: map_box(e),
            low: map_box(low),
            high: map_box(high),
            negated: *negated,
        },
        ExprKind::Function { name, args } => ExprKind::Function {
            name: name.clone(),
            args: args.iter().map(map).collect(),
        },
        ExprKind::Aggregate {
            func,
            distinct,
            arg,
        } => ExprKind::Aggregate {
            func: *func,
            distinct: *distinct,
            arg: arg.as_ref().map(|a| map_box(a)),
        },
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => ExprKind::Case {
            operand: operand.as_ref().map(|o| map_box(o)),
            branches: branches.iter().map(|(w, r)| (map(w), map(r))).collect(),
            else_result: else_result.as_ref().map(|e| map_box(e)),
        },
    };
    Expr {
        kind,
        span: expr.span,
    }
}

/// A copy of `subquery` with references to the enclosing query's columns (per
/// `outer`) replaced by the current outer row's literal values — making a
/// correlated subquery uncorrelated for that row. `None` if the inner scope
/// cannot be determined; the caller then runs the subquery unchanged.
pub(super) fn substitute_subquery_outer_refs(
    storage: &Storage,
    db_id: u32,
    subquery: &Select,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
) -> Option<Select> {
    let inner = subquery_inner_scope(storage, db_id, subquery)?;
    let substitute = |name: &Name| -> Option<Expr> {
        if inner.matches_any(&name.value) {
            return None; // the subquery's own column wins (even if ambiguous)
        }
        let index = outer(&name.value)?;
        Some(Expr {
            kind: ExprKind::Literal(outer_row.get(index)?.clone()),
            span: name.span,
        })
    };
    // An outer reference INSIDE an aggregate argument has outer-aggregate
    // semantics in SQL Server (the aggregate computes over the OUTER group);
    // substituting a per-row literal would silently compute something else.
    // Bail — the subquery runs unchanged and errors cleanly.
    let mut outer_in_agg = false;
    select_aggregate_arg_refs(subquery, &mut |name| {
        if !inner.matches_any(&name.value) && outer(&name.value).is_some() {
            outer_in_agg = true;
        }
    });
    if outer_in_agg {
        return None;
    }
    let mut out = subquery.clone();
    out.where_clause = out
        .where_clause
        .as_ref()
        .map(|e| map_expr_columns(e, &substitute));
    out.items = out
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Expr { expr, alias } => SelectItem::Expr {
                expr: map_expr_columns(expr, &substitute),
                alias: alias.clone(),
            },
            other => other.clone(),
        })
        .collect();
    out.having = out
        .having
        .as_ref()
        .map(|e| map_expr_columns(e, &substitute));
    out.group_by = out
        .group_by
        .iter()
        .map(|e| map_expr_columns(e, &substitute))
        .collect();
    out.order_by = out
        .order_by
        .iter()
        .map(|o| OrderItem {
            expr: map_expr_columns(&o.expr, &substitute),
            descending: o.descending,
        })
        .collect();
    // A correlated reference INSIDE a derived table's body lives in `from`,
    // not in any expression above — descend and substitute there too. The
    // recursive call's own inner-scope check handles shadowing.
    if let Some(from) = out.from.as_mut() {
        substitute_from_outer_refs(storage, db_id, from, outer, outer_row)?;
    }
    Some(out)
}

/// Substitutes outer references inside every derived-table body of a FROM
/// tree. `None` when any derived body's scope cannot be determined.
pub(super) fn substitute_from_outer_refs(
    storage: &Storage,
    db_id: u32,
    from: &mut TableRef,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
) -> Option<()> {
    match from {
        TableRef::Table { .. } => Some(()),
        TableRef::Join { left, right, .. } => {
            substitute_from_outer_refs(storage, db_id, left, outer, outer_row)?;
            substitute_from_outer_refs(storage, db_id, right, outer, outer_row)
        }
        TableRef::Derived { subquery, .. } => {
            **subquery =
                substitute_subquery_outer_refs(storage, db_id, subquery, outer, outer_row)?;
            Some(())
        }
        // A TVF's body lives in the catalog (bound at expansion, not here) and
        // its literal arguments carry no outer references.
        TableRef::Function { .. } => Some(()),
    }
}

/// Evaluates each correlated subquery in `expr` against `outer_row` (binding the
/// enclosing query's columns per `outer`) and replaces it with a literal —
/// producing a subquery-free predicate for that outer row.
/// A [`ColumnResolver`] over a bare name→index closure (the `outer` resolver the
/// correlated-substitution pass carries), so a user scalar function's arguments
/// can be evaluated against the current row.
pub(super) struct FnResolver<'a>(&'a dyn Fn(&str) -> Option<usize>);

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
pub(super) fn resolve_scalar_function(
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
pub(super) fn expr_has_user_function(storage: &Storage, db_id: u32, expr: &Expr) -> bool {
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
pub(super) fn expr_needs_binding(storage: &Storage, db_id: u32, expr: &Expr) -> bool {
    expr_has_subquery(expr) || expr_has_user_function(storage, db_id, expr)
}

pub(super) fn substitute_correlated_in_expr(
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

/// Whether a WHERE/ON predicate keeps a row. The predicate must be
/// boolean-typed (SQL Server 4145): a bare numeric/string expression is
/// rejected rather than silently coerced, and UNKNOWN drops the row (3VL).
pub(super) fn where_keeps(
    predicate: &Expr,
    row: &[SqlValue],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<bool, SqlError> {
    match eval::eval(predicate, row, resolver, eval_ctx)? {
        SqlValue::Bool(b) => Ok(b),
        SqlValue::Null => Ok(false),
        _ => Err(SqlError::new(
            4145,
            15,
            1,
            "An expression of non-boolean type specified in a context where a condition is expected, near 'WHERE'.",
        )
        .at(predicate.span)),
    }
}

pub(super) fn exec_select(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    // A top-level assignment SELECT is routed to exec_select_assign; one reaching
    // here has been nested in a subquery / derived table / CTE, which is invalid.
    if select
        .items
        .iter()
        .any(|i| matches!(i, SelectItem::Assign { .. }))
    {
        return Err(SqlError::message_only(
            141,
            "A SELECT that assigns to a variable cannot be used inside a query expression.",
        ));
    }
    // A single-table scan whose every output column comes from the schema needs
    // no stage that waits for the whole input, so it runs row by row instead.
    // The gate goes before the CTE expansion and the subquery rewrite because it
    // excludes both — and the rewrite would otherwise clone the whole statement
    // and run any subquery eagerly.
    if let Some(plan) = scan_plan(storage, select, eval_ctx) {
        return scan_select(storage, &plan, select, eval_ctx);
    }

    // Inline any WITH common table expressions (as derived tables) first.
    let expanded;
    let select = if select.ctes.is_empty() {
        select
    } else {
        expanded = expand_ctes(select);
        &expanded
    };
    // Resolve each (uncorrelated) subquery once, up front, replacing it with a
    // literal / boolean / value-list so the rest of execution is subquery-free.
    let rewritten = rewrite_select_subqueries(storage, select, eval_ctx)?;
    let select = &rewritten;

    let source = build_source(
        storage,
        select.from.as_ref(),
        &select.where_clause,
        eval_ctx,
    )?;
    let resolver = source.scope();
    let types = source.types();

    // WHERE. The predicate must be boolean-typed (SQL Server 4145): a bare
    // numeric/string expression is rejected rather than silently coerced. Any
    // subquery left in the (already-rewritten) predicate is correlated: bind the
    // outer row into it and evaluate per row.
    let where_correlated = select
        .where_clause
        .as_ref()
        .is_some_and(|w| expr_needs_binding(storage, eval_ctx.database_id, w));
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    // One row: filter it into `rows` or drop it. Shared by both input shapes.
    let take = |row: Vec<Datum>, rows: &mut Vec<Vec<Datum>>| -> Result<(), SqlError> {
        check_cancelled()?;
        let sql_row = row_values(&row, &types);
        let keep = match &select.where_clause {
            None => true,
            Some(predicate) => {
                let bound;
                let predicate = if where_correlated {
                    bound = substitute_correlated_in_expr(
                        storage,
                        predicate,
                        &|name| resolver.resolve(name),
                        &sql_row,
                        eval_ctx,
                    )?;
                    &bound
                } else {
                    predicate
                };
                where_keeps(predicate, &sql_row, &resolver, eval_ctx)?
            }
        };
        if keep {
            rows.push(row);
        }
        Ok(())
    };
    // A scanned base table streams in slices: peak input memory is one slice
    // plus the survivors, not the table (Stage 8 streaming scans). Everything
    // downstream (aggregate/sort/join operators) bounds or spills its own
    // working set, so a filtered pipeline is bounded end to end.
    match source.rows {
        SourceRows::Materialized(input) => {
            for row in input {
                take(row, &mut rows)?;
            }
        }
        SourceRows::Scan(mut stream) => {
            while let Some(slice) = stream.next_slice(storage)? {
                for row in slice {
                    take(row, &mut rows)?;
                }
            }
        }
    }

    // A grouped/aggregated or DISTINCT query projects first (its ORDER BY
    // references the output), while a plain query orders the source rows so it
    // can order by columns that are not in the SELECT list.
    if aggregate::is_aggregated(select) || select.distinct {
        let mut out = if aggregate::is_aggregated(select) {
            aggregate::execute(storage, select, &rows, &types, &resolver, eval_ctx)?
        } else {
            project(
                storage,
                &select.items,
                &source.columns,
                &rows,
                &types,
                &resolver,
                eval_ctx,
            )?
        };
        if select.distinct {
            // Each output column's collation (resolved back to its source column;
            // a computed/aliased column has no source column → the case-
            // insensitive default), so DISTINCT honours an explicit `_CS`/`_BIN`
            // column exactly like GROUP BY / COUNT(DISTINCT) do.
            let out_sens: Vec<CollationSensitivity> = out
                .columns
                .iter()
                .map(|c| {
                    resolver
                        .resolve(&c.name)
                        .map(|i| resolver.collation(i))
                        .unwrap_or(CollationSensitivity::default_collation())
                })
                .collect();
            dedup_rows(storage, &mut out, &out_sens)?;
        }
        order_output(storage, &mut out, &select.order_by, eval_ctx)?;
        if let Some(top) = select.top {
            out.rows.truncate(top as usize);
        }
        return Ok(out);
    }

    // ORDER BY (evaluated against the source row; stable so equal keys keep
    // input order). Spills to temp extents when the input exceeds the budget.
    if !select.order_by.is_empty() {
        let order_by = order_by_with_aliases(&select.order_by, &select.items, &resolver)?;
        rows = order_rows(
            storage,
            rows,
            &order_by,
            &types,
            &source.collations,
            &resolver,
            eval_ctx,
        )?;
    }

    // TOP.
    if let Some(top) = select.top {
        rows.truncate(top as usize);
    }

    project(
        storage,
        &select.items,
        &source.columns,
        &rows,
        &types,
        &resolver,
        eval_ctx,
    )
}

#[cfg(test)]
thread_local! {
    /// Test hook: makes [`scan_plan`] decline everything, so a test can run one
    /// query down both paths and compare. Thread-local, not a `static`: a batch
    /// runs on one thread, and the suite runs its tests in parallel in a single
    /// binary — a global would force every other test's queries onto the
    /// collecting path for as long as this one was set.
    static FORCE_COLLECTING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn force_collecting() -> bool {
    FORCE_COLLECTING.with(|c| c.get())
}

/// Runs `f` with [`scan_select`]'s path disabled, so `f`'s queries take the
/// collecting path. Restores on drop, so a panicking `f` cannot leave the flag
/// set for the next test to run on this thread.
#[cfg(test)]
pub(crate) fn without_scan_path<R>(f: impl FnOnce() -> R) -> R {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            FORCE_COLLECTING.with(|c| c.set(false));
        }
    }
    FORCE_COLLECTING.with(|c| c.set(true));
    let _restore = Restore;
    f()
}

/// A `SELECT` that can be answered a row at a time: one base table, scanned,
/// filtered and projected without any stage that must see the whole input
/// first. Produced by [`scan_plan`], consumed by [`scan_select`].
pub(super) struct ScanPlan {
    /// The base table's database — the namespace the scan reads it from.
    db_id: u32,
    /// The base table's catalog name — what the scan reads.
    table: String,
    /// How to read it: the planner's choice, made once (see [`scan_plan`]).
    pub(super) access: plan::AccessPath,
    /// The schema columns this query reads at all — its projection plus the
    /// WHERE clause's — ascending and distinct. Everything below is expressed
    /// in *these* coordinates, not the table's: the storage layer decodes only
    /// these, so a scanned row has exactly this width.
    needed: Vec<usize>,
    /// Type of each needed column, parallel to `needed`.
    types: Vec<ColumnType>,
    /// Resolves the WHERE clause's column references against a scanned row.
    resolver: JoinScope,
    /// Output columns. Every type here is the schema's, which is what makes the
    /// shape work: a computed column's type comes from `infer_type` over every
    /// value in it, so it cannot be known until the last row has been seen.
    pub(super) columns: Vec<ResultColumn>,
    /// The scanned-row position each output column reads (an index into
    /// `needed`, not into the table's columns).
    picks: Vec<usize>,
    /// An index seek that is *covering*: every needed column's original value
    /// is stored in the index leaves (`INCLUDE`), so the scan answers from the
    /// index alone — no per-row base-table lookup. Never true for a table
    /// scan.
    pub(super) covering: bool,
}

/// Recognises the shape [`scan_select`] can run, or `None` for everything else
/// — which then takes the collecting path unchanged.
///
/// Every rejection here is a stage that cannot answer until it has the whole
/// input: `ORDER BY` sorts it, `DISTINCT` dedups it, an aggregate folds it, a
/// computed column types it, and a join/derived table/CTE/view is another query
/// underneath. An `IndexSeek` is excluded for the opposite reason — it reads
/// *less* than a scan, and reading the whole table to filter it down would
/// trade the planner's work for this one's.
pub(super) fn scan_plan(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Option<ScanPlan> {
    #[cfg(test)]
    if force_collecting() {
        return None;
    }
    if !select.ctes.is_empty()
        || select.distinct
        || !select.order_by.is_empty()
        || aggregate::is_aggregated(select)
    {
        return None;
    }
    // `TOP 0` wants no rows, so this path would never evaluate the WHERE — and
    // the engine reports an unresolvable column (207) or a non-boolean predicate
    // (4145) from that evaluation, having no separate binding pass. Reading a
    // table to discard all of it is not worth answering an invalid query with an
    // empty result set, so the degenerate case stays on the collecting path.
    if select.top == Some(0) {
        return None;
    }
    // An uncorrelated subquery is executed by the rewrite this path skips; a
    // correlated one runs a query per row. (A subquery in the SELECT list is
    // already excluded: it is not a bare column.) A user scalar function in the
    // WHERE needs the same rewrite, so decline it here too.
    if select
        .where_clause
        .as_ref()
        .is_some_and(|w| expr_needs_binding(storage, eval_ctx.database_id, w))
    {
        return None;
    }
    // Whether every output column *could* be a source column, which is a
    // property of the syntax alone. Deciding it before the catalog is read keeps
    // `SELECT id + 1 FROM t` from paying for a table definition, a schema and a
    // resolver it is only going to discard.
    if !select.items.iter().all(|item| {
        matches!(
            item,
            SelectItem::Wildcard
                | SelectItem::QualifiedWildcard(_)
                | SelectItem::Expr {
                    expr: Expr {
                        kind: ExprKind::Column(_),
                        ..
                    },
                    ..
                }
        )
    }) {
        return None;
    }
    let Some(TableRef::Table { name, alias }) = select.from.as_ref() else {
        return None;
    };
    // The `sys.*` virtual tables build their rows in Rust and have no cursor to
    // scan. They are matched by their full name *before* catalog resolution, so
    // this check has to come first for the same reason: `resolve_table` strips a
    // schema prefix, so it would answer `sys.tables` with a user table called
    // `tables`.
    if is_sys_view(&name.value) {
        return None;
    }
    let def = resolve_table(storage, eval_ctx.database_id, &name.value)?;
    // A view is its own SELECT, expanded as a derived table — and its TableDef
    // has no columns and a `root_page` of 0, so a wildcard over it would project
    // nothing and the scan would read the catalog root instead of the view. A
    // PROCEDURE/FUNCTION/TRIGGER has the same empty shape and must not stream as
    // an empty table: the collecting path rejects it (2809/208).
    if def.view_query.is_some() || def.is_procedure() || def.is_function() || def.is_trigger() {
        return None;
    }
    // If SELECT is denied, fall back to the collecting path, which resolves the
    // same table through `build_table_source` and raises the 229 there — keeping
    // the check on the one path the executor uses to touch the object.
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Select).ok()?;
    let schema = def.schema().ok()?;

    let qualifier = alias
        .as_ref()
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    let source: Vec<ResultColumn> = schema
        .columns
        .iter()
        .map(|c| ResultColumn {
            name: c.name.clone(),
            column_type: c.column_type,
        })
        .collect();
    let collations: Vec<Option<String>> =
        schema.columns.iter().map(|c| c.collation.clone()).collect();
    // The full-width scope, used to plan the projection and resolve the WHERE's
    // references. What the scan actually runs on is the pruned scope built
    // below, once the needed columns are known.
    let resolver = JoinScope {
        columns: source
            .iter()
            .map(|c| (Some(qualifier.clone()), c.name.clone()))
            .collect(),
        collations: collations.clone(),
    };

    // The projection plan, mirroring `project`'s: every item must resolve to a
    // source column, so the output's types are the schema's.
    let mut columns = Vec::new();
    let mut picks = Vec::new();
    for item in &select.items {
        match item {
            SelectItem::Wildcard => {
                for (index, column) in source.iter().enumerate() {
                    picks.push(index);
                    columns.push(column.clone());
                }
            }
            SelectItem::QualifiedWildcard(q) => {
                let indices = resolver.indices_for_qualifier(&q.value);
                // Unbound (4104): leave the error to the collecting path.
                if indices.is_empty() {
                    return None;
                }
                for index in indices {
                    picks.push(index);
                    columns.push(source[index].clone());
                }
            }
            SelectItem::Expr { expr, alias } => {
                let index = bare_column_index(expr, &resolver)?;
                let name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| bare_column_name(expr))
                    .unwrap_or_default();
                picks.push(index);
                columns.push(ResultColumn {
                    name,
                    column_type: source[index].column_type,
                });
            }
            // Rejected before projection on both paths.
            SelectItem::Assign { .. } => return None,
        }
    }

    // Projection pruning. The query reads the columns it projects plus the ones
    // its WHERE names, and nothing else — so those are the only ones the storage
    // layer decodes, and a scanned row is exactly that wide. A character column
    // costs a `String` allocation to decode, so on a wide table this is most of
    // the per-row work.
    //
    // A WHERE reference that does not resolve is simply not collected: it is not
    // a column of this table, so there is nothing to decode for it, and `eval`
    // still reports it (207) against the same resolver as before.
    let mut needed = picks.clone();
    let mut where_columns = Vec::new();
    if let Some(predicate) = &select.where_clause {
        collect_column_refs(predicate, &mut where_columns);
    }
    needed.extend(
        where_columns
            .iter()
            .filter_map(|name| resolver.resolve(name)),
    );
    needed.sort_unstable();
    needed.dedup();

    // The same access path `build_table_source` would take (it passes no
    // `needed`, so its choice can differ only toward a covering index — and it
    // never reaches this shape). Choosing here, rather than declining a seek,
    // is what keeps this gate free for the queries it rejects: a decline would
    // have thrown away the definition, the schema and this choice, and
    // `build_table_source` would compute all three again. Chosen after
    // `needed` is known so a covering index can win its tie (see
    // [`plan::choose`]).
    // The row count is a statistic (one buffer-pool-cached page read),
    // fetched only when choose() can use it (it returns a scan outright
    // without a predicate or indexes).
    let row_count = if def.indexes.is_empty() || select.where_clause.is_none() {
        None
    } else {
        storage.rel_row_count(def.database_id, &def.name)
    };
    let access = plan::choose(
        &def,
        &schema,
        &select.where_clause,
        eval_ctx,
        Some(&needed),
        row_count,
    );

    // Everything downstream now speaks in the scanned row's coordinates.
    let position = |index: usize| {
        needed
            .binary_search(&index)
            .expect("a needed column is in `needed`")
    };
    let picks = picks.into_iter().map(position).collect();
    let types = needed.iter().map(|&i| source[i].column_type).collect();
    let resolver = JoinScope {
        columns: needed
            .iter()
            .map(|&i| (Some(qualifier.clone()), source[i].name.clone()))
            .collect(),
        collations: needed.iter().map(|&i| collations[i].clone()).collect(),
    };

    // A seek covers when every column the query reads is INCLUDEd in the
    // index — original values in the leaf, since the key bytes are one-way
    // collation sort keys and cannot serve.
    let covering = match &access {
        plan::AccessPath::IndexSeek {
            index_object_id, ..
        } => def
            .indexes
            .iter()
            .find(|i| i.object_id == *index_object_id)
            .is_some_and(|i| needed.iter().all(|c| i.include.contains(c))),
        plan::AccessPath::TableScan => false,
    };

    Some(ScanPlan {
        db_id: def.database_id,
        table: def.name,
        access,
        needed,
        types,
        resolver,
        columns,
        picks,
        covering,
    })
}

/// Every column name a predicate references, in no particular order (duplicates
/// included — the caller dedups after resolving).
///
/// Exhaustive by construction: no wildcard arm, so a new [`ExprKind`] is a
/// compile error here rather than a column silently left undecoded.
pub(super) fn collect_column_refs(expr: &Expr, out: &mut Vec<String>) {
    match &expr.kind {
        ExprKind::Column(name) => out.push(name.value.clone()),
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => {}
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => collect_column_refs(e, out),
        ExprKind::Binary { left, right, .. } => {
            collect_column_refs(left, out);
            collect_column_refs(right, out);
        }
        ExprKind::Like {
            expr: e, pattern, ..
        } => {
            collect_column_refs(e, out);
            collect_column_refs(pattern, out);
        }
        ExprKind::InList { expr: e, list, .. } => {
            collect_column_refs(e, out);
            for item in list {
                collect_column_refs(item, out);
            }
        }
        ExprKind::Between {
            expr: e, low, high, ..
        } => {
            collect_column_refs(e, out);
            collect_column_refs(low, out);
            collect_column_refs(high, out);
        }
        ExprKind::Function { args, .. } => {
            for arg in args {
                collect_column_refs(arg, out);
            }
        }
        ExprKind::Aggregate { arg, .. } => {
            if let Some(arg) = arg {
                collect_column_refs(arg, out);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_column_refs(operand, out);
            }
            for (when, then) in branches {
                collect_column_refs(when, out);
                collect_column_refs(then, out);
            }
            if let Some(else_result) = else_result {
                collect_column_refs(else_result, out);
            }
        }
        // The gate rejects a subquery before this runs.
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => {}
    }
}

/// The `sys.*` catalog views, which [`build_table_source`] answers by name
/// ahead of any catalog lookup.
pub(super) fn is_sys_view(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "sys.tables"
            | "sys.views"
            | "sys.sql_modules"
            | "sys.columns"
            | "sys.indexes"
            | "sys.check_constraints"
            | "sys.foreign_keys"
            | "sys.default_constraints"
            | "sys.databases"
            | "sys.dm_repl_replica_states"
            | "sys.dm_repl_slots"
            | "sys.configurations"
            | "sys.database_principals"
            | "sys.database_role_members"
            | "sys.database_permissions"
    )
}

/// Scans, filters and projects one base table a row at a time.
///
/// The collecting path builds the whole table into `Source.rows`, filters that
/// into a second vector, converts *every* row to `SqlValue` in `project`, and
/// projects into a third — four copies of the input alive at once, before TOP
/// has discarded any of them. Here a slice is the only input in hand, each row
/// is projected or dropped as it is read, and `TOP n` stops the scan rather
/// than truncating afterwards.
///
/// The result is still collected; what this drops is the *input's*
/// materialization, which is the part that has no upper bound. An index seek
/// keeps its input materialized — `rel_index_scan` has no cursor — so it gains
/// the per-row savings but not that one; a seek's candidate set is bounded by
/// the seek.
///
/// `TOP n` therefore stops the scan without evaluating the predicate on rows
/// past the nth kept one, where the collecting path evaluated every source row
/// before truncating. A predicate that errors on one of those rows (a divide by
/// zero, an overflow) now goes unraised — which is SQL Server's behaviour, whose
/// Top operator likewise stops asking its child for rows.
pub(super) fn scan_select(
    storage: &Storage,
    plan: &ScanPlan,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    let mut out = RowSet {
        columns: plan.columns.clone(),
        rows: Vec::new(),
    };
    scan_select_rows(storage, plan, select, eval_ctx, &mut |row| {
        out.rows.push(row);
    })?;
    Ok(out)
}

/// Rows per [`BatchEmitter::rows`] chunk on the streamed scan path: enough to
/// amortize the per-event cost, small enough that the statement's peak memory
/// is a chunk, not the result.
pub(super) const STREAM_CHUNK_ROWS: usize = 256;

/// The streamed shape of [`scan_select`]: opens the result set, then emits
/// kept rows in [`STREAM_CHUNK_ROWS`] chunks as the scan produces them, so the
/// client sees rows while the scan is still running. On a mid-scan error the
/// full chunks already emitted stand — the caller closes the set (see
/// [`BatchRun::abort_open_rowset`]) — and the partial chunk is dropped.
pub(super) fn scan_select_streamed(
    storage: &Storage,
    plan: &ScanPlan,
    select: &Select,
    eval_ctx: &EvalContext,
    run: &mut BatchRun<'_>,
) -> Result<u64, SqlError> {
    run.open_rowset(plan.columns.clone());
    let mut chunk: Vec<Vec<Datum>> = Vec::new();
    let kept = scan_select_rows(storage, plan, select, eval_ctx, &mut |row| {
        chunk.push(row);
        if chunk.len() >= STREAM_CHUNK_ROWS {
            run.rows(std::mem::take(&mut chunk));
        }
    })?;
    run.rows(chunk);
    Ok(kept)
}

/// Walks the plan's access path, filters, projects, and hands each kept row to
/// `sink`, stopping once `TOP` is satisfied. Returns the number of rows kept
/// (which `TOP` counts, matching the collecting path's truncation of the
/// filtered rows — `TOP 0` never reaches here, the gate declines it). Both
/// executions of the scan shape ride this walk: [`scan_select`] collects into
/// a `RowSet`, [`scan_select_streamed`] emits chunks as slices are read.
pub(super) fn scan_select_rows(
    storage: &Storage,
    plan: &ScanPlan,
    select: &Select,
    eval_ctx: &EvalContext,
    sink: &mut dyn FnMut(Vec<Datum>),
) -> Result<u64, SqlError> {
    #[cfg(test)]
    storage.count_scan_select();
    let types = &plan.types;
    let mut kept: u64 = 0;
    let enough = |kept: u64| select.top.is_some_and(|top| kept >= top);

    // One row: filter it, and project it or drop it. `Ok(false)` once TOP is
    // satisfied and the caller should stop reading.
    let mut take = |row: Vec<Datum>| -> Result<bool, SqlError> {
        check_cancelled()?;
        if let Some(predicate) = &select.where_clause {
            let sql_row = row_values(&row, types);
            if !where_keeps(predicate, &sql_row, &plan.resolver, eval_ctx)? {
                return Ok(true);
            }
        }
        sink(plan.picks.iter().map(|i| row[*i].clone()).collect());
        kept += 1;
        Ok(!enough(kept))
    };

    match &plan.access {
        plan::AccessPath::TableScan => {
            if let Some(snapshot) = current_snapshot() {
                // A versioned reader holds no table lock, so the sliced
                // cursor's between-slice contract does not hold for it; the
                // snapshot scan reads the table atomically and merges the
                // version store.
                let rows = storage
                    .rel_scan_snapshot(plan.db_id, &plan.table, Some(&plan.needed), snapshot)
                    .map_err(|err| map_storage_err(err, &plan.table))?;
                for row in rows {
                    if !take(row)? {
                        break;
                    }
                }
            } else {
                let mut cursor = ScanCursor::start();
                let mut slice: Vec<Vec<Datum>> = Vec::new();
                'scan: while !cursor.done() {
                    cursor = storage
                        .rel_scan_slice(
                            plan.db_id,
                            &plan.table,
                            cursor,
                            SCAN_SLICE_ROWS,
                            Some(&plan.needed),
                            &mut slice,
                        )
                        .map_err(|err| map_storage_err(err, &plan.table))?;
                    for row in slice.drain(..) {
                        if !take(row)? {
                            break 'scan;
                        }
                    }
                }
            }
        }
        plan::AccessPath::IndexSeek {
            index_object_id,
            lower,
            upper,
            ..
        } => {
            // The seek narrows the candidates; the predicate still re-checks
            // each one, so the result matches a full scan.
            let rows = storage
                .rel_index_scan(
                    plan.db_id,
                    &plan.table,
                    *index_object_id,
                    lower.clone(),
                    upper.clone(),
                    Some(&plan.needed),
                    plan.covering,
                    current_snapshot(),
                )
                .map_err(|err| map_storage_err(err, &plan.table))?;
            for row in rows {
                if !take(row)? {
                    break;
                }
            }
        }
    }
    Ok(kept)
}

/// `SELECT @a = expr, @b = expr2 [FROM ...]` — an assignment SELECT. The value
/// expressions are projected as an ordinary result set; each variable then
/// takes the value from the *last* row the query produces (SQL Server's
/// documented behaviour for the final value). Zero rows leave the variables
/// unchanged. A value that reads a variable being assigned in the same
/// statement (running aggregation, cross-referencing targets) is rejected
/// rather than evaluated against the pre-statement snapshot, which would give a
/// result that silently differs from SQL Server's per-row assignment.
pub(super) fn exec_select_assign(
    storage: &Storage,
    select: &Select,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // Every target must be a declared variable; capture their declared types.
    let mut targets: Vec<(String, ColumnType)> = Vec::with_capacity(select.items.len());
    for item in &select.items {
        let SelectItem::Assign { target, .. } = item else {
            // The dispatcher only routes here when every item is an assignment.
            unreachable!("assignment SELECT has a non-assignment item");
        };
        let column_type = txn_ctx
            .variables
            .get(target)
            .map(|(t, _)| *t)
            .ok_or_else(|| undeclared_variable_err(target))?;
        targets.push((target.clone(), column_type));
    }

    // Every value is evaluated against the variables' pre-statement values, so a
    // value that references a variable being assigned here would silently
    // diverge from SQL Server's per-row / left-to-right assignment (running
    // aggregation, cross-referencing targets). Reject those rather than compute
    // a wrong result; the caller can use SET or a set-based aggregate instead.
    let target_names: std::collections::HashSet<&str> =
        targets.iter().map(|(name, _)| name.as_str()).collect();
    for item in &select.items {
        let SelectItem::Assign { value, .. } = item else {
            unreachable!()
        };
        if expr_uses_local_var(value, &target_names) {
            return Err(SqlError::message_only(
                141,
                "An assignment SELECT cannot reference a variable it is assigning in the same statement; use SET or a set-based aggregate.",
            ));
        }
    }

    // Project the value expressions as an ordinary result set.
    let projected = Select {
        items: select
            .items
            .iter()
            .map(|item| {
                let SelectItem::Assign { value, .. } = item else {
                    unreachable!()
                };
                SelectItem::Expr {
                    expr: value.clone(),
                    alias: None,
                }
            })
            .collect(),
        ..select.clone()
    };
    let rowset = exec_select(storage, &projected, &txn_ctx.eval_context())?;

    // Assign the last row's values (SQL Server: the variable holds the value
    // from the final row). No rows -> variables keep their current values.
    if let Some(last) = rowset.rows.last() {
        for (index, (name, column_type)) in targets.iter().enumerate() {
            let produced = value::datum_to_sql(&last[index], &rowset.columns[index].column_type);
            let datum = value::sql_to_datum(&produced, column_type, name)?;
            let coerced = value::datum_to_sql(&datum, column_type);
            txn_ctx
                .variables
                .insert(name.clone(), (*column_type, coerced));
        }
    }
    // SQL Server counts the rows an assignment SELECT processed: the DONE
    // carries it and `@@ROWCOUNT` reports it.
    Ok(StatementResult::RowsAffected(rowset.rows.len() as u64))
}

/// Removes duplicate output rows (SELECT DISTINCT), keeping first occurrence.
/// NULLs are equal to each other (`Datum` equality), matching SQL Server.
pub(super) fn dedup_rows(
    storage: &Storage,
    rowset: &mut RowSet,
    sensitivities: &[CollationSensitivity],
) -> Result<(), SqlError> {
    // Hash-based DISTINCT — O(n) instead of the old O(n²) linear scan. Each
    // output column is single-typed (projection coerced it), so `HashKey`'s
    // `order_key_cmp` equality agrees with the former `Vec<Datum>` equality for
    // every realistic input. (Edge: two `float` NaN rows now collapse to one —
    // `order_key_cmp` treats NaN as equal, like GROUP BY already did — where the
    // old raw `Datum` `==` kept them distinct.)
    let types: Vec<ColumnType> = rowset.columns.iter().map(|c| c.column_type).collect();
    // DISTINCT folds string columns by each output column's collation
    // (`sensitivities`, parallel to the columns), so a `_CI` column folds case
    // and a `_CS`/`_BIN` column stays exact — consistent with GROUP BY and
    // COUNT(DISTINCT). `dedup_key` keeps the original row for output.
    let dedup_key = |row: &[Datum]| hash::fold_hash_key(&row_values(row, &types), sensitivities);
    let approx: usize = rowset.rows.iter().map(|r| approx_row_bytes(r)).sum();
    if approx <= sort_budget() {
        // In-memory: keep first-appearance order (DISTINCT without ORDER BY has
        // no guaranteed order, but this is the least-surprising small-set result).
        let mut seen: std::collections::HashSet<hash::HashKey> = std::collections::HashSet::new();
        rowset
            .rows
            .retain(|row| seen.insert(hash::HashKey(dedup_key(row))));
        return Ok(());
    }

    // Grace-hash DISTINCT: partition rows by key hash into temp extents (equal
    // rows share a partition), then dedup each partition in memory. The per-
    // partition dedup set is bounded to ~one partition instead of the whole
    // input. Output is by partition (immaterial — a spilling DISTINCT is not
    // order-sensitive; any ORDER BY runs afterward).
    let partitions = (approx / sort_budget() + 1).max(2);
    let partition_of = |key: &[SqlValue]| -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        hash::HashKey(key.to_vec()).hash(&mut hasher);
        (hasher.finish() % partitions as u64) as usize
    };
    let spill_err = |e| map_storage_err(e, "<distinct spill>");
    let mut parts: Vec<_> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    for row in &rowset.rows {
        // Partition by the *folded* key so case-insensitive-equal rows land in
        // the same partition (else a cross-partition duplicate is missed); the
        // stored row stays original for output.
        let key = dedup_key(row);
        parts[partition_of(&key)]
            .write_row(row)
            .map_err(spill_err)?;
    }
    for part in parts.iter_mut() {
        part.finish_writing().map_err(spill_err)?;
    }
    let mut out: Vec<Vec<Datum>> = Vec::new();
    for part in parts.iter_mut() {
        let mut seen: std::collections::HashSet<hash::HashKey> = std::collections::HashSet::new();
        let mut reader = part.reader();
        while let Some(row) = reader.next_row().map_err(spill_err)? {
            if seen.insert(hash::HashKey(dedup_key(&row))) {
                out.push(row);
            }
        }
    }
    rowset.rows = out;
    Ok(())
}

/// Orders an output RowSet by ORDER BY items referencing the output: a bare
/// integer is a 1-based output-column ordinal; any other expression is
/// evaluated against the output row (its columns are the resolver). Uses
/// code-point ordering (NULLs first), stable.
pub(super) fn order_output(
    storage: &Storage,
    rowset: &mut RowSet,
    order_by: &[OrderItem],
    eval_ctx: &EvalContext,
) -> Result<(), SqlError> {
    if order_by.is_empty() {
        return Ok(());
    }
    let names: Vec<String> = rowset.columns.iter().map(|c| c.name.clone()).collect();
    let scope = OutputScope { names };
    let types: Vec<ColumnType> = rowset.columns.iter().map(|c| c.column_type).collect();
    let mut keyed: Vec<(Vec<SqlValue>, usize)> = Vec::with_capacity(rowset.rows.len());
    for (index, row) in rowset.rows.iter().enumerate() {
        let sql_row = row_values(row, &types);
        let mut key = Vec::with_capacity(order_by.len());
        for item in order_by {
            let value = if let ExprKind::Int(n) = &item.expr.kind {
                let ordinal = usize::try_from(*n)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .filter(|&i| i < sql_row.len())
                    .ok_or_else(|| {
                        SqlError::new(
                            108,
                            16,
                            1,
                            format!("The ORDER BY position number {n} is out of range."),
                        )
                    })?;
                sql_row[ordinal].clone()
            } else {
                eval_maybe_bound(storage, &item.expr, &sql_row, &scope, eval_ctx)?
            };
            key.push(value);
        }
        keyed.push((key, index));
    }
    keyed.sort_by(|(ka, ia), (kb, ib)| {
        for (index, item) in order_by.iter().enumerate() {
            let mut ord = order_key_cmp(&ka[index], &kb[index]);
            if item.descending {
                ord = ord.reverse();
            }
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        ia.cmp(ib)
    });
    rowset.rows = keyed.iter().map(|(_, i)| rowset.rows[*i].clone()).collect();
    Ok(())
}

/// Per-query sort memory budget: a sort whose rows exceed this spills to temp
/// extents (external merge sort) rather than growing without bound.
pub(super) const SORT_MEMORY_BUDGET: usize = 64 * 1024 * 1024;

/// A row paired with its evaluated ORDER BY key, as carried through the sort.
pub(super) type KeyedRow = (Vec<SqlValue>, Vec<Datum>);

#[cfg(test)]
thread_local! {
    /// Test-only override that forces the external-sort spill path on small
    /// inputs (execution runs on the calling thread in tests).
    static TEST_SORT_BUDGET: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// The active sort memory budget (overridable in tests).
pub(super) fn sort_budget() -> usize {
    #[cfg(test)]
    if let Some(budget) = TEST_SORT_BUDGET.with(std::cell::Cell::get) {
        return budget;
    }
    SORT_MEMORY_BUDGET
}

/// Forces (or clears) the sort spill budget for the current test thread.
#[cfg(test)]
pub(crate) fn set_test_sort_budget(budget: Option<usize>) {
    TEST_SORT_BUDGET.with(|cell| cell.set(budget));
}

/// The ORDER BY comparator for one pair of pre-evaluated key tuples: per item,
/// collation-aware for a character column, else value order (NULLs first);
/// `descending` reverses. No tie-break here — the caller adds stability.
pub(super) fn compare_sort_keys(
    a: &[SqlValue],
    b: &[SqlValue],
    order_by: &[OrderItem],
    collators: &[Option<collation::Collation>],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (col, item) in order_by.iter().enumerate() {
        let ord = match (&collators[col], &a[col], &b[col]) {
            (Some(coll), SqlValue::Str(x), SqlValue::Str(y)) => coll.compare(x, y),
            (Some(_), SqlValue::Null, SqlValue::Null) => Ordering::Equal,
            (Some(_), SqlValue::Null, _) => Ordering::Less,
            (Some(_), _, SqlValue::Null) => Ordering::Greater,
            _ => order_key_cmp(&a[col], &b[col]),
        };
        let ord = if item.descending { ord.reverse() } else { ord };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Builds the per-item collators (only a bare character column is collation-
/// ordered; everything else uses value order).
pub(super) fn sort_collators(
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
) -> Vec<Option<collation::Collation>> {
    order_by
        .iter()
        .map(|item| {
            let index = bare_column_index(&item.expr, resolver)?;
            let is_char = matches!(
                types.get(index),
                Some(ColumnType::VarChar { .. }) | Some(ColumnType::NVarChar { .. })
            );
            if !is_char {
                return None;
            }
            let name = collations
                .get(index)
                .cloned()
                .flatten()
                .unwrap_or_else(|| collation::DEFAULT_COLLATION.to_string());
            Some(collation::Collation::from_name(&name))
        })
        .collect()
}

/// The ORDER BY key of one row.
/// Evaluate an expression that may hold a subquery or user scalar function: fold
/// it against `row` (its columns resolved by `resolver`) before the pure
/// evaluator, mirroring the SELECT-list / WHERE rewrite. Lets a UDF appear in
/// ORDER BY / join-ON / CHECK etc. rather than reaching the pure evaluator and
/// raising 195.
pub(super) fn eval_maybe_bound(
    storage: &Storage,
    expr: &Expr,
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
    eval_ctx: &EvalContext,
) -> Result<SqlValue, SqlError> {
    if expr_needs_binding(storage, eval_ctx.database_id, expr) {
        let outer = |name: &str| resolver.resolve(name);
        let bound = substitute_correlated_in_expr(storage, expr, &outer, row, eval_ctx)?;
        eval::eval(&bound, row, resolver, eval_ctx)
    } else {
        eval::eval(expr, row, resolver, eval_ctx)
    }
}

pub(super) fn sort_key(
    storage: &Storage,
    row: &[Datum],
    order_by: &[OrderItem],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Vec<SqlValue>, SqlError> {
    let values = row_values(row, types);
    order_by
        .iter()
        .map(|item| eval_maybe_bound(storage, &item.expr, &values, resolver, eval_ctx))
        .collect()
}

/// A rough in-memory byte estimate for a row, for the sort budget.
pub(super) fn approx_row_bytes(row: &[Datum]) -> usize {
    let payload: usize = row
        .iter()
        .map(|d| match d {
            Datum::VarChar(s) | Datum::NVarChar(s) => s.len() + 16,
            Datum::VarBinary(b) => b.len() + 16,
            _ => 16,
        })
        .sum();
    payload + 24
}

/// Sorts the (already WHERE-filtered) source rows by ORDER BY. Fits-in-budget
/// inputs sort in memory (Rust's stable `sort_by`); a larger input spills
/// sorted runs to temp extents and k-way merges them (external merge sort),
/// bounding the sort's working memory instead of erroring or doubling memory.
pub(super) fn order_rows(
    storage: &Storage,
    rows: Vec<Vec<Datum>>,
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Vec<Vec<Datum>>, SqlError> {
    order_rows_budgeted(
        storage,
        rows,
        order_by,
        types,
        collations,
        resolver,
        eval_ctx,
        sort_budget(),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn order_rows_budgeted<'a>(
    storage: &'a Storage,
    rows: Vec<Vec<Datum>>,
    order_by: &[OrderItem],
    types: &[ColumnType],
    collations: &[Option<String>],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
    budget: usize,
) -> Result<Vec<Vec<Datum>>, SqlError> {
    let collators = sort_collators(order_by, types, collations, resolver);
    let cmp = |a: &Vec<SqlValue>, b: &Vec<SqlValue>| compare_sort_keys(a, b, order_by, &collators);

    // Generate sorted runs, spilling a run to a `RowSpool` each time the
    // accumulated rows reach the budget. The final (in-memory) run is kept.
    let mut runs: Vec<crate::relstore::spill::RowSpool<'a>> = Vec::new();
    let mut current: Vec<KeyedRow> = Vec::new();
    let mut current_bytes = 0usize;
    for row in rows {
        check_cancelled()?;
        let key = sort_key(storage, &row, order_by, types, resolver, eval_ctx)?;
        current_bytes += approx_row_bytes(&row);
        current.push((key, row));
        if current_bytes >= budget {
            runs.push(sort_and_spill(storage, &mut current, &cmp)?);
            current_bytes = 0;
        }
    }
    // No spill: a plain stable in-memory sort.
    if runs.is_empty() {
        current.sort_by(|(a, _), (b, _)| cmp(a, b));
        return Ok(current.into_iter().map(|(_, row)| row).collect());
    }
    // Sort the final partial run and merge every run.
    current.sort_by(|(a, _), (b, _)| cmp(a, b));
    merge_runs(
        storage, &runs, current, order_by, types, resolver, eval_ctx, &collators,
    )
}

/// Stably sorts `run` in place and writes its rows (in sorted order) to a fresh
/// `RowSpool`, clearing `run`.
pub(super) fn sort_and_spill<'a>(
    storage: &'a Storage,
    run: &mut Vec<KeyedRow>,
    cmp: &impl Fn(&Vec<SqlValue>, &Vec<SqlValue>) -> std::cmp::Ordering,
) -> Result<crate::relstore::spill::RowSpool<'a>, SqlError> {
    run.sort_by(|(a, _), (b, _)| cmp(a, b));
    let mut spool = crate::relstore::spill::RowSpool::new(storage);
    for (_, row) in run.drain(..) {
        spool
            .write_row(&row)
            .map_err(|e| map_storage_err(e, "<sort spill>"))?;
    }
    spool
        .finish_writing()
        .map_err(|e| map_storage_err(e, "<sort spill>"))?;
    Ok(spool)
}

/// K-way merges the sorted spilled `runs` and the sorted in-memory `tail` run
/// into one sorted row vector. Keys are recomputed per row on read (cheap for
/// column refs); ties prefer the earlier run so the merge is globally stable
/// (spilled runs hold earlier input rows than the in-memory tail).
#[allow(clippy::too_many_arguments)]
pub(super) fn merge_runs(
    storage: &Storage,
    runs: &[crate::relstore::spill::RowSpool<'_>],
    tail: Vec<KeyedRow>,
    order_by: &[OrderItem],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
    collators: &[Option<collation::Collation>],
) -> Result<Vec<Vec<Datum>>, SqlError> {
    // One cursor per source: spilled-run readers first, then the in-memory tail.
    let mut readers: Vec<_> = runs.iter().map(|r| r.reader()).collect();
    let mut tail_iter = tail.into_iter();

    // Current head (key + row) of each source, in the same order.
    let source_count = readers.len() + 1;
    let mut heads: Vec<Option<(Vec<SqlValue>, Vec<Datum>)>> = Vec::with_capacity(source_count);
    for reader in &mut readers {
        heads.push(read_head(
            storage, reader, order_by, types, resolver, eval_ctx,
        )?);
    }
    heads.push(tail_iter.next());

    let total: usize = runs.iter().map(|r| r.row_count() as usize).sum::<usize>() + heads.len();
    let mut out: Vec<Vec<Datum>> = Vec::with_capacity(total);
    loop {
        // Pick the smallest head; on a key tie, the earliest source (lowest
        // index) wins, which preserves input order across runs.
        let mut best: Option<usize> = None;
        for (i, head) in heads.iter().enumerate() {
            let Some((key, _)) = head else { continue };
            match best {
                None => best = Some(i),
                Some(b) => {
                    let (bkey, _) = heads[b].as_ref().unwrap();
                    if compare_sort_keys(key, bkey, order_by, collators) == std::cmp::Ordering::Less
                    {
                        best = Some(i);
                    }
                }
            }
        }
        let Some(i) = best else { break };
        let (_, row) = heads[i].take().unwrap();
        out.push(row);
        // Advance the chosen source.
        heads[i] = if i < readers.len() {
            read_head(
                storage,
                &mut readers[i],
                order_by,
                types,
                resolver,
                eval_ctx,
            )?
        } else {
            tail_iter.next()
        };
    }
    Ok(out)
}

/// Reads the next row from a spool reader and pairs it with its ORDER BY key.
pub(super) fn read_head(
    storage: &Storage,
    reader: &mut crate::relstore::spill::RowSpoolReader,
    order_by: &[OrderItem],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<Option<KeyedRow>, SqlError> {
    match reader
        .next_row()
        .map_err(|e| map_storage_err(e, "<sort spill>"))?
    {
        Some(row) => {
            let key = sort_key(storage, &row, order_by, types, resolver, eval_ctx)?;
            Ok(Some((key, row)))
        }
        None => Ok(None),
    }
}

pub(super) fn project(
    storage: &Storage,
    items: &[SelectItem],
    source_columns: &[ResultColumn],
    rows: &[Vec<Datum>],
    types: &[ColumnType],
    resolver: &JoinScope,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    // Output column plan: a source column (typed, pass-through) or a
    // computed expression (evaluated then typed by inference).
    enum Proj<'a> {
        SourceColumn { index: usize, name: String },
        Expr { name: String, expr: &'a Expr },
    }
    let mut projs = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard => {
                for (index, column) in source_columns.iter().enumerate() {
                    projs.push(Proj::SourceColumn {
                        index,
                        name: column.name.clone(),
                    });
                }
            }
            SelectItem::QualifiedWildcard(qualifier) => {
                let indices = resolver.indices_for_qualifier(&qualifier.value);
                if indices.is_empty() {
                    return Err(SqlError::new(
                        4104,
                        16,
                        1,
                        format!(
                            "The multi-part identifier \"{}.*\" could not be bound.",
                            qualifier.value
                        ),
                    )
                    .at(qualifier.span));
                }
                for index in indices {
                    projs.push(Proj::SourceColumn {
                        index,
                        name: source_columns[index].name.clone(),
                    });
                }
            }
            SelectItem::Expr { expr, alias } => {
                let name = alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .or_else(|| bare_column_name(expr))
                    .unwrap_or_default();
                match bare_column_index(expr, resolver) {
                    // A bare column still carries its resolved output name so an
                    // `AS alias` (or the referenced name's casing) is preserved.
                    Some(index) => projs.push(Proj::SourceColumn { index, name }),
                    None => projs.push(Proj::Expr { name, expr }),
                }
            }
            // Assignment SELECTs are rewritten to Expr items before projection.
            SelectItem::Assign { .. } => {
                unreachable!("assignment SELECT handled before projection")
            }
        }
    }

    // Precompute all row values once for expression evaluation.
    let row_sql: Vec<Vec<SqlValue>> = rows.iter().map(|r| row_values(r, types)).collect();

    let mut columns = Vec::with_capacity(projs.len());
    let mut out_rows: Vec<Vec<Datum>> = vec![Vec::with_capacity(projs.len()); rows.len()];
    for proj in &projs {
        match proj {
            Proj::SourceColumn { index, name } => {
                columns.push(ResultColumn {
                    name: name.clone(),
                    column_type: source_columns[*index].column_type,
                });
                for (out, row) in out_rows.iter_mut().zip(rows) {
                    out.push(row[*index].clone());
                }
            }
            Proj::Expr { name, expr } => {
                // Evaluate the column for every row, then infer one type. A
                // subquery still present here is correlated (the rewrite pass
                // left it for the per-row bind): substitute the outer row's
                // values in, making it uncorrelated for that row.
                let correlated = expr_needs_binding(storage, eval_ctx.database_id, expr);
                let mut values = Vec::with_capacity(rows.len());
                for row in &row_sql {
                    let bound;
                    let expr = if correlated {
                        bound = substitute_correlated_in_expr(
                            storage,
                            expr,
                            &|name| resolver.resolve(name),
                            row,
                            eval_ctx,
                        )?;
                        &bound
                    } else {
                        expr
                    };
                    values.push(eval::eval(expr, row, resolver, eval_ctx)?);
                }
                let column_type = value::infer_type(&values);
                for (out, value) in out_rows.iter_mut().zip(&values) {
                    // Coerce each value to the inferred column type (e.g. all
                    // decimals to the widest scale) so the column is uniform.
                    out.push(value::sql_to_datum(value, &column_type, name)?);
                }
                columns.push(ResultColumn {
                    name: name.clone(),
                    column_type,
                });
            }
        }
    }
    Ok(RowSet {
        columns,
        rows: out_rows,
    })
}

pub(super) fn bare_column_name(expr: &Expr) -> Option<String> {
    match &expr.kind {
        // A qualified `t.col` reference outputs the bare column name.
        ExprKind::Column(name) => Some(name.value.rsplit('.').next().unwrap_or("").to_string()),
        _ => None,
    }
}

pub(super) fn bare_column_index(expr: &Expr, scope: &JoinScope) -> Option<usize> {
    match &expr.kind {
        ExprKind::Column(name) => scope.resolve(&name.value),
        _ => None,
    }
}

/// Collects every base-table name referenced in a FROM join tree, recursing
/// into derived-table subqueries so their tables are locked too. (Used for the
/// SHOWPLAN table list; [`collect_locked_tables`] is the lock-set collector.)
pub(super) fn collect_table_names<'a>(tref: &'a TableRef, out: &mut Vec<&'a Name>) {
    match tref {
        TableRef::Table { name, .. } => out.push(name),
        TableRef::Join { left, right, .. } => {
            collect_table_names(left, out);
            collect_table_names(right, out);
        }
        TableRef::Derived { subquery, .. } => {
            if let Some(from) = &subquery.from {
                collect_table_names(from, out);
            }
        }
        TableRef::Function { name, .. } => out.push(name),
    }
}

/// Collects every base table a SELECT reads for the lock set: its FROM tree
/// (including derived-table subqueries and join `ON` clauses) plus every
/// subquery embedded in its expressions (WHERE/SELECT list/HAVING/GROUP BY/
/// ORDER BY). Recurses through nested subqueries.
pub(super) fn collect_locked_tables<'a>(select: &'a Select, out: &mut Vec<&'a Name>) {
    // CTE bodies read their base tables like any derived table — collected
    // HERE, not left to callers' inlining passes: a condition subquery's
    // `WITH` has no expansion pass before lock analysis, and a missed table
    // is a read with no lock under up-front 2PL (the review's finding). A
    // CTE's own name may land in `out` via FROM references; it resolves to
    // no object and locks nothing, which is correct.
    for cte in &select.ctes {
        collect_locked_tables(&cte.query, out);
    }
    if let Some(from) = &select.from {
        collect_from_tables(from, out);
    }
    for item in &select.items {
        match item {
            SelectItem::Expr { expr, .. } | SelectItem::Assign { value: expr, .. } => {
                collect_expr_tables(expr, out)
            }
            SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {}
        }
    }
    for expr in select.where_clause.iter().chain(select.having.iter()) {
        collect_expr_tables(expr, out);
    }
    for expr in &select.group_by {
        collect_expr_tables(expr, out);
    }
    for item in &select.order_by {
        collect_expr_tables(&item.expr, out);
    }
}

/// Collects base tables from a FROM tree, recursing into derived subqueries and
/// join `ON` predicates (which may contain their own subqueries).
pub(super) fn collect_from_tables<'a>(tref: &'a TableRef, out: &mut Vec<&'a Name>) {
    match tref {
        // A `@t` table variable is session-local: it takes no lock and — via
        // statement_reads_tables — must not arm a snapshot, so it is never
        // collected as a locked/snapshotted table.
        TableRef::Table { name, .. } if name.value.starts_with('@') => {}
        TableRef::Table { name, .. } => out.push(name),
        TableRef::Join {
            left, right, on, ..
        } => {
            collect_from_tables(left, out);
            collect_from_tables(right, out);
            if let Some(on) = on {
                collect_expr_tables(on, out);
            }
        }
        TableRef::Derived { subquery, .. } => collect_locked_tables(subquery, out),
        // A TVF in FROM: its name resolves (via read_lock_object_ids) to the
        // tables its body reads, and its arguments may embed subqueries.
        TableRef::Function { name, args, .. } => {
            out.push(name);
            for arg in args {
                collect_expr_tables(arg, out);
            }
        }
    }
}

/// Collects base tables from every subquery embedded in an expression.
/// True if `expr` references any of the named local variables (`@name`, given
/// without the leading `@`), descending into subqueries. Used to reject an
/// assignment SELECT whose value reads a variable it is assigning.
pub(super) fn expr_uses_local_var(expr: &Expr, names: &std::collections::HashSet<&str>) -> bool {
    match &expr.kind {
        ExprKind::LocalVar(name) => names.contains(name.as_str()),
        ExprKind::Subquery(select) | ExprKind::Exists(select) => {
            select_uses_local_var(select, names)
        }
        ExprKind::InSubquery { expr, subquery, .. } => {
            expr_uses_local_var(expr, names) || select_uses_local_var(subquery, names)
        }
        ExprKind::Unary { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Cast { expr, .. } => expr_uses_local_var(expr, names),
        ExprKind::Binary { left, right, .. } => {
            expr_uses_local_var(left, names) || expr_uses_local_var(right, names)
        }
        ExprKind::Like { expr, pattern, .. } => {
            expr_uses_local_var(expr, names) || expr_uses_local_var(pattern, names)
        }
        ExprKind::InList { expr, list, .. } => {
            expr_uses_local_var(expr, names) || list.iter().any(|e| expr_uses_local_var(e, names))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expr_uses_local_var(expr, names)
                || expr_uses_local_var(low, names)
                || expr_uses_local_var(high, names)
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|o| expr_uses_local_var(o, names))
                || branches
                    .iter()
                    .any(|(w, t)| expr_uses_local_var(w, names) || expr_uses_local_var(t, names))
                || else_result
                    .as_ref()
                    .is_some_and(|e| expr_uses_local_var(e, names))
        }
        ExprKind::Function { args, .. } => args.iter().any(|a| expr_uses_local_var(a, names)),
        ExprKind::Aggregate { arg, .. } => {
            arg.as_ref().is_some_and(|a| expr_uses_local_var(a, names))
        }
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_) => false,
    }
}

/// True if any expression in `select` references one of the named local
/// variables (descends the SELECT list, WHERE/HAVING, GROUP BY, and ORDER BY).
pub(super) fn select_uses_local_var(
    select: &Select,
    names: &std::collections::HashSet<&str>,
) -> bool {
    let item_uses = select.items.iter().any(|item| match item {
        SelectItem::Expr { expr, .. } | SelectItem::Assign { value: expr, .. } => {
            expr_uses_local_var(expr, names)
        }
        SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => false,
    });
    item_uses
        || select
            .where_clause
            .iter()
            .chain(select.having.iter())
            .chain(select.group_by.iter())
            .any(|e| expr_uses_local_var(e, names))
        || select
            .order_by
            .iter()
            .any(|o| expr_uses_local_var(&o.expr, names))
}

pub(super) fn collect_expr_tables<'a>(expr: &'a Expr, out: &mut Vec<&'a Name>) {
    match &expr.kind {
        ExprKind::Subquery(select) | ExprKind::Exists(select) => collect_locked_tables(select, out),
        ExprKind::InSubquery { expr, subquery, .. } => {
            collect_expr_tables(expr, out);
            collect_locked_tables(subquery, out);
        }
        ExprKind::Unary { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Cast { expr, .. } => collect_expr_tables(expr, out),
        ExprKind::Binary { left, right, .. } => {
            collect_expr_tables(left, out);
            collect_expr_tables(right, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_expr_tables(expr, out);
            collect_expr_tables(pattern, out);
        }
        ExprKind::InList { expr, list, .. } => {
            collect_expr_tables(expr, out);
            list.iter().for_each(|e| collect_expr_tables(e, out));
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_expr_tables(expr, out);
            collect_expr_tables(low, out);
            collect_expr_tables(high, out);
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_expr_tables(o, out);
            }
            for (when, then) in branches {
                collect_expr_tables(when, out);
                collect_expr_tables(then, out);
            }
            if let Some(e) = else_result {
                collect_expr_tables(e, out);
            }
        }
        ExprKind::Function { args, .. } => args.iter().for_each(|a| collect_expr_tables(a, out)),
        ExprKind::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                collect_expr_tables(a, out);
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
        | ExprKind::LocalVar(_) => {}
    }
}

/// Collects, as OWNED names, the read-lock targets an expression reaches: the
/// tables its subqueries reference and the (user or built-in) functions it
/// calls. Unlike [`collect_expr_tables`], the results do not borrow the input,
/// so they can be gathered from a separately-parsed scalar-function body — the
/// key to locking a table-reading function's inner reads up front under 2PL.
/// Built-in function names collected here resolve to nothing and are harmless.
pub(super) fn collect_expr_read_names(
    expr: &Expr,
    tables: &mut Vec<String>,
    funcs: &mut Vec<String>,
) {
    match &expr.kind {
        ExprKind::Function { name, args } => {
            funcs.push(name.clone());
            args.iter()
                .for_each(|a| collect_expr_read_names(a, tables, funcs));
        }
        ExprKind::Subquery(select) | ExprKind::Exists(select) => {
            collect_select_read_names(select, tables, funcs)
        }
        ExprKind::InSubquery { expr, subquery, .. } => {
            collect_expr_read_names(expr, tables, funcs);
            collect_select_read_names(subquery, tables, funcs);
        }
        ExprKind::Unary { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Cast { expr, .. } => collect_expr_read_names(expr, tables, funcs),
        ExprKind::Binary { left, right, .. } => {
            collect_expr_read_names(left, tables, funcs);
            collect_expr_read_names(right, tables, funcs);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_expr_read_names(expr, tables, funcs);
            collect_expr_read_names(pattern, tables, funcs);
        }
        ExprKind::InList { expr, list, .. } => {
            collect_expr_read_names(expr, tables, funcs);
            list.iter()
                .for_each(|e| collect_expr_read_names(e, tables, funcs));
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_expr_read_names(expr, tables, funcs);
            collect_expr_read_names(low, tables, funcs);
            collect_expr_read_names(high, tables, funcs);
        }
        ExprKind::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                collect_expr_read_names(a, tables, funcs);
            }
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_expr_read_names(o, tables, funcs);
            }
            for (w, r) in branches {
                collect_expr_read_names(w, tables, funcs);
                collect_expr_read_names(r, tables, funcs);
            }
            if let Some(e) = else_result {
                collect_expr_read_names(e, tables, funcs);
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
        | ExprKind::LocalVar(_) => {}
    }
}

/// Owned read-name collection over a SELECT (see [`collect_expr_read_names`]).
pub(super) fn collect_select_read_names(
    select: &Select,
    tables: &mut Vec<String>,
    funcs: &mut Vec<String>,
) {
    for cte in &select.ctes {
        collect_select_read_names(&cte.query, tables, funcs);
    }
    if let Some(from) = &select.from {
        collect_from_read_names(from, tables, funcs);
    }
    for item in &select.items {
        if let SelectItem::Expr { expr, .. } | SelectItem::Assign { value: expr, .. } = item {
            collect_expr_read_names(expr, tables, funcs);
        }
    }
    for expr in select
        .where_clause
        .iter()
        .chain(select.having.iter())
        .chain(select.group_by.iter())
    {
        collect_expr_read_names(expr, tables, funcs);
    }
    for item in &select.order_by {
        collect_expr_read_names(&item.expr, tables, funcs);
    }
}

pub(super) fn collect_from_read_names(
    tref: &TableRef,
    tables: &mut Vec<String>,
    funcs: &mut Vec<String>,
) {
    match tref {
        // A `@t` table variable is session-local — no lock, no snapshot.
        TableRef::Table { name, .. } if name.value.starts_with('@') => {}
        TableRef::Table { name, .. } => tables.push(name.value.clone()),
        TableRef::Join {
            left, right, on, ..
        } => {
            collect_from_read_names(left, tables, funcs);
            collect_from_read_names(right, tables, funcs);
            if let Some(on) = on {
                collect_expr_read_names(on, tables, funcs);
            }
        }
        TableRef::Derived { subquery, .. } => collect_select_read_names(subquery, tables, funcs),
        // A TVF in FROM: push the name into `funcs` so select_function_read_ids
        // recurses its body (the owned-collector twin of collect_from_tables).
        TableRef::Function { name, args, .. } => {
            funcs.push(name.value.clone());
            for arg in args {
                collect_expr_read_names(arg, tables, funcs);
            }
        }
    }
}

/// Owned read-name collection over a scalar function body's statement (its
/// reads come only from expressions — a data-returning statement is rejected at
/// CREATE, 444).
pub(super) fn collect_statement_read_names(
    statement: &Statement,
    tables: &mut Vec<String>,
    funcs: &mut Vec<String>,
) {
    match statement {
        Statement::Return {
            value: Some(expr), ..
        } => collect_expr_read_names(expr, tables, funcs),
        Statement::Set(SetStatement::Variable { value, .. }) => {
            collect_expr_read_names(value, tables, funcs)
        }
        // An assignment SELECT in a function body reads its FROM tables.
        Statement::Select(select) => collect_select_read_names(select, tables, funcs),
        // A multi-statement TVF body's `INSERT @t SELECT …` / `INSERT @t VALUES
        // (subquery)` reads real tables through its source, which must be locked
        // up front. The @t target itself is session-local (no lock).
        Statement::Insert(insert) => match &insert.source {
            InsertSource::Select(select) => collect_select_read_names(select, tables, funcs),
            InsertSource::Values(rows) => {
                for row in rows {
                    for expr in row {
                        collect_expr_read_names(expr, tables, funcs);
                    }
                }
            }
        },
        Statement::Declare(declarations) => {
            for decl in declarations {
                if let Some(init) = &decl.initializer {
                    collect_expr_read_names(init, tables, funcs);
                }
            }
        }
        Statement::If {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_expr_read_names(condition, tables, funcs);
            collect_statement_read_names(then_branch, tables, funcs);
            if let Some(e) = else_branch {
                collect_statement_read_names(e, tables, funcs);
            }
        }
        Statement::While {
            condition, body, ..
        } => {
            collect_expr_read_names(condition, tables, funcs);
            collect_statement_read_names(body, tables, funcs);
        }
        Statement::Block { body, .. } => {
            for inner in body {
                collect_statement_read_names(inner, tables, funcs);
            }
        }
        _ => {}
    }
}

/// A table's exposed name: its alias, else its (schema-stripped) name.
pub(super) fn exposed_name(name: &Name, alias: Option<&Name>) -> String {
    alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string())
}

/// Collects the exposed names of every table in a FROM tree. A derived table's
/// exposed name is its alias (its inner tables are not exposed to the outer
/// query).
pub(super) fn collect_exposed_names(tref: &TableRef, out: &mut Vec<String>) {
    match tref {
        TableRef::Table { name, alias } => out.push(exposed_name(name, alias.as_ref())),
        TableRef::Join { left, right, .. } => {
            collect_exposed_names(left, out);
            collect_exposed_names(right, out);
        }
        TableRef::Derived { alias, .. } => out.push(alias.value.clone()),
        TableRef::Function { name, alias, .. } => out.push(exposed_name(name, alias.as_ref())),
    }
}

/// Rejects a FROM clause with duplicate exposed table names / correlation
/// names (SQL Server 1013), which would otherwise bind ambiguously.
pub(super) fn check_exposed_names(from: &TableRef) -> Result<(), SqlError> {
    let mut names = Vec::new();
    collect_exposed_names(from, &mut names);
    for i in 0..names.len() {
        for j in (i + 1)..names.len() {
            if names[i].eq_ignore_ascii_case(&names[j]) {
                return Err(SqlError::new(
                    1013,
                    16,
                    1,
                    format!(
                        "The objects \"{}\" and \"{}\" in the FROM clause have the same exposed names. Use correlation names to distinguish them.",
                        names[i], names[j]
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn build_source(
    storage: &Storage,
    from: Option<&TableRef>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    if let Some(from) = from {
        check_exposed_names(from)?;
    }
    build_source_inner(storage, from, where_clause, eval_ctx)
}

pub(super) fn build_source_inner(
    storage: &Storage,
    from: Option<&TableRef>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    match from {
        None => Ok(Source {
            // No FROM: one row, no columns (constant SELECT).
            columns: Vec::new(),
            qualifiers: Vec::new(),
            collations: Vec::new(),
            rows: SourceRows::Materialized(vec![Vec::new()]),
        }),
        // A single top-level table may use the WHERE for an index seek; base
        // tables inside a join scan fully (join-order planning is later).
        Some(TableRef::Table { name, alias }) => {
            build_table_source(storage, name, alias.as_ref(), where_clause, eval_ctx)
        }
        Some(join) => build_join(storage, join, eval_ctx),
    }
}

/// SQL Server caps view/function nesting at 32 levels; a deeper chain (or a view
/// cycle) errors here rather than overflowing the stack.
pub(super) const MAX_VIEW_NESTING: u32 = 32;

thread_local! {
    /// Current view-expansion depth on this worker thread (each batch runs on
    /// one thread, so a thread-local is per-request).
    static VIEW_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII guard that increments the view-nesting depth on `enter` and restores it
/// on drop (including the error/`?` paths), erroring past [`MAX_VIEW_NESTING`].
pub(super) struct ViewDepthGuard;

impl ViewDepthGuard {
    fn enter(view_name: &str) -> Result<Self, SqlError> {
        let depth = VIEW_DEPTH.with(|d| d.get());
        if depth >= MAX_VIEW_NESTING {
            return Err(SqlError::message_only(
                436,
                format!(
                    "View '{view_name}' exceeds the maximum view nesting level of {MAX_VIEW_NESTING} (possibly a view cycle)."
                ),
            ));
        }
        VIEW_DEPTH.with(|d| d.set(depth + 1));
        Ok(ViewDepthGuard)
    }
}

impl Drop for ViewDepthGuard {
    fn drop(&mut self) {
        VIEW_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// True where object-permission checks apply — not inside an OWNED stored-object
/// body (procedure, function, TVF, view, or trigger), whose reads are covered by
/// ownership chaining (all objects share the single `dbo` owner today), so the
/// caller's permission on the body suffices (the grant-EXECUTE-only pattern).
/// Dynamic SQL (`sp_executesql`) resets `CHAIN_DEPTH`, so it is checked here even
/// when nested in a procedure — matching SQL Server, which does not chain
/// through dynamic SQL.
pub(super) fn at_top_level() -> bool {
    CHAIN_DEPTH.with(|d| d.get()) == 0 && VIEW_DEPTH.with(|d| d.get()) == 0
}

/// Whether `sec` permits `action` on an object with these permission entries.
/// A matching DENY for any of the session's principals wins (DENY beats GRANT);
/// otherwise a matching GRANT permits; otherwise denied (no implicit grant).
pub(super) fn permits(
    perms: &[PermissionEntry],
    sec: &SecurityContext,
    action: PermAction,
) -> bool {
    let mut granted = false;
    for entry in perms {
        if entry.action == action && sec.principals.contains(&entry.grantee) {
            if entry.deny {
                return false;
            }
            granted = true;
        }
    }
    granted
}

/// Enforces `action` on the resolved object `def`, erroring 229 if the session
/// lacks the permission. A no-op for a bypassing session (sysadmin / dbo /
/// internal) and inside any stored-object body (ownership chaining).
pub(super) fn enforce_object_permission(
    storage: &Storage,
    def: &TableDef,
    sec: &SecurityContext,
    action: PermAction,
) -> Result<(), SqlError> {
    if sec.bypass || !at_top_level() || permits(&def.permissions, sec, action) {
        return Ok(());
    }
    Err(SqlError::new(
        229,
        14,
        5,
        format!(
            "The {} permission was denied on the object '{}', database '{}', schema 'dbo'.",
            action.name(),
            def.name,
            database_name_of(storage, def.database_id)
        ),
    ))
}

/// Builds the row source for one base table (or `sys.*` view), stamping every
/// column with the table's qualifier (its alias, else its name).
pub(super) fn build_table_source(
    storage: &Storage,
    name: &Name,
    alias: Option<&Name>,
    where_clause: &Option<Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let qualifier = alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    // A `@t` table variable: serve its in-memory rows as a materialized source.
    // (The catalog resolver never matches an `@`-name, so this is the only path
    // that handles it — and it never touches Storage.)
    if name.value.starts_with('@') {
        let tv = current_table_var(&name.value)
            .ok_or_else(|| must_declare_table_var(&name.value).at(name.span))?;
        let count = tv.schema.columns.len();
        let columns = tv
            .schema
            .columns
            .iter()
            .map(|c| ResultColumn {
                name: c.name.clone(),
                column_type: c.column_type,
            })
            .collect();
        let collations = tv
            .schema
            .columns
            .iter()
            .map(|c| c.collation.clone())
            .collect();
        return Ok(Source {
            columns,
            qualifiers: vec![Some(qualifier); count],
            collations,
            rows: SourceRows::Materialized(tv.rows),
        });
    }
    // `inserted`/`deleted`: the firing trigger's pseudo-tables. Resolved before
    // the catalog so a real table named `inserted` cannot be reached from inside
    // a trigger body (SQL Server reserves them there too). Only matches when a
    // trigger scope is armed; otherwise falls through to catalog resolution.
    if let Some(source) = current_trigger_source(&name.value, &qualifier) {
        return Ok(source);
    }
    let base = match name.value.to_ascii_lowercase().as_str() {
        "sys.tables" => sys_tables(storage, eval_ctx.database_id),
        "sys.databases" => sys_databases(storage),
        "sys.dm_repl_replica_states" => sys_dm_repl_replica_states(storage),
        "sys.dm_repl_slots" => sys_dm_repl_slots(storage),
        "sys.configurations" => sys_configurations(),
        "sys.views" => sys_views(storage, eval_ctx.database_id),
        "sys.procedures" => sys_procedures(storage, eval_ctx.database_id),
        "sys.triggers" => sys_triggers(storage, eval_ctx.database_id),
        "sys.trigger_events" => sys_trigger_events(storage, eval_ctx.database_id),
        "sys.server_principals" => sys_server_principals(storage),
        "sys.sql_logins" => sys_sql_logins(storage),
        "sys.database_principals" => sys_database_principals(storage),
        "sys.database_role_members" => sys_database_role_members(storage),
        "sys.database_permissions" => sys_database_permissions(storage, eval_ctx.database_id),
        "sys.parameters" => sys_parameters(storage, eval_ctx.database_id),
        "sys.objects" => sys_objects(storage, eval_ctx.database_id),
        "sys.sql_modules" => sys_sql_modules(storage, eval_ctx.database_id),
        "sys.columns" => sys_columns(storage, eval_ctx.database_id),
        "sys.indexes" => sys_indexes(storage, eval_ctx.database_id),
        "sys.check_constraints" => sys_check_constraints(storage, eval_ctx.database_id),
        "sys.foreign_keys" => sys_foreign_keys(storage, eval_ctx.database_id),
        "sys.default_constraints" => sys_default_constraints(storage, eval_ctx.database_id),
        _ => {
            let def = resolve_table(storage, eval_ctx.database_id, &name.value)
                .ok_or_else(|| SqlError::invalid_object(&name.value).at(name.span))?;
            // A procedure is not a queryable object (SQL Server 2809).
            if def.is_procedure() {
                return Err(procedure_not_a_table(&def.name).at(name.span));
            }
            // A trigger is not a queryable object either — resolving it as a base
            // table would heap-scan its (empty) root page 0. 208 invalid object.
            if def.is_trigger() {
                return Err(SqlError::invalid_object(&name.value).at(name.span));
            }
            // A scalar function is not a rowset — it cannot appear in FROM.
            // (Table-valued functions, added later, expand here instead.)
            if def.is_function() {
                return Err(function_not_a_table(&def.name).at(name.span));
            }
            // SELECT permission on the base table or view (checked here, at the
            // top level, before a view body expands — the body's own reads are
            // covered by ownership chaining and not re-checked).
            enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Select)
                .map_err(|e| e.at(name.span))?;
            // A view: run its stored SELECT as a derived table under the view's
            // qualifier. A view over another view expands recursively — building
            // the derived source re-enters `build_table_source` for the inner
            // view — bounded by a nesting-depth guard that turns a view cycle
            // (self- or mutually-referential views) into a clean error instead
            // of a stack overflow.
            if let Some(query_text) = &def.view_query {
                let _guard = ViewDepthGuard::enter(&def.name)?;
                let body = parse_view_query(query_text, &def.name)?;
                let qual = Name {
                    value: qualifier,
                    quoted: false,
                    span: name.span,
                };
                // A view body is a stored-object scope, like a function/TVF
                // body: it must not read the CALLER's table variables. Shadow
                // the read view with an empty one so `SELECT ... FROM @t` inside
                // a view errors 1087 rather than returning caller rows. (An
                // in-statement derived table or CTE is NOT a separate scope and
                // keeps the statement's view — only stored bodies shadow.)
                let _table_var_scope = arm_table_var_view(&std::collections::HashMap::new());
                let _trigger_shadow = TriggerScope::clear();
                // The body's unqualified names are the VIEW's database's (a
                // cross-database view reads its own home, as SQL Server
                // resolves it) — matching collect_read_lock_ids' analysis.
                let mut view_ctx = eval_ctx.clone();
                view_ctx.database_id = def.database_id;
                return build_derived_source(storage, &body, &qual, &view_ctx);
            }
            let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
            // An index seek narrows the candidate set; the WHERE filter later
            // re-checks, so results match a full scan.
            // Fetched only when choose() can use it (it returns a scan
            // outright without a predicate or indexes).
            let row_count = if def.indexes.is_empty() || where_clause.is_none() {
                None
            } else {
                storage.rel_row_count(def.database_id, &def.name)
            };
            let rows = match plan::choose(&def, &schema, where_clause, eval_ctx, None, row_count) {
                // A scan is handed out LAZY: the consumer pulls slices, so a
                // filtering/folding reader holds one slice, not the table
                // (and the storage lock is still taken per slice, as before).
                // Under a read snapshot the scan materializes atomically
                // instead: a versioned reader holds no table lock, so the
                // sliced cursor's contract does not hold for it.
                plan::AccessPath::TableScan => match current_snapshot() {
                    Some(snapshot) => SourceRows::Materialized(
                        storage
                            .rel_scan_snapshot(def.database_id, &def.name, None, snapshot)
                            .map_err(|err| map_storage_err(err, &def.name))?,
                    ),
                    None => SourceRows::Scan(ScanStream {
                        db_id: def.database_id,
                        table: def.name.clone(),
                        cursor: ScanCursor::start(),
                    }),
                },
                plan::AccessPath::IndexSeek {
                    index_object_id,
                    lower,
                    upper,
                    ..
                } => SourceRows::Materialized(
                    storage
                        .rel_index_scan(
                            def.database_id,
                            &def.name,
                            index_object_id,
                            lower,
                            upper,
                            None,
                            false,
                            current_snapshot(),
                        )
                        .map_err(|err| map_storage_err(err, &def.name))?,
                ),
            };
            let columns = schema
                .columns
                .iter()
                .map(|c| ResultColumn {
                    name: c.name.clone(),
                    column_type: c.column_type,
                })
                .collect();
            let collations = schema.columns.iter().map(|c| c.collation.clone()).collect();
            Source {
                columns,
                qualifiers: Vec::new(),
                collations,
                rows,
            }
        }
    };
    let count = base.columns.len();
    Ok(Source {
        qualifiers: vec![Some(qualifier); count],
        ..base
    })
}

/// Expands an inline table-valued function call `dbo.f(args) [AS alias]` in a
/// FROM clause: binds the call's argument values to the function's `@params`,
/// then runs its stored body SELECT as a derived table under the call's
/// qualifier — a parameterized view. The body's table reads are locked and
/// snapshotted up front by the lock analysis and the snapshot-scope arming,
/// which both resolve the function name into its body (see collect_read_lock_ids
/// and statement_reads_tables); the body reads under the caller's ambient
/// snapshot on this thread. Recursion is bounded by the shared view-depth guard.
pub(super) fn build_function_source(
    storage: &Storage,
    name: &Name,
    args: &[Expr],
    alias: Option<&Name>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let def = resolve_table(storage, eval_ctx.database_id, &name.value)
        .ok_or_else(|| SqlError::invalid_object(&name.value).at(name.span))?;
    let function = def
        .function
        .as_ref()
        .ok_or_else(|| function_not_a_table(&def.name).at(name.span))?;
    // A table-valued function in FROM is read like a table: SELECT permission.
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Select)
        .map_err(|e| e.at(name.span))?;
    if args.len() < function.params.len() {
        return Err(SqlError::new(
            313,
            16,
            3,
            format!(
                "An insufficient number of arguments were supplied for the procedure or function {}.",
                def.name
            ),
        )
        .at(name.span));
    }
    if args.len() > function.params.len() {
        return Err(SqlError::new(
            8144,
            16,
            2,
            format!(
                "Procedure or function {} has too many arguments specified.",
                def.name
            ),
        )
        .at(name.span));
    }
    let qualifier = alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string());
    let qual = Name {
        value: qualifier,
        quoted: false,
        span: name.span,
    };
    match &function.returns {
        FunctionReturns::InlineTable { select_text } => {
            // Bind the arguments to the parameters, coercing to the declared
            // types, in a FRESH variable scope (a TVF body sees only its
            // parameters, not caller locals). Arguments may themselves contain
            // subqueries or scalar UDFs.
            let no_outer = |_: &str| -> Option<usize> { None };
            let mut variables = std::collections::HashMap::new();
            for (param, arg) in function.params.iter().zip(args) {
                let column_type = ColumnType::parse(&param.type_spec)
                    .map_err(|e| SqlError::message_only(245, e.to_string()))?;
                let value = substitute_correlated_in_expr(storage, arg, &no_outer, &[], eval_ctx)
                    .and_then(|bound| eval_constant(&bound, eval_ctx))?;
                let datum = value::sql_to_datum(&value, &column_type, &param.name)?;
                variables.insert(
                    param.name.clone(),
                    value::datum_to_sql(&datum, &column_type),
                );
            }
            let mut fn_ctx = eval_ctx.clone();
            fn_ctx.variables = variables;
            // The body's unqualified names are the FUNCTION's database's.
            fn_ctx.database_id = def.database_id;
            // Expand the body like a view (bounded by the shared nesting guard).
            let _guard = ViewDepthGuard::enter(&def.name)?;
            let body = parse_view_query(select_text, &def.name)?;
            // A TVF body sees only its parameters, not caller locals — the scalar
            // side is isolated above (fresh `variables`); do the same for the
            // table-variable read view. Without this the body's `FROM @t` would
            // resolve against the CALLER's table variable, since build_derived_
            // source runs under whatever scope the calling statement armed. An
            // empty view makes such a body error 1087, as SQL Server rejects it.
            let _table_var_scope = arm_table_var_view(&std::collections::HashMap::new());
            let _trigger_shadow = TriggerScope::clear();
            build_derived_source(storage, &body, &qual, &fn_ctx)
        }
        FunctionReturns::MultiStatementTable {
            returns_var,
            columns_text,
            body,
        } => run_multi_statement_tvf(
            storage,
            def.database_id,
            function,
            returns_var,
            columns_text,
            body,
            args,
            &qual,
            eval_ctx,
        ),
        // A scalar function called in table position is not a rowset.
        FunctionReturns::Scalar { .. } => Err(function_not_a_table(&def.name).at(name.span)),
    }
}

/// Runs a multi-statement TVF and returns its result table variable's rows as a
/// materialized source. The body runs in an isolated context (a fresh
/// `TxnContext`, like a scalar UDF: parameters only, no transaction, ambient
/// snapshot for its reads) seeded with the empty result table variable, which
/// its statements populate; the accumulated rows are the function's result.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_multi_statement_tvf(
    storage: &Storage,
    home_db_id: u32,
    function: &FunctionDef,
    returns_var: &str,
    columns_text: &str,
    body_text: &str,
    args: &[Expr],
    qual: &Name,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    // Rebuild the result table variable's schema (re-parsed per call, like the
    // body — the CREATE-time validation guarantees this succeeds).
    let (columns, primary_key) = truthdb_sql::parse_table_var_columns(columns_text)?;
    let (schema, key_columns, defaults) =
        build_table_var_definition(returns_var, &columns, &primary_key)?;
    // Fresh isolated scope: parameters only, caller session identity carried for
    // DB_NAME()/SUSER_SNAME()/USER_NAME()/@@SPID and role membership. Arguments
    // evaluate in the CALLER's context. The sids are left 0 (the body does not
    // re-resolve membership — it reuses the caller's already-computed role set).
    let mut txn_ctx = TxnContext::default();
    // The body's unqualified names resolve in the FUNCTION's home database;
    // DB_ID/DB_NAME keep working via the caller's databases snapshot.
    txn_ctx.set_session_identity(
        database_name_of(storage, home_db_id),
        home_db_id,
        eval_ctx.login.clone(),
        eval_ctx.spid,
        eval_ctx.user.clone(),
        0,
        0,
    );
    txn_ctx.databases_snapshot = eval_ctx.databases.clone();
    txn_ctx.session_server_roles = eval_ctx.server_roles.clone();
    txn_ctx.session_db_roles = eval_ctx.db_roles.clone();
    txn_ctx.security = eval_ctx.security.clone();
    let no_outer = |_: &str| -> Option<usize> { None };
    for (param, arg) in function.params.iter().zip(args) {
        let column_type = ColumnType::parse(&param.type_spec)
            .map_err(|e| SqlError::message_only(245, e.to_string()))?;
        let value = substitute_correlated_in_expr(storage, arg, &no_outer, &[], eval_ctx)
            .and_then(|bound| eval_constant(&bound, eval_ctx))?;
        let datum = value::sql_to_datum(&value, &column_type, &param.name)?;
        txn_ctx.variables.insert(
            param.name.clone(),
            (column_type, value::datum_to_sql(&datum, &column_type)),
        );
    }
    // Seed the empty result table variable; the body populates it.
    txn_ctx.table_variables.insert(
        returns_var.to_string(),
        TableVar {
            schema,
            key_columns,
            defaults,
            rows: Vec::new(),
        },
    );
    let statements = truthdb_sql::parse_table_function_body(body_text)?;
    // A multi-statement TVF called from a trigger body does not see
    // inserted/deleted.
    let _trigger_shadow = TriggerScope::clear();
    // A multi-statement TVF body ownership-chains: reads are not re-checked.
    let _chain = ChainGuard::enter();
    // Same nesting cap as a scalar UDF (217), decremented on every exit path.
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    let result = if depth > 32 {
        Err(SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        ))
    } else {
        let mut emitter = DiscardEmitter;
        let mut run = BatchRun {
            emitter: &mut emitter,
            deferred: Vec::new(),
            rowset_open: false,
            durability_failed: false,
            committed: false,
            last_error: None,
            // A multi-statement TVF's RETURN carries no value.
            function_return_type: None,
        };
        run_block(storage, &statements, &mut txn_ctx, &mut run, false).and_then(end_of_scope)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    result?;
    // The accumulated rows are the result. Serve them as a materialized source
    // stamped with the call's qualifier (identical shape to the @t FROM branch).
    let tv = txn_ctx
        .table_variables
        .get(returns_var)
        .expect("seeded above");
    let count = tv.schema.columns.len();
    let columns_out = tv
        .schema
        .columns
        .iter()
        .map(|c| ResultColumn {
            name: c.name.clone(),
            column_type: c.column_type,
        })
        .collect();
    let collations = tv
        .schema
        .columns
        .iter()
        .map(|c| c.collation.clone())
        .collect();
    Ok(Source {
        columns: columns_out,
        qualifiers: vec![Some(qual.value.clone()); count],
        collations,
        rows: SourceRows::Materialized(tv.rows.clone()),
    })
}

/// Recursively builds a join tree's combined row source (base tables scan
/// fully).
pub(super) fn build_join(
    storage: &Storage,
    tref: &TableRef,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    match tref {
        TableRef::Table { name, alias } => {
            build_table_source(storage, name, alias.as_ref(), &None, eval_ctx)
        }
        TableRef::Join {
            left,
            right,
            kind,
            on,
        } => {
            if matches!(kind, JoinKind::CrossApply | JoinKind::OuterApply) {
                return build_apply(
                    storage,
                    left,
                    right,
                    matches!(kind, JoinKind::OuterApply),
                    eval_ctx,
                );
            }
            let left = build_join(storage, left, eval_ctx)?;
            let right = build_join(storage, right, eval_ctx)?;
            join_sources(storage, left, right, *kind, on.as_ref(), eval_ctx)
        }
        TableRef::Derived { subquery, alias } => {
            build_derived_source(storage, subquery, alias, eval_ctx)
        }
        TableRef::Function { name, args, alias } => {
            build_function_source(storage, name, args, alias.as_ref(), eval_ctx)
        }
    }
}

/// `CROSS`/`OUTER APPLY`: the right side is re-evaluated once per left row,
/// correlated to it. For each left row the right `TableRef` is rebound to that
/// row's values (a TVF's arguments become literals; a derived table's outer
/// column references are substituted) and built; its rows are concatenated onto
/// the left row. CROSS APPLY drops a left row that produced none; OUTER APPLY
/// keeps it with NULLs for the right columns.
pub(super) fn build_apply(
    storage: &Storage,
    left: &TableRef,
    right: &TableRef,
    is_outer: bool,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let left_source = build_join(storage, left, eval_ctx)?;
    let left_types = left_source.types();
    let left_columns = left_source.columns.clone();
    let left_qualifiers = left_source.qualifiers.clone();
    let left_collations = left_source.collations.clone();
    // A resolver over the left columns so the right side's correlated references
    // (and TVF arguments) bind to the current left row.
    let left_scope = JoinScope {
        columns: left_qualifiers
            .iter()
            .zip(&left_columns)
            .map(|(q, c)| (q.clone(), c.name.clone()))
            .collect(),
        collations: left_collations.clone(),
    };
    let left_rows = left_source.rows.materialize(storage)?;

    let build_right_for = |vals: &[SqlValue]| -> Result<Source, SqlError> {
        let outer = |name: &str| left_scope.resolve(name);
        let bound = substitute_outer_in_tref(storage, right, &outer, vals, eval_ctx)?;
        build_join(storage, &bound, eval_ctx)
    };

    // (columns, qualifiers, collations) of the right source — learned from the
    // first built right and reused for the result's shape.
    type RightMeta = (Vec<ResultColumn>, Vec<Option<String>>, Vec<Option<String>>);
    let mut out_rows: Vec<Vec<Datum>> = Vec::new();
    let mut right_meta: Option<RightMeta> = None;
    for left_row in &left_rows {
        check_cancelled()?;
        let vals = row_values(left_row, &left_types);
        let right_source = build_right_for(&vals)?;
        let right_col_count = right_source.columns.len();
        if right_meta.is_none() {
            right_meta = Some((
                right_source.columns.clone(),
                right_source.qualifiers.clone(),
                right_source.collations.clone(),
            ));
        }
        let right_rows = right_source.rows.materialize(storage)?;
        if right_rows.is_empty() {
            if is_outer {
                let mut combined = left_row.clone();
                combined.extend(std::iter::repeat_n(Datum::Null, right_col_count));
                out_rows.push(combined);
            }
        } else {
            for rr in &right_rows {
                let mut combined = left_row.clone();
                combined.extend(rr.iter().cloned());
                out_rows.push(combined);
            }
        }
    }
    // With no left rows the right was never built; build it once against a NULL
    // left row to learn its column shape (the result is still zero rows).
    let (right_columns, right_qualifiers, right_collations) = match right_meta {
        Some(meta) => meta,
        None => {
            let nulls = vec![SqlValue::Null; left_columns.len()];
            let right_source = build_right_for(&nulls)?;
            (
                right_source.columns,
                right_source.qualifiers,
                right_source.collations,
            )
        }
    };

    let mut columns = left_columns;
    columns.extend(right_columns);
    let mut qualifiers = left_qualifiers;
    qualifiers.extend(right_qualifiers);
    let mut collations = left_collations;
    collations.extend(right_collations);
    Ok(Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(out_rows),
    })
}

/// Rebinds a right-of-APPLY `TableRef` to one left row: a TVF's arguments are
/// evaluated against the left row to literals; a derived table's correlated
/// outer references are substituted; a base table is unchanged. The rebound
/// reference builds with no remaining correlation.
pub(super) fn substitute_outer_in_tref(
    storage: &Storage,
    tref: &TableRef,
    outer: &dyn Fn(&str) -> Option<usize>,
    outer_row: &[SqlValue],
    eval_ctx: &EvalContext,
) -> Result<TableRef, SqlError> {
    match tref {
        TableRef::Table { .. } => Ok(tref.clone()),
        TableRef::Function { name, args, alias } => {
            let resolver = FnResolver(outer);
            let bound_args = args
                .iter()
                .map(|arg| {
                    let bound =
                        substitute_correlated_in_expr(storage, arg, outer, outer_row, eval_ctx)?;
                    let value = eval::eval(&bound, outer_row, &resolver, eval_ctx)?;
                    Ok(Expr {
                        kind: ExprKind::Literal(value),
                        span: arg.span,
                    })
                })
                .collect::<Result<Vec<_>, SqlError>>()?;
            Ok(TableRef::Function {
                name: name.clone(),
                args: bound_args,
                alias: alias.clone(),
            })
        }
        TableRef::Derived { subquery, alias } => {
            let bound = substitute_subquery_outer_refs(
                storage,
                eval_ctx.database_id,
                subquery,
                outer,
                outer_row,
            )
            .unwrap_or_else(|| (**subquery).clone());
            Ok(TableRef::Derived {
                subquery: Box::new(bound),
                alias: alias.clone(),
            })
        }
        TableRef::Join {
            left,
            right,
            kind,
            on,
        } => Ok(TableRef::Join {
            left: Box::new(substitute_outer_in_tref(
                storage, left, outer, outer_row, eval_ctx,
            )?),
            right: Box::new(substitute_outer_in_tref(
                storage, right, outer, outer_row, eval_ctx,
            )?),
            kind: *kind,
            on: on
                .as_ref()
                .map(|e| substitute_correlated_in_expr(storage, e, outer, outer_row, eval_ctx))
                .transpose()?,
        }),
    }
}

/// Builds a derived table's row source by executing its subquery and stamping
/// every output column with the derived-table alias. Every column must be named
/// (8155) and names must be unique within the derived table (8156).
pub(super) fn build_derived_source(
    storage: &Storage,
    subquery: &Select,
    alias: &Name,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let rowset = exec_select(storage, subquery, eval_ctx)?;
    for (index, column) in rowset.columns.iter().enumerate() {
        if column.name.is_empty() {
            return Err(SqlError::new(
                8155,
                16,
                2,
                format!(
                    "No column name was specified for column {} of '{}'.",
                    index + 1,
                    alias.value
                ),
            ));
        }
        if rowset.columns[..index]
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(&column.name))
        {
            return Err(SqlError::new(
                8156,
                16,
                1,
                format!(
                    "The column '{}' was specified multiple times for '{}'.",
                    column.name, alias.value
                ),
            ));
        }
    }
    let count = rowset.columns.len();
    Ok(Source {
        columns: rowset.columns,
        qualifiers: vec![Some(alias.value.clone()); count],
        // KNOWN LIMITATION: a RowSet carries no per-column collation, so a
        // derived character column loses its source collation and an outer
        // ORDER BY sorts it under the database default rather than the base
        // column's COLLATE. Fixing this needs collation threaded through the
        // project/RowSet boundary; deferred (narrow, non-default-collation only).
        collations: vec![None; count],
        rows: SourceRows::Materialized(rowset.rows),
    })
}

/// Joins two sources. The PROBE side — the side driving output, walked exactly
/// once: left, or right for a RIGHT join — streams slice-by-slice; only the
/// BUILD side is materialized here, and the hash join grace-spills it past the
/// memory budget. The ON predicate (absent for CROSS) is evaluated against the
/// concatenated row; outer joins emit NULL-extended rows for unmatched sides.
pub(super) fn join_sources(
    storage: &Storage,
    left: Source,
    right: Source,
    kind: JoinKind,
    on: Option<&Expr>,
    eval_ctx: &EvalContext,
) -> Result<Source, SqlError> {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    let mut qualifiers = left.qualifiers.clone();
    qualifiers.extend(right.qualifiers.clone());
    let mut collations = left.collations.clone();
    collations.extend(right.collations.clone());
    let types: Vec<ColumnType> = columns.iter().map(|c| c.column_type).collect();
    let scope = JoinScope {
        columns: qualifiers
            .iter()
            .zip(&columns)
            .map(|(q, c)| (q.clone(), c.name.clone()))
            .collect(),
        collations: collations.clone(),
    };
    let left_nulls = vec![Datum::Null; left.columns.len()];
    let right_nulls = vec![Datum::Null; right.columns.len()];

    // A subquery or user scalar function in the ON predicate is folded against
    // the joined row before the pure evaluator runs (per candidate pair).
    let on_needs_binding =
        on.is_some_and(|pred| expr_needs_binding(storage, eval_ctx.database_id, pred));
    let concat = |l: &[Datum], r: &[Datum]| -> Vec<Datum> { l.iter().chain(r).cloned().collect() };
    let matches = |l: &[Datum], r: &[Datum]| -> Result<bool, SqlError> {
        match on {
            None => Ok(true),
            Some(pred) => {
                let vals = row_values(&concat(l, r), &types);
                let value = if on_needs_binding {
                    let outer = |name: &str| scope.resolve(name);
                    let bound =
                        substitute_correlated_in_expr(storage, pred, &outer, &vals, eval_ctx)?;
                    eval::eval(&bound, &vals, &scope, eval_ctx)?
                } else {
                    eval::eval(pred, &vals, &scope, eval_ctx)?
                };
                match value {
                    SqlValue::Bool(b) => Ok(b),
                    SqlValue::Null => Ok(false),
                    _ => Err(SqlError::new(
                        4145,
                        15,
                        1,
                        "An expression of non-boolean type specified in a context where a condition is expected, near 'ON'.",
                    )
                    .at(pred.span)),
                }
            }
        }
    };

    // Equijoin key columns (bare `left_col = right_col` conjuncts of a
    // hash-compatible type). When present on an INNER/LEFT/RIGHT/FULL join, a
    // hash join replaces the O(n·m) nested loop; the full ON predicate is still
    // re-checked on each hash candidate, so the result set and its order are
    // identical to the nested loop. (Like a real optimizer, the hash join
    // evaluates the ON predicate only on candidate pairs sharing a key, so a
    // side-effecting error in a residual conjunct — e.g. `1/b.z` — may be raised
    // on fewer rows than the loop would; the SQL result set is unaffected.)
    // CROSS and equi-key-less joins keep the loop.
    let equi = match on {
        Some(pred) => extract_equi_keys(pred, &left, &right),
        None => Vec::new(),
    };

    // The build side is the one NOT driving output: left for RIGHT, else
    // right. It is walked repeatedly (nested loop) or hashed whole, so it
    // materializes here (bounded by the grace-hash spill past the budget);
    // the probe side stays a stream.
    let build_left = matches!(kind, JoinKind::Right);
    let (probe, build) = if build_left {
        (right, left)
    } else {
        (left, right)
    };
    let build = MaterializedSource::from(build, storage)?;
    // LEFT/RIGHT/FULL null-extend unmatched probe rows; FULL also null-extends
    // unmatched build rows. Emission is oriented so output is always
    // [left columns .. right columns].
    let preserve_probe = matches!(kind, JoinKind::Left | JoinKind::Right | JoinKind::Full);
    let preserve_build = matches!(kind, JoinKind::Full);
    let emit_match = |p: &[Datum], b: &[Datum]| -> Vec<Datum> {
        if build_left {
            concat(b, p)
        } else {
            concat(p, b)
        }
    };
    let emit_probe_only = |p: &[Datum]| -> Vec<Datum> {
        if build_left {
            concat(&left_nulls, p)
        } else {
            concat(p, &right_nulls)
        }
    };
    let emit_build_only = |b: &[Datum]| -> Vec<Datum> {
        if build_left {
            concat(b, &right_nulls)
        } else {
            concat(&left_nulls, b)
        }
    };

    let mut rows = Vec::new();
    if !equi.is_empty() && !matches!(kind, JoinKind::Cross) {
        hash_join(
            storage,
            probe,
            &build,
            build_left,
            &equi,
            preserve_probe,
            preserve_build,
            &matches,
            &emit_match,
            &emit_probe_only,
            &emit_build_only,
            &mut rows,
        )?;
    } else {
        // Nested loop: stream the probe side, walking the whole build side
        // per probe row.
        let mut build_matched = vec![false; build.rows.len()];
        let mut probe_rows = probe.rows;
        while let Some(slice) = probe_rows.next_slice(storage)? {
            for p in &slice {
                check_cancelled()?;
                let mut matched = false;
                for (bi, b) in build.rows.iter().enumerate() {
                    if matches_oriented(p, b, build_left, &matches)? {
                        rows.push(emit_match(p, b));
                        matched = true;
                        build_matched[bi] = true;
                    }
                }
                if preserve_probe && !matched {
                    rows.push(emit_probe_only(p));
                }
            }
        }
        if preserve_build {
            for (bi, b) in build.rows.iter().enumerate() {
                if !build_matched[bi] {
                    rows.push(emit_build_only(b));
                }
            }
        }
    }
    Ok(Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    })
}

/// An equijoin key pair: `(left column index, right column index)` for a
/// `left_col = right_col` conjunct of the ON predicate.
pub(super) type EquiKey = (usize, usize);

/// Extracts the equijoin key pairs usable for a hash join from an ON predicate:
/// the top-level `AND` conjuncts that are `col = col` with one bare column
/// resolving uniquely to the left source, the other uniquely to the right, and
/// matching hash classes. A predicate with no such conjunct (a range/disjunction
/// join, an expression key, or a type-mismatched equality) yields an empty list
/// and the caller keeps the nested-loop join. Non-equi conjuncts are left for
/// the full-ON re-check on each hash candidate, so results are unchanged.
pub(super) fn extract_equi_keys(pred: &Expr, left: &Source, right: &Source) -> Vec<EquiKey> {
    let left_scope = left.scope();
    let right_scope = right.scope();
    // `Some(true)` = resolves uniquely to the left source, `Some(false)` = right,
    // `None` = neither, both, or not a bare column.
    let side_of = |expr: &Expr| -> Option<(bool, usize)> {
        let ExprKind::Column(name) = &expr.kind else {
            return None;
        };
        match (
            left_scope.resolve(&name.value),
            right_scope.resolve(&name.value),
        ) {
            (Some(i), None) => Some((true, i)),
            (None, Some(j)) => Some((false, j)),
            _ => None,
        }
    };
    let mut conjuncts = Vec::new();
    flatten_and(pred, &mut conjuncts);
    let mut keys = Vec::new();
    for conjunct in conjuncts {
        let ExprKind::Binary {
            op: ast::BinaryOp::Eq,
            left: le,
            right: re,
        } = &conjunct.kind
        else {
            continue;
        };
        let pair = match (side_of(le), side_of(re)) {
            (Some((true, i)), Some((false, j))) => (i, j),
            (Some((false, j)), Some((true, i))) => (i, j),
            _ => continue,
        };
        if hash::hash_class(left.columns[pair.0].column_type)
            == hash::hash_class(right.columns[pair.1].column_type)
        {
            keys.push(pair);
        }
    }
    keys
}

/// Collects the top-level `AND` conjuncts of an expression (flattening nested
/// `AND`s); any other expression is one conjunct.
pub(super) fn flatten_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let ExprKind::Binary {
        op: ast::BinaryOp::And,
        left,
        right,
    } = &expr.kind
    {
        flatten_and(left, out);
        flatten_and(right, out);
    } else {
        out.push(expr);
    }
}

/// Grace-hash join for any kind: partition both inputs by join-key hash into
/// temp extents (matching rows share a partition, since equal keys hash equally,
/// so per-partition matched/unmatched equals globally matched/unmatched), then
/// join each partition pair in memory — the build hash table is bounded to one
/// partition. The probe input streams straight into its partitions; each
/// partition then materializes only its build rows and streams its probe rows
/// back. NULL-keyed rows never match: the outer side's are null-extended
/// directly, the inner side's are dropped. Output order is by partition
/// (immaterial — a spilling join is not order-sensitive).
#[allow(clippy::too_many_arguments)]
pub(super) fn grace_hash_join(
    storage: &Storage,
    mut probe_rows: SourceRows,
    build: &MaterializedSource,
    build_left: bool,
    preserve_probe: bool,
    preserve_build: bool,
    probe_key: &impl Fn(&[Datum]) -> Vec<SqlValue>,
    build_key: &impl Fn(&[Datum]) -> Vec<SqlValue>,
    matches: &impl Fn(&[Datum], &[Datum]) -> Result<bool, SqlError>,
    emit_match: &impl Fn(&[Datum], &[Datum]) -> Vec<Datum>,
    emit_probe_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    emit_build_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    rows: &mut Vec<Vec<Datum>>,
) -> Result<(), SqlError> {
    use hash::{HashKey, key_has_null};
    use std::collections::HashMap;

    let build_bytes: usize = build.rows.iter().map(|r| approx_row_bytes(r)).sum();
    let partitions = (build_bytes / sort_budget() + 1).max(2);
    let partition_of = |key: &[SqlValue]| -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        HashKey(key.to_vec()).hash(&mut hasher);
        (hasher.finish() % partitions as u64) as usize
    };
    let spill_err = |e| map_storage_err(e, "<join spill>");

    let mut probe_parts: Vec<_> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    let mut build_parts: Vec<_> = (0..partitions)
        .map(|_| crate::relstore::spill::RowSpool::new(storage))
        .collect();
    // Partition non-null-key rows; null-key rows can't match, so emit the outer
    // side's now and drop the rest. The probe side streams slice-by-slice.
    while let Some(slice) = probe_rows.next_slice(storage)? {
        for p in &slice {
            check_cancelled()?;
            let key = probe_key(p);
            if key_has_null(&key) {
                if preserve_probe {
                    rows.push(emit_probe_only(p));
                }
                continue;
            }
            probe_parts[partition_of(&key)]
                .write_row(p)
                .map_err(spill_err)?;
        }
    }
    for b in &build.rows {
        check_cancelled()?;
        let key = build_key(b);
        if key_has_null(&key) {
            if preserve_build {
                rows.push(emit_build_only(b));
            }
            continue;
        }
        build_parts[partition_of(&key)]
            .write_row(b)
            .map_err(spill_err)?;
    }
    for part in probe_parts.iter_mut().chain(build_parts.iter_mut()) {
        part.finish_writing().map_err(spill_err)?;
    }

    for part in 0..partitions {
        let mut b_rows: Vec<Vec<Datum>> =
            Vec::with_capacity(build_parts[part].row_count() as usize);
        let mut b_reader = build_parts[part].reader();
        while let Some(row) = b_reader.next_row().map_err(spill_err)? {
            b_rows.push(row);
        }
        let mut table: HashMap<HashKey, Vec<usize>> = HashMap::new();
        for (bi, b) in b_rows.iter().enumerate() {
            table.entry(HashKey(build_key(b))).or_default().push(bi);
        }
        let mut build_matched = vec![false; b_rows.len()];
        let mut p_reader = probe_parts[part].reader();
        while let Some(p) = p_reader.next_row().map_err(spill_err)? {
            let mut matched = false;
            if let Some(cands) = table.get(&HashKey(probe_key(&p))) {
                for &bi in cands {
                    let b = &b_rows[bi];
                    if matches_oriented(&p, b, build_left, matches)? {
                        rows.push(emit_match(&p, b));
                        matched = true;
                        build_matched[bi] = true;
                    }
                }
            }
            if preserve_probe && !matched {
                rows.push(emit_probe_only(&p));
            }
        }
        if preserve_build {
            for (bi, b) in b_rows.iter().enumerate() {
                if !build_matched[bi] {
                    rows.push(emit_build_only(b));
                }
            }
        }
    }
    Ok(())
}

/// Evaluates the ON predicate for a probe/build pair in the caller's left/right
/// orientation (`matches` always takes `(left, right)`).
pub(super) fn matches_oriented(
    probe: &[Datum],
    build: &[Datum],
    build_left: bool,
    matches: &impl Fn(&[Datum], &[Datum]) -> Result<bool, SqlError>,
) -> Result<bool, SqlError> {
    if build_left {
        matches(build, probe)
    } else {
        matches(probe, build)
    }
}

/// Hash join on the given equi-key columns. The build side is hashed by its
/// key tuple; the probe side streams and drives output, so row order matches
/// the nested loop exactly (unmatched build rows for FULL are null-extended at
/// the end, as the loop does). NULL key components never match (`x = NULL` is
/// UNKNOWN), so NULL-keyed rows are excluded from the table and treated as
/// unmatched. The full ON predicate is re-evaluated on every candidate, so
/// residual (non-equi) conjuncts and the 3VL of the equality are honored
/// identically to the nested loop. A build side past the memory budget spills
/// via [`grace_hash_join`].
#[allow(clippy::too_many_arguments)]
pub(super) fn hash_join(
    storage: &Storage,
    probe: Source,
    build: &MaterializedSource,
    build_left: bool,
    equi: &[EquiKey],
    preserve_probe: bool,
    preserve_build: bool,
    matches: &impl Fn(&[Datum], &[Datum]) -> Result<bool, SqlError>,
    emit_match: &impl Fn(&[Datum], &[Datum]) -> Vec<Datum>,
    emit_probe_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    emit_build_only: &impl Fn(&[Datum]) -> Vec<Datum>,
    rows: &mut Vec<Vec<Datum>>,
) -> Result<(), SqlError> {
    use hash::{HashKey, key_has_null};
    use std::collections::HashMap;

    let Source {
        columns: probe_columns,
        collations: probe_collations,
        rows: mut probe_rows,
        ..
    } = probe;

    // The case sensitivity governing each equi-key pair — the combined collation
    // of its two columns, combined in left/right order (`combine` favors its
    // first operand when both sides are exact, and `matches` combines in that
    // same order). The hash key is only a *pre-filter*: the full ON predicate
    // (collation-aware `matches`) re-checks each candidate, so the buckets must
    // be a superset of true matches. Folding both sides' key strings by this
    // sensitivity ensures case-insensitive-equal keys share a bucket (an
    // unfolded, case-sensitive hash would put `'abc'` and `'ABC'` in different
    // buckets, and the CI `matches` would never be consulted → a lost match).
    let (left_collations, right_collations) = if build_left {
        (&build.collations, &probe_collations)
    } else {
        (&probe_collations, &build.collations)
    };
    let key_sens: Vec<CollationSensitivity> = equi
        .iter()
        .map(|&(i, j)| {
            CollationSensitivity::from_optional(left_collations.get(i).and_then(|c| c.as_deref()))
                .combine(CollationSensitivity::from_optional(
                    right_collations.get(j).and_then(|c| c.as_deref()),
                ))
        })
        .collect();
    // Each equi pair reoriented as (probe column, build column): the pairs are
    // (left, right), and the build side is left exactly for a RIGHT join.
    let key_cols: Vec<(usize, usize)> = equi
        .iter()
        .map(|&(i, j)| if build_left { (j, i) } else { (i, j) })
        .collect();
    let probe_key = |p: &[Datum]| -> Vec<SqlValue> {
        key_cols
            .iter()
            .zip(&key_sens)
            .map(|(&(pc, _), &sens)| {
                sens.fold_value(value::datum_to_sql(&p[pc], &probe_columns[pc].column_type))
            })
            .collect()
    };
    let build_key = |b: &[Datum]| -> Vec<SqlValue> {
        key_cols
            .iter()
            .zip(&key_sens)
            .map(|(&(_, bc), &sens)| {
                sens.fold_value(value::datum_to_sql(&b[bc], &build.columns[bc].column_type))
            })
            .collect()
    };

    // Grace-hash spill for a large build side (any kind): partition both sides
    // by join-key hash so each partition's build table fits the memory budget.
    let build_bytes: usize = build.rows.iter().map(|r| approx_row_bytes(r)).sum();
    if build_bytes > sort_budget() {
        return grace_hash_join(
            storage,
            probe_rows,
            build,
            build_left,
            preserve_probe,
            preserve_build,
            &probe_key,
            &build_key,
            matches,
            emit_match,
            emit_probe_only,
            emit_build_only,
            rows,
        );
    }

    let mut table: HashMap<HashKey, Vec<usize>> = HashMap::new();
    for (index, row) in build.rows.iter().enumerate() {
        check_cancelled()?;
        let key = build_key(row);
        if key_has_null(&key) {
            continue;
        }
        table.entry(HashKey(key)).or_default().push(index);
    }

    let mut build_matched = vec![false; build.rows.len()];
    while let Some(slice) = probe_rows.next_slice(storage)? {
        for p in &slice {
            check_cancelled()?;
            let key = probe_key(p);
            let mut matched = false;
            if !key_has_null(&key)
                && let Some(cands) = table.get(&HashKey(key))
            {
                for &bi in cands {
                    let b = &build.rows[bi];
                    if matches_oriented(p, b, build_left, matches)? {
                        rows.push(emit_match(p, b));
                        matched = true;
                        build_matched[bi] = true;
                    }
                }
            }
            if preserve_probe && !matched {
                rows.push(emit_probe_only(p));
            }
        }
    }
    if preserve_build {
        for (bi, b) in build.rows.iter().enumerate() {
            if !build_matched[bi] {
                rows.push(emit_build_only(b));
            }
        }
    }
    Ok(())
}
