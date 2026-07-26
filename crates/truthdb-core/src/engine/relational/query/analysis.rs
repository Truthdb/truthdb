use super::super::prelude::*;

/// Collects every base-table name referenced in a FROM join tree, recursing
/// into derived-table subqueries so their tables are locked too. (Used for the
/// SHOWPLAN table list; [`collect_locked_tables`] is the lock-set collector.)
pub(in crate::engine::relational) fn collect_table_names<'a>(
    tref: &'a TableRef,
    out: &mut Vec<&'a Name>,
) {
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
pub(in crate::engine::relational) fn collect_locked_tables<'a>(
    select: &'a Select,
    out: &mut Vec<&'a Name>,
) {
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
pub(in crate::engine::relational) fn collect_from_tables<'a>(
    tref: &'a TableRef,
    out: &mut Vec<&'a Name>,
) {
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
pub(in crate::engine::relational) fn expr_uses_local_var(
    expr: &Expr,
    names: &std::collections::HashSet<&str>,
) -> bool {
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
pub(in crate::engine::relational) fn select_uses_local_var(
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

pub(in crate::engine::relational) fn collect_expr_tables<'a>(
    expr: &'a Expr,
    out: &mut Vec<&'a Name>,
) {
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
pub(in crate::engine::relational) fn collect_expr_read_names(
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
pub(in crate::engine::relational) fn collect_select_read_names(
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

pub(in crate::engine::relational) fn collect_from_read_names(
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
pub(in crate::engine::relational) fn collect_statement_read_names(
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
pub(in crate::engine::relational) fn exposed_name(name: &Name, alias: Option<&Name>) -> String {
    alias
        .map(|a| a.value.clone())
        .unwrap_or_else(|| strip_schema(&name.value).to_string())
}

/// Collects the exposed names of every table in a FROM tree. A derived table's
/// exposed name is its alias (its inner tables are not exposed to the outer
/// query).
pub(in crate::engine::relational) fn collect_exposed_names(tref: &TableRef, out: &mut Vec<String>) {
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
pub(in crate::engine::relational) fn check_exposed_names(from: &TableRef) -> Result<(), SqlError> {
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
