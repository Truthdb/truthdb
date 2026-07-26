use crate::ast::{Expr, ExprKind};
use crate::decimal::Decimal;
use crate::error::{SqlError, SqlResult};
use crate::functions;
use crate::value::SqlValue;

use super::context::{ColumnResolver, ErrorInfo, EvalContext};
use super::expression::eval_at;

#[inline(never)]
fn trigger_function_outside_trigger() -> SqlError {
    SqlError::message_only(
        4101,
        "UPDATE() and COLUMNS_UPDATED() may be used only inside a trigger body.".to_string(),
    )
}

pub(super) fn eval_call<R: ColumnResolver>(
    name: &str,
    args: &[Expr],
    row: &[SqlValue],
    resolver: &R,
    ctx: &EvalContext,
    depth: usize,
) -> SqlResult<SqlValue> {
    // Trigger-only column-change functions. UPDATE(<col>) takes a column NAME
    // (not a value), so it is handled before the arguments are evaluated.
    if name.eq_ignore_ascii_case("UPDATE") {
        let col = match args {
            [arg] => match &arg.kind {
                ExprKind::Column(n) => &n.value,
                _ => {
                    return Err(SqlError::message_only(
                        174,
                        "The UPDATE function requires one column-name argument.".to_string(),
                    ));
                }
            },
            _ => {
                return Err(SqlError::message_only(
                    174,
                    "The UPDATE function requires one column-name argument.".to_string(),
                ));
            }
        };
        let u = ctx
            .updated_columns
            .as_ref()
            .ok_or_else(trigger_function_outside_trigger)?;
        let index = u
            .columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(col))
            .ok_or_else(|| SqlError::message_only(207, format!("Invalid column name '{col}'.")))?;
        return Ok(SqlValue::Bool(u.touched.contains(&index)));
    }
    if name.eq_ignore_ascii_case("COLUMNS_UPDATED") {
        let u = ctx
            .updated_columns
            .as_ref()
            .ok_or_else(trigger_function_outside_trigger)?;
        // A varbinary bitmask: column c (1-based) sets bit (c-1)%8 of byte (c-1)/8.
        let mut bytes = vec![0u8; u.columns.len().div_ceil(8).max(1)];
        for &i in &u.touched {
            bytes[i / 8] |= 1 << (i % 8);
        }
        return Ok(SqlValue::Binary(bytes));
    }
    // Session-context functions read the EvalContext, which the pure
    // functions::eval_function does not receive.
    if let Some(value) = eval_session_function(name, args) {
        return Ok(value(ctx));
    }
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval_at(arg, row, resolver, ctx, depth + 1)?);
    }
    // SERVERPROPERTY dispatches on its ARGUMENT VALUE and reads the session
    // context — it fits neither tier (session functions never see argument
    // values; pure functions never see the context).
    if name.eq_ignore_ascii_case("SERVERPROPERTY") {
        if values.len() != 1 {
            return Err(SqlError::message_only(
                174,
                "The SERVERPROPERTY function requires 1 argument(s).".to_string(),
            ));
        }
        return Ok(serverproperty(&values[0]));
    }
    // Role-membership intrinsics read an argument (the role name) AND the session
    // context (its effective role set) — like SERVERPROPERTY, they fit neither
    // tier. IS_SRVROLEMEMBER consults the SERVER roles, IS_ROLEMEMBER the DATABASE
    // roles.
    if name.eq_ignore_ascii_case("IS_SRVROLEMEMBER") {
        return Ok(is_rolemember(&values, ctx, &ctx.server_roles));
    }
    if name.eq_ignore_ascii_case("IS_ROLEMEMBER") {
        return Ok(is_rolemember(&values, ctx, &ctx.db_roles));
    }
    // The by-id/by-name forms of USER_NAME (the no-argument session form is
    // handled in eval_session_function) need the catalog to resolve an arbitrary
    // principal; without it, report NULL rather than a misleading not-a-builtin
    // error.
    if name.eq_ignore_ascii_case("USER_NAME") {
        return Ok(SqlValue::Null);
    }
    // DB_NAME(id) / DB_ID(name): argument + the session's database snapshot —
    // the SERVERPROPERTY shape. Unknown id/name is NULL, per SQL Server.
    if name.eq_ignore_ascii_case("DB_NAME") && values.len() == 1 {
        let id = match &values[0] {
            SqlValue::Int(id) => *id,
            _ => return Ok(SqlValue::Null),
        };
        return Ok(ctx
            .databases
            .iter()
            .find(|(db, _)| *db as i64 == id)
            .map_or(SqlValue::Null, |(_, name)| SqlValue::Str(name.clone())));
    }
    if name.eq_ignore_ascii_case("DB_ID") && values.len() == 1 {
        let target = match &values[0] {
            SqlValue::Str(name) => name,
            _ => return Ok(SqlValue::Null),
        };
        return Ok(ctx
            .databases
            .iter()
            .find(|(_, name)| name.eq_ignore_ascii_case(target))
            .map_or(SqlValue::Null, |(id, _)| SqlValue::Int(*id as i64)));
    }
    functions::eval_function(name, values)
}

/// `IS_ROLEMEMBER('role'[, 'principal'])` / `IS_SRVROLEMEMBER('role'[, 'login'])`.
/// Returns 1 if the tested principal belongs to `role` (looked up in `roles`, the
/// relevant server- or database-role set), 0 if not, and NULL when the role
/// argument is NULL/empty. The optional second argument names a specific
/// principal; only the session's own login/user can be answered here (an
/// arbitrary principal needs the catalog), so any other name yields NULL. A role
/// the session does not belong to reads as 0 (a nonexistent role is not
/// distinguished from a non-membership without the catalog — a documented,
/// membership-preserving simplification).
fn is_rolemember(
    values: &[SqlValue],
    ctx: &EvalContext,
    roles: &std::collections::HashSet<String>,
) -> SqlValue {
    let role = match values.first() {
        Some(SqlValue::Str(role)) if !role.is_empty() => role.to_ascii_lowercase(),
        _ => return SqlValue::Null,
    };
    if let Some(second) = values.get(1) {
        let SqlValue::Str(named) = second else {
            return SqlValue::Null;
        };
        let named = named.to_ascii_lowercase();
        let is_self =
            named == ctx.login.to_ascii_lowercase() || named == ctx.user.to_ascii_lowercase();
        if !is_self {
            return SqlValue::Null;
        }
    }
    SqlValue::Int(i64::from(roles.contains(&role)))
}

/// `SERVERPROPERTY(<name>)` — the SSMS query-window probes. Unknown
/// properties return NULL, exactly as SQL Server does; the version strings
/// agree with `@@VERSION` and the LOGINACK bytes.
fn serverproperty(property: &SqlValue) -> SqlValue {
    let SqlValue::Str(property) = property else {
        return SqlValue::Null;
    };
    match property.to_ascii_lowercase().as_str() {
        "productversion" => SqlValue::Str("16.0.1000.6".to_string()),
        "productmajorversion" => SqlValue::Str("16".to_string()),
        "productlevel" => SqlValue::Str("RTM".to_string()),
        "edition" => SqlValue::Str("TruthDB Edition (64-bit)".to_string()),
        // 3 = Enterprise-class engine; what tools branch on.
        "engineedition" => SqlValue::Int(3),
        "servername" | "machinename" => SqlValue::Str("TruthDB".to_string()),
        // Default instance: no instance name.
        "instancename" => SqlValue::Null,
        "collation" => SqlValue::Str("SQL_Latin1_General_CP1_CI_AS".to_string()),
        "isintegratedsecurityonly" | "isclustered" | "ishadrenabled" | "issingleuser" => {
            SqlValue::Int(0)
        }
        _ => SqlValue::Null,
    }
}

/// Session-identity intrinsics that resolve against the [`EvalContext`] rather
/// than their arguments. Returns a closure so the (cheap) context read happens
/// only when the name matches. With a single database, `DB_NAME()` and
/// `DB_NAME(id)` both report the current database.
fn eval_session_function(name: &str, args: &[Expr]) -> Option<fn(&EvalContext) -> SqlValue> {
    match name.to_ascii_uppercase().as_str() {
        "DB_NAME" if args.is_empty() => Some(|ctx| SqlValue::Str(ctx.database.clone())),
        "DB_ID" if args.is_empty() => Some(|ctx| SqlValue::Int(ctx.database_id as i64)),
        "SUSER_SNAME" | "SUSER_NAME" if args.is_empty() => {
            Some(|ctx| SqlValue::Str(ctx.login.clone()))
        }
        // The session's database user. The by-id form USER_NAME(<id>) needs the
        // catalog and is not resolved here. (The niladic CURRENT_USER — no
        // parentheses — is a separate parser-token feature, not implemented.)
        "USER_NAME" if args.is_empty() => Some(|ctx| SqlValue::Str(ctx.user.clone())),
        // SQL Server returns NUMERIC(38,0); NULL when no identity has been
        // generated in the scope yet.
        "SCOPE_IDENTITY" if args.is_empty() => Some(|ctx| match ctx.scope_identity {
            Some(value) => SqlValue::Decimal(Box::new(Decimal::new(value as i128, 38, 0))),
            None => SqlValue::Null,
        }),
        // Error intrinsics — NULL outside a CATCH block, the caught error's
        // fields within one. (Line/procedure untracked: 0 / NULL.)
        "ERROR_NUMBER" if args.is_empty() => Some(|ctx| match &ctx.error {
            Some(e) => SqlValue::Int(e.number as i64),
            None => SqlValue::Null,
        }),
        "ERROR_MESSAGE" if args.is_empty() => Some(|ctx| match &ctx.error {
            Some(e) => SqlValue::Str(e.message.clone()),
            None => SqlValue::Null,
        }),
        "ERROR_SEVERITY" if args.is_empty() => Some(|ctx| match &ctx.error {
            Some(e) => SqlValue::Int(e.severity as i64),
            None => SqlValue::Null,
        }),
        "ERROR_STATE" if args.is_empty() => Some(|ctx| match &ctx.error {
            Some(e) => SqlValue::Int(e.state as i64),
            None => SqlValue::Null,
        }),
        "ERROR_LINE" if args.is_empty() => Some(|ctx| match &ctx.error {
            Some(_) => SqlValue::Int(0),
            None => SqlValue::Null,
        }),
        "ERROR_PROCEDURE" if args.is_empty() => Some(|ctx| match &ctx.error {
            Some(ErrorInfo {
                procedure: Some(name),
                ..
            }) => SqlValue::Str(name.clone()),
            _ => SqlValue::Null,
        }),
        // Committability of the current transaction (independent of any CATCH).
        "XACT_STATE" if args.is_empty() => Some(|ctx| SqlValue::Int(ctx.xact_state as i64)),
        _ => None,
    }
}
