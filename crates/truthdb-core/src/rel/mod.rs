//! SQL execution over the relational storage engine.
//!
//! Parses a batch with [`truthdb_sql`], then binds and runs each statement
//! against [`Storage`]'s `rel_*` API. SELECT uses a simple Volcano-style
//! pipeline materialized in memory: source scan -> WHERE filter -> ORDER BY
//! sort -> TOP limit -> projection. `sys.tables`/`sys.columns` are virtual
//! sources built from the catalog. Storage errors are mapped to SQL Server
//! error numbers.

mod aggregate;
mod api;
mod batch;
pub mod collation;
mod context;
mod describe;
mod hash;
mod helpers;
mod plan;
mod query;
mod sys_views;
mod value;

pub use api::{
    BatchEmitter, BatchOutcome, Collector, DoneCommand, FATAL_SEVERITY, ResultColumn, RowSet,
    RpcParam, StatementResult,
};
pub use batch::{execute_batch, execute_batch_streamed, execute_batch_with_params};
pub use context::{Isolation, TxnContext};
pub use describe::describe_first_result_set;
pub use helpers::{SqlStatement, render_cell};
pub(crate) use query::decl_names;
pub use query::{CancelScope, check_cancelled};
#[cfg(test)]
pub(crate) use query::{
    clear_test_cancel, set_test_cancel, set_test_sort_budget, without_scan_path,
};

use api::{DeferredDone, DiscardEmitter, XACT_ABORT_SEVERITY};
use batch::BatchRun;
use batch::done_command;
use context::TableVar;
use describe::produces_rowset;
use helpers::*;
use query::*;
use sys_views::*;

use truthdb_sql::ast::{
    AlterAction, AlterDatabase, AlterTable, CheckConstraint, ColumnDef, CreateFunction,
    CreateIndex, CreateLogin, CreateProcedure, CreateTable, CreateTrigger, CreateUser, CreateView,
    DataType, DatabaseOption, Declaration, Delete, DropIndex, DropTable, DropView, ExecStatement,
    Expr, ExprKind, FetchDirection, ForeignKey, Insert, InsertSource, IsolationLevel, JoinKind,
    Name, OrderItem, PermissionAction, PermissionKind, PermissionStatement, RaiseError,
    RestoreMode, ReturnsClause, RoleMemberAction, Select, SelectItem, SetStatement, Statement,
    TableRef, ThrowArgs, ThrowStatement, Update,
};
use truthdb_sql::collation::CollationSensitivity;
use truthdb_sql::error::SqlError;
use truthdb_sql::eval::{ColumnResolver, EvalContext, SecurityContext};
use truthdb_sql::lexer::Span;
use truthdb_sql::value::{SqlValue, order_key_cmp};
use truthdb_sql::{ast, eval};

use xxhash_rust::xxh64::xxh64;

use crate::lock::{LockMode, Resource};
use crate::relstore::btree::ScanCursor;
use crate::relstore::catalog::{
    self, FunctionDef, FunctionReturns, PermAction, PermissionEntry, PrincipalDef, ProcParamDef,
    ProcedureDef, TableDef, TriggerDef,
};
use crate::relstore::row::{Column, Schema};
use crate::relstore::types::{ColumnType, Datum};
use crate::relstore::version::ReadSnapshot;
use crate::storage::{RowLocator, Storage, StorageError, StorageTxn, TxnScope};

/// The inner SQL text of an `EXEC sp_executesql N'...'` whose statement
/// argument is a string LITERAL — the analyzable case. `None` for any other
/// procedure or a non-literal statement argument.
/// Runs a statement list, recursing into `TRY`/`CATCH`. `in_try` is true while
/// executing inside a `TRY` block, where a statement error transfers control to
/// the matching `CATCH` (returned as `Err`) instead of applying the normal
/// batch policy. Returns `Err` when the enclosing context must stop: a cancel,
/// an error that propagates to a `CATCH`, or a dooming/terminating error at the
/// top level.
fn exec_literal_sql(exec: &ExecStatement) -> Option<String> {
    if !strip_schema(&exec.proc.value).eq_ignore_ascii_case("sp_executesql") {
        return None;
    }
    let stmt = exec
        .args
        .iter()
        .find(|a| {
            a.name.as_ref().is_some_and(|n| {
                n.value.eq_ignore_ascii_case("stmt") || n.value.eq_ignore_ascii_case("statement")
            })
        })
        .or_else(|| exec.args.first().filter(|a| a.name.is_none()))?;
    match &stmt.value.kind {
        ExprKind::Str(text) => Some(text.clone()),
        _ => None,
    }
}

thread_local! {
    /// Nesting depth of EXEC inner batches on this worker (SQL Server caps
    /// procedure nesting at 32, error 217).
    static EXEC_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Ownership-chaining depth for object-permission checks: how many OWNED
    /// stored-object bodies (procedure, scalar UDF, multi-statement TVF, trigger)
    /// enclose the current statement. Distinct from [`EXEC_DEPTH`] because
    /// `sp_executesql` bumps that but does NOT chain — dynamic SQL runs in the
    /// caller's own permission context. Permission checks fire only where this
    /// (and `VIEW_DEPTH`) is 0.
    static CHAIN_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII guard entered when running an OWNED stored-object body (procedure,
/// scalar UDF, multi-statement TVF, trigger): it raises the ownership-chaining
/// depth so the body's object reads are not re-permission-checked (the caller's
/// permission on the object suffices — single `dbo` owner).
struct ChainGuard;

impl ChainGuard {
    fn enter() -> Self {
        CHAIN_DEPTH.with(|d| d.set(d.get() + 1));
        ChainGuard
    }
}

impl Drop for ChainGuard {
    fn drop(&mut self) {
        CHAIN_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// RAII guard entered when running DYNAMIC SQL (`sp_executesql`): it RESETS the
/// ownership-chaining depth to 0 for the duration, then restores it — dynamic
/// SQL never chains, so its statements are permission-checked as the caller's
/// own, even when the `sp_executesql` call sits inside a procedure body.
struct DynamicScope(u32);

impl DynamicScope {
    fn enter() -> Self {
        let saved = CHAIN_DEPTH.with(|d| d.replace(0));
        DynamicScope(saved)
    }
}

impl Drop for DynamicScope {
    fn drop(&mut self) {
        CHAIN_DEPTH.with(|d| d.set(self.0));
    }
}

/// Runs `EXEC sp_executesql @stmt [, @params, values...]`: evaluates the
/// arguments against the CURRENT variables, then runs the inner text as its
/// own batch scope — fresh variables seeded from the declared parameters
/// (inner DECLAREs do not leak out; outer variables are not visible inside),
/// sharing the transaction context. Each inner statement emits its own
/// events, exactly like a top-level statement. Any other procedure answers
/// 2812, the same as the RPC path.
/// An EXEC failure, tagged by ORIGIN — the fact the EXEC arm needs and must
/// not guess: `run_exec`'s own validation/depth errors are statement-scope at
/// the EXEC site, while an error that crossed out of the inner batch already
/// terminated it (batch-abort scope is the whole nest).
enum ExecError {
    Own(SqlError),
    Inner(SqlError),
}

/// Applies the standard doom rule to an error raised outside any statement's
/// own execution — `run_exec`'s validation and depth errors, which no inner
/// `run_block` arm will see. The decision is made here, at the source, so the
/// TRY boundary never has to re-derive it (it cannot know the error's origin).
fn doom_per_rule(txn_ctx: &mut TxnContext, error: SqlError) -> SqlError {
    if txn_ctx.in_txn() && (txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY) {
        txn_ctx.doomed = true;
    }
    error
}

/// Executes a user stored procedure: binds arguments to declared parameters
/// (positional and named, defaults filling gaps, OUTPUT validated), runs the
/// stored body text under a fresh variable scope with SET options reverting
/// at exit (the sp_executesql posture), captures the RETURN status into
/// `EXEC @rc =`, and copies OUTPUT parameters back — both only when the body
/// completes (SQL Server skips them when execution aborts).
fn run_user_procedure(
    storage: &Storage,
    exec: &ExecStatement,
    def: &TableDef,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), ExecError> {
    let procedure = def.procedure.as_ref().expect("checked by the caller");
    let own = |txn_ctx: &mut TxnContext, error: SqlError| -> ExecError {
        ExecError::Own(doom_per_rule(txn_ctx, error))
    };
    // Evaluate arguments in the CALLER's scope.
    let eval_ctx = txn_ctx.eval_context();
    let mut positional = Vec::new();
    let mut named: Vec<(String, SqlValue, bool, Option<String>)> = Vec::new();
    let mut positional_meta: Vec<(bool, Option<String>)> = Vec::new();
    for (arg_index, arg) in exec.args.iter().enumerate() {
        // Once an argument is named, the rest must be (SQL Server 119) —
        // silently continuing would bind positions past the named one.
        if arg.name.is_none() && !named.is_empty() {
            let error = SqlError::new(
                119,
                15,
                1,
                format!(
                    "Must pass parameter number {} and subsequent parameters as '@name = value'. \
                     After the form '@name = value' has been used, all subsequent parameters must \
                     be passed in the form '@name = value'.",
                    arg_index + 1
                ),
            );
            return Err(own(txn_ctx, error));
        }
        // An OUTPUT argument must be a bare variable (it receives a value).
        let arg_var = match &arg.value.kind {
            ExprKind::LocalVar(name) => Some(name.clone()),
            _ => None,
        };
        if arg.output && arg_var.is_none() {
            let error = SqlError::new(
                179,
                16,
                1,
                "Cannot use the OUTPUT option when passing a constant to a stored procedure.",
            );
            return Err(own(txn_ctx, error));
        }
        let value = eval_constant(&arg.value, &eval_ctx).map_err(|e| own(txn_ctx, e))?;
        match &arg.name {
            Some(n) => {
                let key = n.value.trim_start_matches('@').to_ascii_lowercase();
                // A parameter supplied twice (named twice, or named on top
                // of a positional binding) is an error, not a silent pick.
                let position_of = |name: &str| procedure.params.iter().position(|p| p.name == name);
                let already_positional =
                    position_of(&key).is_some_and(|index| index < positional.len());
                if already_positional || named.iter().any(|(n, ..)| *n == key) {
                    let error = SqlError::new(
                        8143,
                        16,
                        1,
                        format!(
                            "Parameter '@{key}' was supplied multiple times for procedure {}.",
                            def.name
                        ),
                    );
                    return Err(own(txn_ctx, error));
                }
                named.push((key, value, arg.output, arg_var));
            }
            None => {
                positional.push(value);
                positional_meta.push((arg.output, arg_var));
            }
        }
    }
    // `EXEC @rc = p`: the status variable must already be declared (137).
    if let Some(rc) = &exec.return_var
        && !txn_ctx.variables.contains_key(rc)
    {
        let error = undeclared_variable_err(rc);
        return Err(own(txn_ctx, error));
    }
    if positional.len() > procedure.params.len() {
        let error = SqlError::new(
            8144,
            16,
            2,
            format!(
                "Procedure or function {} has too many arguments specified.",
                def.name
            ),
        );
        return Err(own(txn_ctx, error));
    }
    // Named arguments that match no declared parameter fail before any
    // binding (8145 precedes 201, as SQL Server orders it).
    for (name, ..) in &named {
        if !procedure.params.iter().any(|p| p.name == *name) {
            let error = SqlError::new(
                8145,
                16,
                2,
                format!("@{name} is not a parameter for procedure {}.", def.name),
            );
            return Err(own(txn_ctx, error));
        }
    }
    // Bind: positional in declaration order, then named by name, then
    // defaults; a missing non-default parameter is 201. OUTPUT copy-back
    // targets (param name -> caller variable) are collected as we bind.
    let mut bound: Vec<(String, ColumnType, SqlValue)> = Vec::new();
    let mut copy_back: Vec<(String, String)> = Vec::new();
    for (index, param) in procedure.params.iter().enumerate() {
        let column_type = ColumnType::parse(&param.type_spec).map_err(|e| {
            let error = SqlError::message_only(245, e.to_string());
            own(txn_ctx, error)
        })?;
        let supplied = if index < positional.len() {
            let (output, arg_var) = positional_meta[index].clone();
            Some((positional[index].clone(), output, arg_var))
        } else {
            named
                .iter()
                .find(|(n, ..)| *n == param.name)
                .map(|(_, v, output, arg_var)| (v.clone(), *output, arg_var.clone()))
        };
        let coerce = |value: SqlValue| -> Result<SqlValue, SqlError> {
            let datum = value::sql_to_datum(&value, &column_type, &param.name)?;
            Ok(value::datum_to_sql(&datum, &column_type))
        };
        let value = match supplied {
            Some((value, output, arg_var)) => {
                if output {
                    if !param.output {
                        let error = SqlError::new(
                            8162,
                            16,
                            2,
                            format!(
                                "The formal parameter \"@{}\" was not declared as an OUTPUT \
                                 parameter, but the actual parameter passed in requested output.",
                                param.name
                            ),
                        );
                        return Err(own(txn_ctx, error));
                    }
                    copy_back.push((
                        param.name.clone(),
                        arg_var.expect("validated: OUTPUT arguments are variables"),
                    ));
                }
                // Bind-time conversion to the DECLARED type, as SQL Server
                // converts (or errors) at the call — without it a string
                // argument flows into an INT parameter mistagged.
                coerce(value).map_err(|e| own(txn_ctx, e))?
            }
            None => match &param.default {
                Some(text) => {
                    let expr = truthdb_sql::parse_expr(text).map_err(|e| own(txn_ctx, e))?;
                    let value = eval_constant(&expr, &eval_ctx).map_err(|e| own(txn_ctx, e))?;
                    coerce(value).map_err(|e| own(txn_ctx, e))?
                }
                None => {
                    let error = SqlError::new(
                        201,
                        16,
                        4,
                        format!(
                            "Procedure or function '{}' expects parameter '@{}', which was not \
                             supplied.",
                            def.name, param.name
                        ),
                    );
                    return Err(own(txn_ctx, error));
                }
            },
        };
        bound.push((param.name.clone(), column_type, value));
    }
    // The stored body parses under the in-procedure grammar.
    let statements =
        truthdb_sql::parse_procedure_body(&procedure.body).map_err(|e| own(txn_ctx, e))?;

    // Fresh scope, SET options reverting at exit — the sp_executesql shape.
    let outer_database = txn_ctx.database.clone();
    let outer_database_id = txn_ctx.database_id();
    let outer_vars = std::mem::take(&mut txn_ctx.variables);
    let outer_table_vars = std::mem::take(&mut txn_ctx.table_variables);
    let outer_xact_abort = txn_ctx.xact_abort;
    let outer_nocount = txn_ctx.nocount;
    let outer_isolation = txn_ctx.isolation;
    let outer_showplan = txn_ctx.showplan_text;
    for (name, column_type, value) in bound {
        txn_ctx.variables.insert(name, (column_type, value));
    }
    // The body's unqualified names resolve in the procedure's HOME database,
    // not the caller's (SQL Server's rule). The body cannot USE (parser 154),
    // so this holds for its whole extent; the caller's context returns below.
    txn_ctx.set_current_database(database_name_of(storage, def.database_id), def.database_id);
    txn_ctx.proc_stack.push(def.name.clone());
    txn_ctx.proc_return = None;
    // A procedure called from a trigger body does NOT see the trigger's
    // inserted/deleted (they are visible only in the trigger's own statements).
    let _trigger_shadow = TriggerScope::clear();
    // A procedure body ownership-chains: its object reads are not re-checked.
    let _chain = ChainGuard::enter();
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    let result = if depth > 32 {
        let error = SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        );
        Err(ExecError::Own(doom_per_rule(txn_ctx, error)))
    } else {
        run_block(storage, &statements, txn_ctx, run, in_try)
            .and_then(end_of_scope)
            .map_err(ExecError::Inner)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    txn_ctx.proc_stack.pop();
    // Capture OUTPUT values from the inner scope BEFORE restoring the outer.
    let output_values: Vec<(String, (ColumnType, SqlValue))> = copy_back
        .iter()
        .filter_map(|(param, var)| {
            txn_ctx
                .variables
                .get(param)
                .map(|slot| (var.clone(), slot.clone()))
        })
        .collect();
    txn_ctx.variables = outer_vars;
    txn_ctx.table_variables = outer_table_vars;
    txn_ctx.xact_abort = outer_xact_abort;
    txn_ctx.nocount = outer_nocount;
    txn_ctx.isolation = outer_isolation;
    txn_ctx.showplan_text = outer_showplan;
    txn_ctx.set_current_database(outer_database, outer_database_id);
    let return_status = txn_ctx.proc_return.take().unwrap_or(0);
    if result.is_ok() {
        // OUTPUT copy-back and the return status land only when the body
        // completed (SQL Server skips both when execution aborts).
        for (var, slot) in output_values {
            txn_ctx.variables.insert(var, slot);
        }
        if let Some(rc) = &exec.return_var {
            txn_ctx
                .variables
                .insert(rc.clone(), (ColumnType::Int, SqlValue::Int(return_status)));
        }
    }
    result
}

/// Runs a scalar user-defined function's body once with `arg_values` bound to
/// its parameters, returning the value its `RETURN` produced, coerced to the
/// declared return type.
///
/// The body runs in an isolated throwaway context — only the parameters are
/// visible (SQL Server functions do not see caller locals), no transaction is
/// open (functions are side-effect-free), and any table reads observe the
/// caller's ambient snapshot on this thread. Nesting shares the `EXEC_DEPTH`
/// budget (217 at depth 32). Because the context has no transaction, an error in
/// the body always terminates the function (there is no XACT_ABORT-OFF continue
/// path), which is exactly the SQL Server posture: a function error aborts the
/// statement that called it.
fn run_user_scalar_function(
    storage: &Storage,
    def: &TableDef,
    arg_values: &[SqlValue],
    caller: &EvalContext,
) -> Result<SqlValue, SqlError> {
    let function = def.function.as_ref().expect("checked by the caller");
    // The caller (resolve_scalar_function) only routes scalar functions here.
    let FunctionReturns::Scalar { type_spec, body } = &function.returns else {
        return Err(function_not_a_table(&def.name));
    };
    // Invoking a scalar function needs EXECUTE permission.
    enforce_object_permission(storage, def, &caller.security, PermAction::Execute)?;
    if arg_values.len() < function.params.len() {
        return Err(SqlError::new(
            313,
            16,
            3,
            format!(
                "An insufficient number of arguments were supplied for the procedure or function {}.",
                def.name
            ),
        ));
    }
    if arg_values.len() > function.params.len() {
        return Err(SqlError::new(
            8144,
            16,
            2,
            format!(
                "Procedure or function {} has too many arguments specified.",
                def.name
            ),
        ));
    }
    let return_type =
        ColumnType::parse(type_spec).map_err(|e| SqlError::message_only(245, e.to_string()))?;
    // Fresh scope with only the parameters; the caller's session identity is
    // carried so DB_NAME()/SUSER_SNAME()/USER_NAME()/@@SPID and role membership
    // resolve inside the body. The sids are left 0 (the body reuses the caller's
    // already-computed role set rather than re-resolving membership).
    let mut txn_ctx = TxnContext::default();
    // The body's unqualified names resolve in the FUNCTION's home database
    // (matching collect_read_lock_ids); DB_ID/DB_NAME keep working via the
    // caller's databases snapshot.
    txn_ctx.set_session_identity(
        database_name_of(storage, def.database_id),
        def.database_id,
        caller.login.clone(),
        caller.spid,
        caller.user.clone(),
        0,
        0,
    );
    txn_ctx.databases_snapshot = caller.databases.clone();
    txn_ctx.session_server_roles = caller.server_roles.clone();
    txn_ctx.session_db_roles = caller.db_roles.clone();
    txn_ctx.security = caller.security.clone();
    for (param, value) in function.params.iter().zip(arg_values) {
        let column_type = ColumnType::parse(&param.type_spec)
            .map_err(|e| SqlError::message_only(245, e.to_string()))?;
        let datum = value::sql_to_datum(value, &column_type, &param.name)?;
        let coerced = value::datum_to_sql(&datum, &column_type);
        txn_ctx
            .variables
            .insert(param.name.clone(), (column_type, coerced));
    }
    let statements = truthdb_sql::parse_function_body(body)?;
    // A scalar function called from a trigger body does not see inserted/deleted.
    let _trigger_shadow = TriggerScope::clear();
    // A function body ownership-chains: its object reads are not re-checked.
    let _chain = ChainGuard::enter();
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
            function_return_type: Some(return_type),
        };
        run_block(storage, &statements, &mut txn_ctx, &mut run, false).and_then(end_of_scope)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    result?;
    // The body ends in `RETURN <expr>` (enforced at CREATE, 455), so a completed
    // body always set `func_return`.
    txn_ctx.func_return.take().ok_or_else(|| {
        SqlError::new(
            455,
            16,
            2,
            "The last statement included within a function must be a return statement.",
        )
    })
}

fn run_exec(
    storage: &Storage,
    exec: &ExecStatement,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), ExecError> {
    if !strip_schema(&exec.proc.value).eq_ignore_ascii_case("sp_executesql") {
        // A user procedure, if the catalog has one; 2812 otherwise.
        if let Some(def) = resolve_table(storage, txn_ctx.database_id(), &exec.proc.value)
            && def.is_procedure()
        {
            enforce_object_permission(storage, &def, &txn_ctx.security, PermAction::Execute)
                .map_err(|e| ExecError::Own(doom_per_rule(txn_ctx, e.at(exec.proc.span))))?;
            return run_user_procedure(storage, exec, &def, txn_ctx, run, in_try);
        }
        let error = SqlError::new(
            2812,
            16,
            62,
            format!("Could not find stored procedure '{}'.", exec.proc.value),
        )
        .at(exec.proc.span);
        return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
    }
    if exec.return_var.is_some() {
        let error = SqlError::new(
            179,
            16,
            1,
            "Cannot capture a return status from sp_executesql.",
        );
        return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
    }
    let eval_ctx = txn_ctx.eval_context();
    let mut positional = Vec::new();
    let mut named: Vec<(String, SqlValue)> = Vec::new();
    for arg in &exec.args {
        let value = eval_constant(&arg.value, &eval_ctx)
            .map_err(|e| ExecError::Own(doom_per_rule(txn_ctx, e)))?;
        match &arg.name {
            Some(n) => named.push((n.value.clone(), value)),
            None => positional.push(value),
        }
    }
    let take_named = |named: &mut Vec<(String, SqlValue)>, keys: &[&str]| -> Option<SqlValue> {
        let index = named
            .iter()
            .position(|(n, _)| keys.iter().any(|k| n.eq_ignore_ascii_case(k)))?;
        Some(named.remove(index).1)
    };
    let mut positional = positional.into_iter();
    let stmt = match take_named(&mut named, &["stmt", "statement"]).or_else(|| positional.next()) {
        Some(value) => value,
        None => {
            let error = SqlError::new(
                214,
                16,
                2,
                "Procedure expects parameter '@statement' of type 'ntext/nchar/nvarchar'.",
            );
            return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
        }
    };
    let SqlValue::Str(sql) = stmt else {
        let error = SqlError::new(
            214,
            16,
            2,
            "Procedure expects parameter '@statement' of type 'ntext/nchar/nvarchar'.",
        );
        return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
    };
    let decls =
        match take_named(&mut named, &["params", "parameters"]).or_else(|| positional.next()) {
            Some(SqlValue::Str(d)) => d,
            Some(SqlValue::Null) | None => String::new(),
            Some(_) => {
                let error = SqlError::new(
                    214,
                    16,
                    3,
                    "Procedure expects parameter '@params' of type 'ntext/nchar/nvarchar'.",
                );
                return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
            }
        };
    // Bind values: named ones by their own names, positional ones from the
    // declaration list, exactly as the RPC path binds unnamed wire values.
    let names = decl_names(&decls);
    let mut seeded: Vec<(String, SqlValue)> = named;
    for (i, value) in positional.enumerate() {
        let Some(name) = names.get(i) else {
            let error = SqlError::new(
                8144,
                16,
                2,
                "Procedure or function has too many arguments specified.",
            );
            return Err(ExecError::Own(doom_per_rule(txn_ctx, error)));
        };
        seeded.push((name.clone(), value));
    }
    let statements =
        truthdb_sql::parse(&sql).map_err(|e| ExecError::Own(doom_per_rule(txn_ctx, e)))?;

    // The inner batch is its own variable scope, on the shared transaction —
    // and SET options revert at scope exit, as SQL Server reverts them: an
    // inner SET (XACT_ABORT, ISOLATION LEVEL, SHOWPLAN) must not outlive the
    // EXEC, or a post-EXEC statement would run under an isolation the up-front
    // lock analysis never saw.
    let outer_database = txn_ctx.database.clone();
    let outer_database_id = txn_ctx.database_id();
    let outer_vars = std::mem::take(&mut txn_ctx.variables);
    let outer_table_vars = std::mem::take(&mut txn_ctx.table_variables);
    let outer_xact_abort = txn_ctx.xact_abort;
    let outer_nocount = txn_ctx.nocount;
    let outer_isolation = txn_ctx.isolation;
    let outer_showplan = txn_ctx.showplan_text;
    for (name, value) in seeded {
        let key = name.trim_start_matches('@').to_ascii_lowercase();
        let column_type = value::infer_type(std::slice::from_ref(&value));
        txn_ctx.variables.insert(key, (column_type, value));
    }
    // Dynamic SQL run from a trigger body does not see inserted/deleted.
    let _trigger_shadow = TriggerScope::clear();
    // Dynamic SQL does NOT ownership-chain: reset the chaining depth so its
    // statements are permission-checked as the caller's own, even when this
    // sp_executesql sits inside a procedure body.
    let _dynamic = DynamicScope::enter();
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    let result = if depth > 32 {
        let error = SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        );
        Err(ExecError::Own(doom_per_rule(txn_ctx, error)))
    } else {
        // An inner RETURN exits the inner batch only (Break/Continue cannot
        // escape — the inner parse rejects them, its own 135/136 scope). An
        // error crossing out already carries every decision: dooming, and by
        // crossing at all, termination of the whole nest.
        run_block(storage, &statements, txn_ctx, run, in_try)
            .and_then(end_of_scope)
            .map_err(ExecError::Inner)
    };
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    txn_ctx.variables = outer_vars;
    txn_ctx.table_variables = outer_table_vars;
    txn_ctx.xact_abort = outer_xact_abort;
    txn_ctx.nocount = outer_nocount;
    txn_ctx.isolation = outer_isolation;
    txn_ctx.showplan_text = outer_showplan;
    // A USE inside the dynamic batch is scoped to it (SQL Server's rule):
    // the caller's database context comes back at scope exit — and with it,
    // agreement with the lock analysis that resolved the OUTER batch.
    txn_ctx.set_current_database(outer_database, outer_database_id);
    result
}

/// The ONE place a failed statement's fate is decided — continue the batch
/// (`Ok(())`), or end it (`Err`, dooming already applied). The doom decision
/// needs the statement's KIND (RAISERROR is exempt from XACT_ABORT; THROW is
/// batch-terminating without dooming), so every decide-now error site funnels
/// here: the generic statement arm and IF/WHILE condition failures. (EXEC
/// boundary errors do NOT — theirs were decided at the source, in the inner
/// `run_block` or `doom_per_rule`.)
fn statement_error_ladder(
    statement: &Statement,
    error: SqlError,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<(), SqlError> {
    // A cancelled statement aborts the batch immediately: key on the cancel
    // marker, not any flag, so an Attention landing concurrently with an
    // unrelated failure cannot suppress that failure's dooming. A cancel is
    // not a SQL error, so `@@ERROR` is untouched.
    if error.number == CANCEL_ERROR {
        return Err(error);
    }
    txn_ctx.record_error(error.number);
    // A durability failure wedged the store (a flush inside the statement,
    // e.g. before a snapshot capture): never continue past a lost commit.
    if run.durability_failed {
        return Err(error);
    }
    // Severity >= 20 is fatal to the connection: it bypasses TRY (the
    // TryCatch arm refuses it too), dooms the transaction, and the protocol
    // layer closes the stream after delivering it.
    if error.level >= FATAL_SEVERITY {
        if txn_ctx.in_txn() {
            txn_ctx.doomed = true;
        }
        return Err(error);
    }
    // The doom decision is made HERE, where the failing statement's kind is
    // known — never re-derived at the TRY boundary, which cannot see it.
    // `SET XACT_ABORT` (or severity >= 17) dooms; RAISERROR is exempt by
    // definition (SQL Server: "errors raised by RAISERROR are not affected
    // by SET XACT_ABORT") and never dooms.
    let dooms = !matches!(statement, Statement::RaiseError(_))
        && (txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY);
    if txn_ctx.in_txn() && dooms {
        txn_ctx.doomed = true;
    }
    // Inside a TRY, the error then transfers to the matching CATCH (which
    // sees XACT_STATE() = -1 when it doomed). The CATCH runs more statements,
    // so a result set this one already started streaming must be closed.
    if in_try {
        run.abort_open_rowset(txn_ctx.in_txn());
        return Err(error);
    }
    // RAISERROR is statement-scope: the batch always continues.
    if matches!(statement, Statement::RaiseError(_)) {
        run.abort_open_rowset(txn_ctx.in_txn());
        run.last_error = Some(error);
        return Ok(());
    }
    // THROW always terminates the batch — even when it does not doom the
    // transaction (XACT_ABORT OFF leaves it open and committable later).
    if matches!(statement, Statement::Throw(_)) {
        return Err(error);
    }
    // Other statements: a non-dooming in-transaction error rolls back only
    // the statement and the batch continues; a dooming one ends the batch
    // (only ROLLBACK is then accepted, error 3930). This must stay keyed on the
    // ERROR (its severity / XACT_ABORT), NOT on whether the transaction is
    // already doomed: a doomed transaction still runs a CATCH's reads and
    // statement-terminating errors (division by zero, conversion) so the CATCH
    // can reach `IF XACT_STATE() <> 0 ROLLBACK` — terminating the batch on those
    // would leave the uncommittable transaction open holding its locks.
    if txn_ctx.in_txn() && !dooms {
        run.abort_open_rowset(txn_ctx.in_txn());
        run.last_error = Some(error);
        return Ok(());
    }
    Err(error)
}

/// Enters the versioned-read scopes for an IF/WHILE condition that reads
/// tables — the SAME rules a SELECT gets in `exec_statement_streamed`: under
/// RCSI the condition reads its own statement snapshot; under SNAPSHOT
/// isolation it establishes/uses the transaction snapshot and enforces 3952.
/// Without this the condition read holds NEITHER lock nor snapshot (analysis
/// assumes versioned reads and drops Table S) — a live dirty read, the
/// Stage 13 seam class, caught by the control-flow review.
fn enter_condition_scopes<'a>(
    storage: &'a Storage,
    condition: &Expr,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<(Option<SnapshotScope<'a>>, Option<TxnSnapshotScope>), SqlError> {
    let mut tables = Vec::new();
    collect_expr_tables(condition, &mut tables);
    // A scalar function the condition calls may read tables through its body;
    // those reads must observe the same snapshot as a direct read (the lock
    // analysis already resolved them), so arm the scope when the condition
    // reaches any table directly OR through a called function.
    if tables.is_empty()
        && expr_function_read_ids(storage, txn_ctx.database_id(), condition).is_empty()
    {
        return Ok((None, None));
    }
    match txn_ctx.isolation() {
        Isolation::ReadCommitted if storage.rcsi_enabled() => {
            // The snapshot is the durable commit prefix: the session's own
            // just-committed statements must be durable before capture.
            run.flush(storage)?;
            Ok((
                Some(SnapshotScope::enter(
                    storage,
                    txn_ctx.txn.as_ref().map(StorageTxn::txn_id),
                )),
                None,
            ))
        }
        Isolation::Snapshot => {
            if !storage.snapshot_isolation_allowed() {
                if txn_ctx.in_txn() {
                    txn_ctx.doomed = true;
                }
                return Err(snapshot_not_allowed_error(&txn_ctx.database));
            }
            if txn_ctx.in_txn() {
                if txn_ctx.txn_snapshot.is_none() {
                    // First data access establishes the transaction's view —
                    // a condition read counts.
                    run.flush(storage)?;
                    let own = txn_ctx.txn.as_ref().map(StorageTxn::txn_id);
                    txn_ctx.txn_snapshot = Some(storage.capture_read_snapshot(own));
                }
                Ok((None, txn_ctx.txn_snapshot.map(TxnSnapshotScope::enter)))
            } else {
                run.flush(storage)?;
                Ok((Some(SnapshotScope::enter(storage, None)), None))
            }
        }
        // A readable STANDBY snapshots condition reads too (below the
        // RCSI/SNAPSHOT arms — see the statement arming): only the
        // last-applied-commit snapshot yields committed-state reads there.
        _ if storage.is_standby() => {
            run.flush(storage)?;
            Ok((Some(SnapshotScope::enter(storage, None)), None))
        }
        _ => Ok((None, None)),
    }
}

/// Evaluates an IF/WHILE condition: subqueries (EXISTS, scalar, IN) resolve
/// eagerly through the same machinery as WHERE-clause subqueries, then the
/// residual expression evaluates against the session context. T-SQL
/// three-valued: TRUE runs the branch; FALSE and NULL (UNKNOWN) do not.
fn eval_condition(
    storage: &Storage,
    condition: &Expr,
    txn_ctx: &TxnContext,
) -> Result<bool, SqlError> {
    let eval_ctx = txn_ctx.eval_context();
    let no_outer = |_: &str| -> Option<usize> { None };
    let resolved = substitute_correlated_in_expr(storage, condition, &no_outer, &[], &eval_ctx)?;
    match eval_constant(&resolved, &eval_ctx)? {
        SqlValue::Bool(taken) => Ok(taken),
        SqlValue::Null => Ok(false),
        _ => Err(SqlError::new(
            4145,
            15,
            1,
            "An expression of non-boolean type specified in a context where a condition is              expected.",
        )),
    }
}

/// How a statement block ended: normally, or via a control-flow statement
/// that must propagate to the construct that absorbs it (`WHILE` for
/// Break/Continue, the batch — later the procedure — for Return, the nearest
/// block holding the target label for `Goto`). TRY/CATCH and plain blocks pass
/// every non-Normal flow straight through (a `Goto` is first checked against the
/// current block's labels, then propagated).
#[derive(Clone, PartialEq, Eq)]
enum Flow {
    Normal,
    Break,
    Continue,
    Return,
    /// A `GOTO <label>` still looking for its target label.
    Goto(String),
}

/// What `run_block`'s loop should do with a flow bubbling up from a nested
/// construct: a `GOTO` to a label in this block jumps there; anything else
/// propagates to the enclosing block.
enum GotoAction {
    /// Resume at this statement index (a resolved `GOTO`).
    Jump(usize),
    /// The nested construct ended normally — fall through.
    Fall,
    /// Return this flow to the caller (Break/Continue/Return, or a `GOTO` to a
    /// label not defined in this block).
    Propagate(Flow),
}

fn resolve_goto(flow: Flow, labels: &std::collections::HashMap<String, usize>) -> GotoAction {
    match flow {
        Flow::Normal => GotoAction::Fall,
        Flow::Goto(label) => match labels.get(&label.to_ascii_lowercase()) {
            Some(&target) => GotoAction::Jump(target),
            None => GotoAction::Propagate(Flow::Goto(label)),
        },
        other => GotoAction::Propagate(other),
    }
}

/// A statement list run as its own scope — a batch, or a procedure / function /
/// trigger body — cannot be a GOTO target from outside and a GOTO cannot jump
/// out of it. A GOTO that reaches the end of such a scope unresolved references
/// a label defined nowhere in scope: error 133.
fn end_of_scope(flow: Flow) -> Result<(), SqlError> {
    match flow {
        Flow::Goto(label) => Err(SqlError::new(
            133,
            15,
            1,
            format!("A GOTO statement references the label '{label}:' which has not been defined."),
        )),
        _ => Ok(()),
    }
}

fn run_block(
    storage: &Storage,
    statements: &[Statement],
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
    in_try: bool,
) -> Result<Flow, SqlError> {
    // Label positions for GOTO. A jump sets the index to the label's position;
    // execution resumes there (the label statement itself is a no-op). A label
    // repeated in the same list is error 132.
    let mut labels: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (idx, s) in statements.iter().enumerate() {
        if let Statement::Label { name, .. } = s
            && labels.insert(name.to_ascii_lowercase(), idx).is_some()
        {
            return Err(SqlError::new(
                132,
                15,
                1,
                format!(
                    "The label '{name}:' has already been declared. Label names must be unique \
                     within a query batch or stored procedure."
                ),
            ));
        }
    }
    let mut i = 0;
    'stmts: while i < statements.len() {
        let statement = &statements[i];
        i += 1;
        // A TDS Attention (cancel) aborts the batch before the next statement.
        // It is never catchable — it propagates straight out, past any TRY.
        check_cancelled()?;
        if let Statement::Exec(exec) = statement {
            // The inner statements flow through `run_block` recursion, whose
            // own loop applies the per-statement flush and commit flag — the
            // same shape as TRY/CATCH dispatch. Errors take the ordinary
            // statement path: cancels and durability failures propagate, a
            // TRY transfers to CATCH, XACT_ABORT OFF continues the batch.
            match run_exec(storage, exec, txn_ctx, run, in_try) {
                Ok(()) => {}
                Err(exec_error) => {
                    // A failed EXEC sets @@ROWCOUNT to 0 like any failed
                    // statement.
                    txn_ctx.rowcount = 0;
                    let (error, from_inner) = match exec_error {
                        ExecError::Own(error) => (error, false),
                        ExecError::Inner(error) => (error, true),
                    };
                    if error.number == CANCEL_ERROR {
                        return Err(error);
                    }
                    // Inner errors were recorded at their raise site (the
                    // inner ladder), where the procedure frame was still
                    // live; re-recording here would blank ERROR_PROCEDURE().
                    if !from_inner {
                        txn_ctx.record_error(error.number);
                    }
                    if run.durability_failed {
                        return Err(error);
                    }
                    // Transfer to CATCH: decisions (dooming included) were
                    // already made where the error arose — per-statement in
                    // the inner `run_block`, or `doom_per_rule` for
                    // `run_exec`'s own errors. A fatal (>= 20) error is
                    // refused by the TryCatch arm's own filter.
                    if in_try {
                        run.abort_open_rowset(txn_ctx.in_txn());
                        return Err(error);
                    }
                    // An error crossing OUT of the inner batch already
                    // terminated it — and batch-abort scope is the whole
                    // nest, so the outer batch ends too (a THROW inside
                    // EXEC'd text ends the calling batch even when nothing
                    // doomed; non-dooming ordinary errors never cross — the
                    // inner run_block continued past them). Nothing is
                    // re-derived from severity here: the review showed that
                    // second derivation dropped THROW's termination.
                    if from_inner {
                        return Err(error);
                    }
                    // run_exec's OWN failure (unknown proc, 214, 8144, parse,
                    // depth): statement-scope at the EXEC site. Dooming was
                    // applied at the source; this decides only continuation.
                    let terminates = txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY;
                    if txn_ctx.in_txn() && !terminates {
                        run.abort_open_rowset(txn_ctx.in_txn());
                        run.last_error = Some(error);
                        continue;
                    }
                    return Err(error);
                }
            }
            continue;
        }
        match statement {
            Statement::Block { body, .. } => {
                match resolve_goto(run_block(storage, body, txn_ctx, run, in_try)?, &labels) {
                    GotoAction::Jump(t) => {
                        i = t;
                        continue 'stmts;
                    }
                    GotoAction::Propagate(flow) => return Ok(flow),
                    GotoAction::Fall => {}
                }
                continue;
            }
            Statement::If {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                // A successful condition evaluation resets `@@ERROR` (the IF
                // itself is a statement) — AFTER the condition read it, which
                // is what makes `IF @@ERROR <> 0` work.
                // A condition subquery reads table variables through the same
                // FROM path as a SELECT, so it needs the same read view armed —
                // the IF/WHILE arms bypass exec_statement_streamed, so arm here.
                let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                let taken = match enter_condition_scopes(storage, condition, txn_ctx, run)
                    .and_then(|_scopes| eval_condition(storage, condition, txn_ctx))
                {
                    Ok(taken) => taken,
                    Err(error) => {
                        txn_ctx.rowcount = 0;
                        statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                        continue;
                    }
                };
                txn_ctx.last_error = 0;
                let branch = if taken {
                    Some(then_branch)
                } else {
                    else_branch.as_ref()
                };
                if let Some(branch) = branch {
                    let flow =
                        run_block(storage, std::slice::from_ref(branch), txn_ctx, run, in_try)?;
                    match resolve_goto(flow, &labels) {
                        GotoAction::Jump(t) => {
                            i = t;
                            continue 'stmts;
                        }
                        GotoAction::Propagate(flow) => return Ok(flow),
                        GotoAction::Fall => {}
                    }
                }
                continue;
            }
            Statement::While {
                condition, body, ..
            } => {
                loop {
                    // A TDS Attention lands between iterations too — an
                    // infinite `WHILE 1 = 1` must die on cancel even when its
                    // body runs no cancellable statement.
                    check_cancelled()?;
                    // Re-armed each iteration: the body may INSERT into @t, and
                    // the next condition read must see the updated rows.
                    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                    let taken = match enter_condition_scopes(storage, condition, txn_ctx, run)
                        .and_then(|_scopes| eval_condition(storage, condition, txn_ctx))
                    {
                        Ok(taken) => taken,
                        Err(error) => {
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            break;
                        }
                    };
                    txn_ctx.last_error = 0;
                    if !taken {
                        break;
                    }
                    let flow =
                        run_block(storage, std::slice::from_ref(body), txn_ctx, run, in_try)?;
                    match flow {
                        Flow::Normal | Flow::Continue => {}
                        Flow::Break => break,
                        // RETURN or a GOTO leaves the loop: a GOTO to a label in
                        // this block jumps out of the WHILE to it, else propagate.
                        other => match resolve_goto(other, &labels) {
                            GotoAction::Jump(t) => {
                                i = t;
                                continue 'stmts;
                            }
                            GotoAction::Propagate(flow) => return Ok(flow),
                            GotoAction::Fall => {}
                        },
                    }
                }
                continue;
            }
            // The parser rejects BREAK/CONTINUE outside a WHILE (135/136), so
            // these only ever propagate up to an enclosing loop.
            Statement::Break { .. } => return Ok(Flow::Break),
            Statement::Continue { .. } => return Ok(Flow::Continue),
            // The parser rejects `RETURN <value>` outside a procedure (178);
            // inside one the status is stashed for `EXEC @rc =` to read.
            Statement::Return { value, .. } => {
                // A scalar function body's RETURN: evaluate its (mandatory)
                // value, coerce it to the declared return type, and stash it for
                // the caller. Nested user functions and subqueries in the RETURN
                // expression are rewritten to literals first, exactly like an
                // IF/WHILE condition.
                if let Some(return_type) = run.function_return_type {
                    let value = value
                        .as_ref()
                        .expect("a scalar function RETURN carries a value (parser-enforced)");
                    // A RETURN subquery reads table variables through the FROM
                    // path; arm the body's own (empty) view so it shadows the
                    // caller's rather than reading caller locals.
                    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
                    let eval_ctx = txn_ctx.eval_context();
                    let no_outer = |_: &str| -> Option<usize> { None };
                    let coerced =
                        substitute_correlated_in_expr(storage, value, &no_outer, &[], &eval_ctx)
                            .and_then(|bound| eval_constant(&bound, &eval_ctx))
                            .and_then(|raw| {
                                let datum =
                                    value::sql_to_datum(&raw, &return_type, "return value")?;
                                Ok(value::datum_to_sql(&datum, &return_type))
                            });
                    match coerced {
                        Ok(coerced) => {
                            txn_ctx.func_return = Some(coerced);
                            return Ok(Flow::Return);
                        }
                        Err(error) => {
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                    }
                }
                if let Some(value) = value {
                    let eval_ctx = txn_ctx.eval_context();
                    match eval_constant(value, &eval_ctx) {
                        Ok(SqlValue::Int(status))
                            if (i32::MIN as i64..=i32::MAX as i64).contains(&status) =>
                        {
                            txn_ctx.proc_return = Some(status)
                        }
                        // A RETURN value outside int range overflows, as SQL
                        // Server does (8115) — the status is an int. Without this
                        // the out-of-range value would be stashed and later fail
                        // to encode (and, on the RPC path, read back as NULL and
                        // be mistaken for a procedure that never completed).
                        Ok(SqlValue::Int(_)) => {
                            let error = SqlError::new(
                                8115,
                                16,
                                2,
                                "Arithmetic overflow error converting expression to data type int.",
                            );
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                        Ok(SqlValue::Null) => {
                            // SQL Server warns and returns 0; we return 0.
                            txn_ctx.proc_return = Some(0);
                        }
                        Ok(_) | Err(_) => {
                            let error =
                                eval_constant(value, &eval_ctx).err().unwrap_or_else(|| {
                                    SqlError::new(
                                        257,
                                        16,
                                        3,
                                        "The RETURN status must be an integer.",
                                    )
                                });
                            txn_ctx.rowcount = 0;
                            statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
                            continue;
                        }
                    }
                }
                return Ok(Flow::Return);
            }
            // A label is a no-op when reached in sequence.
            Statement::Label { .. } => continue,
            // GOTO jumps to a label in this block, or propagates to an enclosing
            // one (the batch top turns an unresolved GOTO into error 133).
            Statement::Goto { label, .. } => match labels.get(&label.to_ascii_lowercase()) {
                Some(&target) => {
                    i = target;
                    continue 'stmts;
                }
                None => return Ok(Flow::Goto(label.clone())),
            },
            _ => {}
        }
        if let Statement::TryCatch {
            try_block,
            catch_block,
            ..
        } = statement
        {
            match run_block(storage, try_block, txn_ctx, run, true) {
                Ok(Flow::Normal) => {}
                // BREAK/CONTINUE/RETURN/GOTO cross a TRY without running its
                // CATCH; a GOTO to a label in this block jumps there.
                Ok(flow) => match resolve_goto(flow, &labels) {
                    GotoAction::Jump(t) => {
                        i = t;
                        continue 'stmts;
                    }
                    GotoAction::Propagate(flow) => return Ok(flow),
                    GotoAction::Fall => {}
                },
                // An Attention that landed inside the TRY block is not caught.
                Err(cancel) if cancel.number == CANCEL_ERROR => return Err(cancel),
                // A durability failure wedged the store: no CATCH swallows a
                // lost commit (the old batch-end fsync ran past every TRY).
                Err(error) if run.durability_failed => return Err(error),
                // Severity >= 20 is fatal to the connection: no CATCH sees
                // it. Already recorded (and doomed) at the raise site.
                Err(error) if error.level >= FATAL_SEVERITY => return Err(error),
                Err(error) => {
                    // The failed statement's own writes were already undone to
                    // its savepoint (`rel_statement_scoped`), and the doom
                    // decision was made where the statement failed — the inner
                    // `run_block` knows the statement's kind (RAISERROR is
                    // exempt from XACT_ABORT), this boundary does not. Control
                    // transfers to CATCH either way; a doomed transaction
                    // reports XACT_STATE() = -1 there.
                    txn_ctx.push_error(&error);
                    // The CATCH block runs in the *enclosing* try-context: its
                    // own errors are not caught here, so they propagate to an
                    // outer CATCH (or end the batch) per `in_try`.
                    let caught = run_block(storage, catch_block, txn_ctx, run, in_try);
                    txn_ctx.pop_error();
                    match resolve_goto(caught?, &labels) {
                        GotoAction::Jump(t) => {
                            i = t;
                            continue 'stmts;
                        }
                        GotoAction::Propagate(flow) => return Ok(flow),
                        GotoAction::Fall => {}
                    }
                }
            }
            continue;
        }
        // A statement that can open a result set is a durability point: the
        // deferred DONEs must reach the stream before its columns do, and any
        // commit made so far must be fsync-durable before rows that can carry
        // its state (an identity value, via SCOPE_IDENTITY()) leave the server.
        if produces_rowset(statement) || matches!(statement, Statement::RaiseError(_)) {
            run.flush(storage)?;
        }
        // Flag durability by statement kind, before matching the result: a
        // write/DDL/COMMIT can commit even when it then errors — an autocommit
        // statement, an identity reservation (its own mini-commit, made even
        // inside an open transaction and even if the row insert later fails),
        // or the outermost COMMIT.
        run.committed |= statement_may_commit(statement);
        match exec_statement_streamed(storage, statement, txn_ctx, run) {
            Ok(outcome) => {
                // The statement succeeded: `@@ERROR` reads 0 — except after a
                // severity <= 10 RAISERROR, which set it itself (0, or 50000
                // under SETERROR).
                if !matches!(statement, Statement::RaiseError(_)) {
                    txn_ctx.last_error = 0;
                }
                let in_transaction = txn_ctx.in_txn();
                let command = done_command(statement);
                // `SET NOCOUNT ON` suppresses the DONE's count on the wire;
                // rows/results are untouched. `@@ROWCOUNT` records the true
                // count either way (NOCOUNT does not change it).
                let nocount = txn_ctx.nocount;
                let wire_count =
                    |count: u64| -> Option<u64> { if nocount { None } else { Some(count) } };
                // `USE` succeeded: earlier statements' deferred DONEs go out
                // first, then the database-context ENVCHANGE + 5701 INFO the
                // client (SSMS) expects, then the USE's own DONE below —
                // SQL Server's exact order.
                if let Statement::Use { .. } = statement {
                    run.flush(storage)?;
                    run.database_context(&txn_ctx.database);
                }
                match outcome {
                    StatementOutcome::Streamed { rows } => {
                        txn_ctx.rowcount = rows as i64;
                        run.done(wire_count(rows), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::Rows(rowset)) => {
                        let count = rowset.rows.len() as u64;
                        txn_ctx.rowcount = count as i64;
                        run.open_rowset(rowset.columns);
                        run.rows(rowset.rows);
                        run.done(wire_count(count), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::RowsAffected(n)) => {
                        txn_ctx.rowcount = n as i64;
                        run.done(wire_count(n), in_transaction, command);
                    }
                    StatementOutcome::Result(StatementResult::Done) => {
                        // A simple variable assignment (`SET @x = ...`) sets
                        // @@ROWCOUNT to 1 — recorded by exec_set, preserved
                        // here; every other Done statement resets it to 0.
                        if !matches!(
                            statement,
                            Statement::Set(SetStatement::Variable { .. }) | Statement::Declare(_)
                        ) {
                            txn_ctx.rowcount = 0;
                        }
                        run.done(None, in_transaction, command);
                    }
                }
            }
            Err(error) => {
                // A failed statement sets @@ROWCOUNT to 0, as SQL Server does.
                txn_ctx.rowcount = 0;
                statement_error_ladder(statement, error, txn_ctx, run, in_try)?;
            }
        }
    }
    Ok(Flow::Normal)
}

/// One executed statement's outcome, from [`exec_statement_streamed`].
enum StatementOutcome {
    /// The statement's whole result, for the caller to emit.
    Result(StatementResult),
    /// A streamed `SELECT`: its columns and rows already left through the
    /// emitter as the scan produced them; only its DONE remains.
    Streamed { rows: u64 },
}

thread_local! {
    /// The running statement's read snapshot (Stage 13), when its isolation
    /// is versioned — RCSI's per-statement view. Thread-local rather than
    /// threaded through every read path: a batch executes synchronously on
    /// one worker thread, and every nested read of the statement (subqueries,
    /// views, derived tables, correlated re-evaluation) shares the statement
    /// snapshot by construction.
    static CURRENT_SNAPSHOT: std::cell::Cell<Option<ReadSnapshot>> =
        const { std::cell::Cell::new(None) };
}

/// The running statement's read snapshot, if it reads versioned.
fn current_snapshot() -> Option<ReadSnapshot> {
    CURRENT_SNAPSHOT.get()
}

thread_local! {
    /// The running statement's table variables (the session's, shared read-only
    /// for the statement). Thread-local for the same reason as CURRENT_SNAPSHOT:
    /// a batch runs on one worker thread, and the FROM-source builders carry
    /// only an EvalContext (a truthdb-sql type that cannot hold core `Datum`
    /// rows), so the store cannot ride through it.
    static CURRENT_TABLE_VARS: std::cell::RefCell<
        Option<std::rc::Rc<std::collections::HashMap<String, TableVar>>>,
    > = const { std::cell::RefCell::new(None) };
}

/// The table variable `@name` visible to the running statement, cloned out for a
/// FROM read (an in-memory rowset).
fn current_table_var(name: &str) -> Option<TableVar> {
    let key = name.trim_start_matches('@').to_ascii_lowercase();
    CURRENT_TABLE_VARS.with(|c| c.borrow().as_ref().and_then(|m| m.get(&key).cloned()))
}

/// Installs the statement's table variables for its execution, restoring the
/// prior installation on drop (scopes can nest — a subquery or TVF body reads
/// within the caller's — so restore rather than clear).
struct TableVarScope {
    prev: Option<std::rc::Rc<std::collections::HashMap<String, TableVar>>>,
}

impl TableVarScope {
    fn enter(vars: std::rc::Rc<std::collections::HashMap<String, TableVar>>) -> Self {
        let prev = CURRENT_TABLE_VARS.with(|c| c.borrow_mut().replace(vars));
        TableVarScope { prev }
    }
}

impl Drop for TableVarScope {
    fn drop(&mut self) {
        CURRENT_TABLE_VARS.with(|c| *c.borrow_mut() = self.prev.take());
    }
}

/// Installs `vars` as the table-variable read view for the returned guard's
/// lifetime — the SINGLE arming rule shared by every path that can read a table
/// variable: ordinary statements, IF/WHILE conditions, scalar-function RETURN
/// expressions, and TVF bodies. Armed when `vars` is non-empty OR an outer scope
/// is already armed. The second clause is the correctness hinge: a function,
/// procedure, or TVF body runs with a fresh (empty) table-variable set, and it
/// must SHADOW the caller's view — not inherit it — so its `FROM @t` resolves
/// against its own (empty) locals and errors 1087, never the caller's rows.
/// When neither holds (the common no-table-variable batch) it arms nothing, so
/// the hot path pays only a thread-local read.
fn arm_table_var_view(vars: &std::collections::HashMap<String, TableVar>) -> Option<TableVarScope> {
    let outer_armed = CURRENT_TABLE_VARS.with(|c| c.borrow().is_some());
    (!vars.is_empty() || outer_armed).then(|| TableVarScope::enter(std::rc::Rc::new(vars.clone())))
}

/// The `inserted`/`deleted` pseudo-tables a firing trigger body reads: the new
/// and old row images of the statement that fired it, with the parent table's
/// schema. Rows are in schema order, exactly like a base-table row.
struct TriggerTables {
    schema: Schema,
    inserted: Vec<Vec<Datum>>,
    deleted: Vec<Vec<Datum>>,
    /// The 0-based indices of the columns the firing statement touched.
    updated: Vec<usize>,
}

thread_local! {
    /// The `inserted`/`deleted` view visible to the running trigger body (like
    /// CURRENT_TABLE_VARS for table variables — a batch runs on one thread and
    /// the FROM-source builders carry only an EvalContext).
    static CURRENT_TRIGGER_TABLES: std::cell::RefCell<Option<std::rc::Rc<TriggerTables>>> =
        const { std::cell::RefCell::new(None) };
}

/// The `inserted` or `deleted` pseudo-table rows visible to the running trigger,
/// as a materialized source, if a trigger scope is armed and `name` is one of
/// them. Returns `None` for any other name (falls through to catalog resolution).
fn current_trigger_source(name: &str, qualifier: &str) -> Option<Source> {
    let which = name.to_ascii_lowercase();
    if which != "inserted" && which != "deleted" {
        return None;
    }
    CURRENT_TRIGGER_TABLES.with(|c| {
        let borrow = c.borrow();
        let tables = borrow.as_ref()?;
        let rows = if which == "inserted" {
            tables.inserted.clone()
        } else {
            tables.deleted.clone()
        };
        let count = tables.schema.columns.len();
        let columns = tables
            .schema
            .columns
            .iter()
            .map(|col| ResultColumn {
                name: col.name.clone(),
                column_type: col.column_type,
            })
            .collect();
        let collations = tables
            .schema
            .columns
            .iter()
            .map(|col| col.collation.clone())
            .collect();
        Some(Source {
            columns,
            qualifiers: vec![Some(qualifier.to_string()); count],
            collations,
            rows: SourceRows::Materialized(rows),
        })
    })
}

/// Installs the `inserted`/`deleted` view for a trigger body's execution,
/// restoring the prior installation on drop (a nested trigger's body shadows the
/// outer's — restore rather than clear).
struct TriggerScope {
    prev: Option<std::rc::Rc<TriggerTables>>,
}

impl TriggerScope {
    fn enter(tables: std::rc::Rc<TriggerTables>) -> Self {
        let prev = CURRENT_TRIGGER_TABLES.with(|c| c.borrow_mut().replace(tables));
        TriggerScope { prev }
    }

    /// Clears the `inserted`/`deleted` view for a stored-object body (a
    /// procedure, function, TVF, or view called from within a trigger body):
    /// those pseudo-tables are visible only in the trigger's OWN statements, not
    /// in objects it calls. Restores the prior view on drop. A no-op (cheap) when
    /// no trigger scope is armed.
    fn clear() -> Self {
        let prev = CURRENT_TRIGGER_TABLES.with(|c| c.borrow_mut().take());
        TriggerScope { prev }
    }
}

impl Drop for TriggerScope {
    fn drop(&mut self) {
        CURRENT_TRIGGER_TABLES.with(|c| *c.borrow_mut() = self.prev.take());
    }
}

thread_local! {
    /// The row images captured by the DML that is currently firing triggers, so
    /// exec_insert/update/delete can populate `inserted`/`deleted` without a
    /// signature change. Armed by the firing wrapper ONLY when the target table
    /// has triggers — the common no-trigger path leaves this `None` (no clone).
    static TRIGGER_CAPTURE: std::cell::RefCell<Option<CapturedImages>> =
        const { std::cell::RefCell::new(None) };
}

/// New (`inserted`) and old (`deleted`) row images collected during a DML that
/// has triggers to fire, plus the indices of the columns the statement touched
/// (its SET list, or every inserted column) for `UPDATE()`/`COLUMNS_UPDATED()`.
#[derive(Default)]
struct CapturedImages {
    inserted: Vec<Vec<Datum>>,
    deleted: Vec<Vec<Datum>>,
    updated: Vec<usize>,
}

/// Records row images into the active capture, if one is armed. `f` builds the
/// (inserted, deleted) images for a statement; it runs only when capture is on,
/// so the no-trigger path pays nothing.
fn capture_trigger_images(f: impl FnOnce() -> (Vec<Vec<Datum>>, Vec<Vec<Datum>>)) {
    TRIGGER_CAPTURE.with(|c| {
        let mut borrow = c.borrow_mut();
        if let Some(images) = borrow.as_mut() {
            let (ins, del) = f();
            images.inserted.extend(ins);
            images.deleted.extend(del);
        }
    });
}

/// Records the indices of the columns a firing UPDATE's SET list (or an INSERT's
/// target columns) touched, for `UPDATE()`/`COLUMNS_UPDATED()`, if capture is on.
fn capture_trigger_updated(indices: Vec<usize>) {
    TRIGGER_CAPTURE.with(|c| {
        if let Some(images) = c.borrow_mut().as_mut() {
            images.updated = indices;
        }
    });
}

/// The columns the firing trigger's statement touched, resolved against the
/// parent table's schema — the value behind `UPDATE()`/`COLUMNS_UPDATED()` in a
/// trigger body. `None` outside a trigger.
fn current_trigger_updated_columns() -> Option<truthdb_sql::eval::UpdatedColumns> {
    CURRENT_TRIGGER_TABLES.with(|c| {
        let borrow = c.borrow();
        let tables = borrow.as_ref()?;
        Some(truthdb_sql::eval::UpdatedColumns {
            columns: tables
                .schema
                .columns
                .iter()
                .map(|col| col.name.clone())
                .collect(),
            touched: tables.updated.iter().copied().collect(),
        })
    })
}

thread_local! {
    /// The object_ids of triggers whose bodies are currently on the stack. With
    /// recursive triggers OFF (the default), a trigger must not re-fire itself
    /// (direct recursion) — a trigger on T whose body DMLs T is suppressed for
    /// that same trigger. Nested triggers on OTHER tables are not affected.
    static FIRING_TRIGGERS: std::cell::RefCell<Vec<u32>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Statement-scoped snapshot registration: capture on entry, and release —
/// pruning must not wait on a statement that errored — on every exit path.
struct SnapshotScope<'a> {
    storage: &'a Storage,
    seq: u64,
    /// The snapshot that was current when this scope was entered, restored on
    /// exit. Scopes can nest — a scalar function's body statement runs under the
    /// caller's active statement/transaction snapshot — so a nested scope must
    /// restore the caller's snapshot on drop, not erase it.
    prev: Option<ReadSnapshot>,
}

impl<'a> SnapshotScope<'a> {
    fn enter(storage: &'a Storage, own_txn: Option<u64>) -> Self {
        let prev = CURRENT_SNAPSHOT.get();
        let snap = storage.capture_read_snapshot(own_txn);
        CURRENT_SNAPSHOT.set(Some(snap));
        SnapshotScope {
            storage,
            seq: snap.seq,
            prev,
        }
    }
}

impl Drop for SnapshotScope<'_> {
    fn drop(&mut self) {
        CURRENT_SNAPSHOT.set(self.prev);
        self.storage.release_read_snapshot(self.seq);
    }
}

/// Statement-scoped view of a TRANSACTION's snapshot (SNAPSHOT isolation):
/// sets the thread-local for this statement and restores the prior one on exit
/// (see [`SnapshotScope::prev`]), but the registration lives with the
/// transaction, not the statement.
struct TxnSnapshotScope {
    prev: Option<ReadSnapshot>,
}

impl TxnSnapshotScope {
    fn enter(snap: ReadSnapshot) -> Self {
        let prev = CURRENT_SNAPSHOT.get();
        CURRENT_SNAPSHOT.set(Some(snap));
        TxnSnapshotScope { prev }
    }
}

impl Drop for TxnSnapshotScope {
    fn drop(&mut self) {
        CURRENT_SNAPSHOT.set(self.prev);
    }
}

/// Whether a statement touches any base table: DML always does; a SELECT
/// only when its FROM/subqueries name one. `SELECT 1` under SNAPSHOT must
/// neither raise 3952 nor establish the transaction's snapshot — SQL Server
/// defers both to the first read of an actual object.
fn statement_reads_tables(storage: &Storage, db_id: u32, statement: &Statement) -> bool {
    match statement {
        Statement::Select(select) => select_reads_tables(storage, db_id, select),
        // An INSERT whose TARGET is a table variable writes only session memory,
        // so — unlike a base-table INSERT — it is not itself a data access; but a
        // `SELECT` source still reads real tables and must arm the snapshot.
        Statement::Insert(insert) if insert.table.value.starts_with('@') => match &insert.source {
            InsertSource::Select(select) => select_reads_tables(storage, db_id, select),
            _ => false,
        },
        _ => true,
    }
}

/// Whether a SELECT reads any real table — directly (FROM/subqueries) or through
/// a scalar function it calls. A `@t` table-variable source is session-local and
/// is not counted (it neither locks nor snapshots).
fn select_reads_tables(storage: &Storage, db_id: u32, select: &Select) -> bool {
    let expanded = expand_ctes(select);
    let mut tables = Vec::new();
    collect_locked_tables(&expanded, &mut tables);
    !tables.is_empty() || !select_function_read_ids(storage, db_id, &expanded).is_empty()
}

/// SQL Server 3952: SNAPSHOT isolation used while the database does not
/// allow it — raised at data access, not at SET, exactly as SQL Server does.
fn snapshot_not_allowed_error(database: &str) -> SqlError {
    SqlError::new(
        3952,
        16,
        1,
        format!(
            "Snapshot isolation transaction failed accessing database '{database}' because \
             snapshot isolation is not allowed in this database. Use ALTER DATABASE to allow \
             snapshot isolation."
        ),
    )
}

/// Runs one statement. A plain `SELECT` the scan planner accepts streams its
/// rows through `run` as the scan reads them — the whole point of the event
/// stream: the client sees rows while the scan runs, and the statement's peak
/// memory is one chunk, not the result. Everything else executes exactly as
/// before and returns its materialized result.
fn exec_statement_streamed(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<StatementOutcome, SqlError> {
    // Versioned reads (Stage 13). RCSI: a SELECT under READ COMMITTED with
    // the option on reads a per-statement snapshot instead of blocking on
    // writers' locks (DML and the reads inside it stay lock-based —
    // conservative versus SQL Server; the write locks subsume what
    // versioning would relax). SNAPSHOT isolation: every data-access
    // statement shares the transaction's snapshot, captured at its first
    // data access; outside a transaction each statement is its own.
    let data_access = matches!(
        statement,
        Statement::Select(_) | Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_)
    );
    let mut _stmt_scope = None;
    let mut _txn_scope = None;
    // Make the running context's table variables visible to this statement's
    // FROM reads. The clone is the statement's read view; INSERT/UPDATE write
    // the real store on TxnContext. Inside a function/procedure body (fresh,
    // empty table variables) this shadows the caller's view with an empty one,
    // so the body cannot read the caller's @t — see arm_table_var_view.
    let _table_var_scope = arm_table_var_view(&txn_ctx.table_variables);
    match txn_ctx.isolation() {
        Isolation::ReadCommitted
            if matches!(statement, Statement::Select(_)) && storage.rcsi_enabled() =>
        {
            // The snapshot is the durable commit prefix, so the session's
            // own just-committed statements must be fsync-durable before
            // capture or the statement would not see them. Rowset-producing
            // SELECTs already flushed in `run_block`; this covers assignment
            // SELECTs (and then no-ops when nothing committed since the
            // last durability point).
            run.flush(storage)?;
            _stmt_scope = Some(SnapshotScope::enter(
                storage,
                txn_ctx.txn.as_ref().map(StorageTxn::txn_id),
            ));
        }
        Isolation::Snapshot
            if data_access && statement_reads_tables(storage, txn_ctx.database_id(), statement) =>
        {
            if !storage.snapshot_isolation_allowed() {
                if txn_ctx.in_txn() {
                    txn_ctx.doomed = true;
                }
                return Err(snapshot_not_allowed_error(&txn_ctx.database));
            }
            if txn_ctx.in_txn() {
                if txn_ctx.txn_snapshot.is_none() {
                    // First data access establishes the transaction's view.
                    run.flush(storage)?;
                    let own = txn_ctx.txn.as_ref().map(StorageTxn::txn_id);
                    txn_ctx.txn_snapshot = Some(storage.capture_read_snapshot(own));
                }
                _txn_scope = txn_ctx.txn_snapshot.map(TxnSnapshotScope::enter);
            } else {
                // Autocommit: the statement is its own transaction, so its
                // snapshot is statement-scoped, like RCSI's.
                run.flush(storage)?;
                _stmt_scope = Some(SnapshotScope::enter(storage, None));
            }
        }
        // A readable STANDBY snapshots every table-reading statement — not
        // just SELECTs: cursors, table-variable INSERT ... SELECT sources, and
        // function bodies read too — regardless of the session's isolation
        // (redo leaves the primary's in-flight rows on its pages, and shipped
        // transactions hold no local locks; only the version-store snapshot at
        // the last applied commit yields committed-state reads). Ordered
        // BELOW the RCSI/SNAPSHOT arms so a SNAPSHOT session on a standby
        // keeps its transaction-lifetime view.
        _ if statement_reads_tables(storage, txn_ctx.database_id(), statement)
            && storage.is_standby() =>
        {
            run.flush(storage)?;
            _stmt_scope = Some(SnapshotScope::enter(storage, None));
        }
        _ => {}
    }
    exec_statement_streamed_inner(storage, statement, txn_ctx, run)
}

fn exec_statement_streamed_inner(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<StatementOutcome, SqlError> {
    if let Statement::RaiseError(raise) = statement {
        return exec_raiserror(raise, txn_ctx, run);
    }
    // The streamed shape: a plain SELECT — no SHOWPLAN (its rows are the plan's,
    // not the table's), no assignment (routed to exec_select_assign) — that
    // `scan_plan` accepts. A doomed transaction still allows reads, so the gate
    // needs no doomed check for a SELECT.
    if let Statement::Select(select) = statement
        && !txn_ctx.showplan_text
        && !select
            .items
            .iter()
            .any(|i| matches!(i, SelectItem::Assign { .. }))
    {
        let eval_ctx = txn_ctx.eval_context();
        if let Some(plan) = scan_plan(storage, select, &eval_ctx) {
            let rows = scan_select_streamed(storage, &plan, select, &eval_ctx, run)?;
            return Ok(StatementOutcome::Streamed { rows });
        }
    }
    exec_statement(storage, statement, txn_ctx).map(StatementOutcome::Result)
}

/// Whether a statement can make a durable commit that the batch must fsync: any
/// write/DDL (its own autocommit, or an identity reservation's mini-commit even
/// inside a transaction) or a `COMMIT`. Conservative by design — it flags by
/// kind, not by transaction state, so a hidden mini-commit (e.g. identity) is
/// never missed. Reads, `BEGIN`, `ROLLBACK`, `SET` and `DECLARE` never commit.
fn statement_may_commit(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::AlterDatabase(_)
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
            | Statement::Exec(_)
            | Statement::Block { .. }
            | Statement::If { .. }
            | Statement::While { .. }
            | Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_)
            | Statement::SetTriggerState { .. }
            | Statement::Commit { .. }
    )
}

/// The table/database locks a batch needs, from its statements and the
/// session isolation level, deduped to the strongest mode per resource. The
/// engine acquires these up front (before running any statement) so a
/// conflicting batch can be parked and restarted cleanly.
///
/// A parse error yields no locks — execution re-parses and surfaces it.
/// `sys.*` views and unresolved tables take no lock (catalog reads are
/// unlocked; missing tables error at execution).
/// Object ids of the parent tables a table's foreign keys reference.
fn fk_parent_object_ids(storage: &Storage, def: &TableDef) -> Vec<u32> {
    def.foreign_keys
        .iter()
        .filter_map(|fk| resolve_table(storage, def.database_id, &fk.parent).map(|p| p.object_id))
        .collect()
}

/// Object ids of the tables whose foreign keys reference `parent_name`.
fn fk_child_object_ids(storage: &Storage, db_id: u32, parent_name: &str) -> Vec<u32> {
    storage
        .rel_tables()
        .into_iter()
        .filter(|t| {
            t.database_id == db_id
                && t.foreign_keys
                    .iter()
                    .any(|fk| fk.parent.eq_ignore_ascii_case(parent_name))
        })
        .map(|t| t.object_id)
        .collect()
}

/// True if any table has a foreign key referencing `name` — i.e. `name` is an
/// FK parent. Such a table keeps table-granular write locks so an FK
/// existence-read (Table IS on the parent) still serializes against a
/// concurrent change to the referenced row.
fn is_fk_parent(storage: &Storage, db_id: u32, name: &str) -> bool {
    !fk_child_object_ids(storage, db_id, name).is_empty()
}

/// Above this many row-lock keys for one statement, `analyze_locks` escalates to
/// a single table lock (SQL Server-style lock escalation) rather than flooding
/// the lock table.
const ROW_LOCK_ESCALATION_THRESHOLD: usize = 1000;

/// A key hash for the [`Resource::Row`] lock, from the row's clustered-key bytes.
fn row_key_hash(schema: &Schema, key_columns: &[usize], key_values: &[Datum]) -> Option<u64> {
    let bytes = crate::relstore::key::encode_key(schema, key_columns, key_values).ok()?;
    Some(xxh64(&bytes, 0))
}

/// True if the clustered key can be safely hashed for a row lock: no key column
/// is a floating type. REAL/FLOAT keys are excluded because `-0.0 == 0.0` (and
/// NaN) compare equal in evaluation but encode to distinct key bytes, so two
/// writers to one physical row could get distinct hashes and not serialize.
///
/// Character keys are safe even under a case-insensitive collation: the row-lock
/// hash is taken over the *folded* key (`encode_key` folds character keys by
/// collation, Stage 5), so `WHERE key = 'ABC'` and a concurrent write of `'abc'`
/// hash to the same row resource and serialize.
fn key_columns_row_lockable(schema: &Schema, key_columns: &[usize]) -> bool {
    key_columns.iter().all(|&i| {
        !matches!(
            schema.columns[i].column_type,
            ColumnType::Real | ColumnType::Float
        )
    })
}

/// True if a literal may pin a key column for a row lock: the executor's
/// equality must be a direct same-domain match so the lock key equals the stored
/// row's key. The hazard is a **character** key compared to a non-string literal:
/// the executor coerces the stored string to the literal's number (many strings
/// → one number: `'05' = 5`), while the lock key coerces the number to one
/// canonical string — opposite directions that disagree. So a character key
/// column requires a string literal; other domains coerce unambiguously (or
/// `sql_to_datum` errors and the caller falls back).
fn literal_pins_key(value: &SqlValue, column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::VarChar { .. } | ColumnType::NVarChar { .. } => {
            matches!(value, SqlValue::Str(_))
        }
        _ => true,
    }
}

/// True if the table has a secondary UNIQUE index. Such a table keeps
/// table-granular locks for INSERT/UPDATE: a Row X on the clustered key alone
/// would not serialize two writers colliding on the *unique index* key.
fn has_secondary_unique_index(def: &TableDef) -> bool {
    def.indexes.iter().any(|ix| ix.unique)
}

/// Evaluates a constant literal expression (`5`, `'x'`, `-3`, NULL, …) to a
/// value. Returns `None` for anything that is not a self-contained literal —
/// a column reference, variable, function call, or subquery — so the caller
/// falls back to a coarser (table) lock rather than a wrong row key.
fn eval_literal_const(expr: &Expr) -> Option<SqlValue> {
    if !is_literal_const(expr) {
        return None;
    }
    let empty: Vec<String> = Vec::new();
    eval::eval(expr, &[], &empty, &EvalContext::default()).ok()
}

/// True if `expr` is a self-contained literal (no columns/vars/functions/
/// subqueries): a literal, or a unary +/- over one.
fn is_literal_const(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Null
        | ExprKind::Literal(_) => true,
        ExprKind::Unary { expr: inner, .. } => is_literal_const(inner),
        _ => false,
    }
}

/// True if `expr` contains any subquery node (scalar, EXISTS, or IN (SELECT)).
fn expr_has_subquery(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => true,
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::Column(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => false,
        ExprKind::Unary { expr: e, .. }
        | ExprKind::IsNull { expr: e, .. }
        | ExprKind::Cast { expr: e, .. } => expr_has_subquery(e),
        ExprKind::Binary { left, right, .. } => expr_has_subquery(left) || expr_has_subquery(right),
        ExprKind::Like {
            expr: e, pattern, ..
        } => expr_has_subquery(e) || expr_has_subquery(pattern),
        ExprKind::InList { expr: e, list, .. } => {
            expr_has_subquery(e) || list.iter().any(expr_has_subquery)
        }
        ExprKind::Between {
            expr: e, low, high, ..
        } => expr_has_subquery(e) || expr_has_subquery(low) || expr_has_subquery(high),
        ExprKind::Function { args, .. } => args.iter().any(expr_has_subquery),
        ExprKind::Aggregate { arg, .. } => arg.as_deref().is_some_and(expr_has_subquery),
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            operand.as_deref().is_some_and(expr_has_subquery)
                || branches
                    .iter()
                    .any(|(w, r)| expr_has_subquery(w) || expr_has_subquery(r))
                || else_result.as_deref().is_some_and(expr_has_subquery)
        }
    }
}

/// The row-lock keys for an INSERT: `Some(hashes)` when the target is a
/// clustered table and every row supplies all key columns as constant literals
/// (so two concurrent inserters of *different* keys need not serialize).
/// `None` — fall back to a table lock — for a heap, an IDENTITY/defaulted key
/// (value is server-generated, unknown here), `INSERT ... SELECT`, a
/// non-constant key expression, or more keys than the escalation threshold.
fn insert_row_key_hashes(def: &TableDef, insert: &Insert) -> Option<Vec<u64>> {
    if def.key_columns.is_empty() {
        return None;
    }
    let InsertSource::Values(value_rows) = &insert.source else {
        return None;
    };
    let schema = def.schema().ok()?;
    if !key_columns_row_lockable(&schema, &def.key_columns) {
        return None;
    }
    let ncols = schema.columns.len();
    let identity_col = def.identity.map(|s| s.column);
    // Column index for each value position (explicit list, else all non-identity
    // columns in order — matching `exec_insert`).
    let target: Vec<usize> = match &insert.columns {
        Some(names) => names
            .iter()
            .map(|n| column_index(&schema, &n.value))
            .collect::<Option<Vec<_>>>()?,
        None => (0..ncols).filter(|i| Some(*i) != identity_col).collect(),
    };
    let mut hashes = Vec::with_capacity(value_rows.len());
    for row in value_rows {
        if row.len() != target.len() {
            return None; // arity mismatch — executor will error; table-lock it
        }
        let mut key_values = vec![Datum::Null; ncols];
        for &kc in &def.key_columns {
            if Some(kc) == identity_col {
                return None; // server-generated key value
            }
            let pos = target.iter().position(|&t| t == kc)?; // key not supplied
            let value = eval_literal_const(&row[pos])?;
            let column = &schema.columns[kc];
            if !literal_pins_key(&value, &column.column_type) {
                return None;
            }
            key_values[kc] = value::sql_to_datum(&value, &column.column_type, &column.name).ok()?;
        }
        hashes.push(row_key_hash(&schema, &def.key_columns, &key_values)?);
        if hashes.len() > ROW_LOCK_ESCALATION_THRESHOLD {
            return None;
        }
    }
    Some(hashes)
}

/// The single row-lock key for a point UPDATE/DELETE: `Some(hash)` when the
/// WHERE clause is a subquery-free conjunction that pins *every* clustered-key
/// column to a constant literal. `None` — fall back to a table lock — otherwise
/// (heap, partial/absent key predicate, range/OR/subquery predicate).
fn where_point_key_hash(def: &TableDef, where_clause: &Option<Expr>) -> Option<u64> {
    if def.key_columns.is_empty() {
        return None;
    }
    let where_clause = where_clause.as_ref()?;
    if expr_has_subquery(where_clause) {
        return None;
    }
    let schema = def.schema().ok()?;
    if !key_columns_row_lockable(&schema, &def.key_columns) {
        return None;
    }
    let mut conjuncts = Vec::new();
    flatten_and(where_clause, &mut conjuncts);
    let mut key_values = vec![Datum::Null; schema.columns.len()];
    let mut bound: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for conjunct in conjuncts {
        let ExprKind::Binary {
            op: ast::BinaryOp::Eq,
            left,
            right,
        } = &conjunct.kind
        else {
            continue;
        };
        let (name, value_expr) = match (&left.kind, &right.kind) {
            (ExprKind::Column(n), _) => (n, right.as_ref()),
            (_, ExprKind::Column(n)) => (n, left.as_ref()),
            _ => continue,
        };
        let Some(ci) = column_index(&schema, &name.value) else {
            continue;
        };
        if !def.key_columns.contains(&ci) {
            continue;
        }
        let Some(value) = eval_literal_const(value_expr) else {
            continue;
        };
        let column = &schema.columns[ci];
        if !literal_pins_key(&value, &column.column_type) {
            continue;
        }
        if let Ok(datum) = value::sql_to_datum(&value, &column.column_type, &column.name) {
            key_values[ci] = datum;
            bound.insert(ci);
        }
    }
    if def.key_columns.iter().any(|kc| !bound.contains(kc)) {
        return None; // not every key column pinned
    }
    row_key_hash(&schema, &def.key_columns, &key_values)
}

/// The row-lock key for a point UPDATE: as [`where_point_key_hash`], but only
/// when no assignment targets a key column (a key change moves the row, touching
/// two keys) and no assignment value contains a subquery (which would read rows
/// the single row lock does not cover).
fn update_row_key_hash(def: &TableDef, update: &Update) -> Option<u64> {
    let schema = def.schema().ok()?;
    for assignment in &update.assignments {
        let ci = column_index(&schema, &assignment.column.value)?;
        if def.key_columns.contains(&ci) || expr_has_subquery(&assignment.value) {
            return None;
        }
    }
    where_point_key_hash(def, &update.where_clause)
}

/// Collects the database ids every `USE` in the batch -- including one hidden
/// in LITERAL `sp_executesql` text, whose inner statements execute under it --
/// can switch to. Bounded like the analysis recursion; a non-literal dynamic
/// batch contributes nothing here (its analysis arm already locks the
/// database exclusively). Procedure bodies cannot contain USE (parser 154).
fn collect_use_targets(
    storage: &Storage,
    statements: &[Statement],
    depth: u32,
    dbs: &mut Vec<u32>,
) {
    if depth > 32 {
        return;
    }
    let mut flat = Vec::new();
    flatten_statements(statements, &mut flat);
    for statement in &flat {
        match statement {
            Statement::Use { database, .. } => {
                if let Some(id) = storage.rel_database_id_by_name(&database.value)
                    && !dbs.contains(&id)
                {
                    dbs.push(id);
                }
            }
            Statement::Exec(exec) => {
                if let Some(inner) = exec_literal_sql(exec)
                    && let Ok(parsed) = truthdb_sql::parse(&inner)
                {
                    collect_use_targets(storage, &parsed, depth + 1, dbs);
                }
            }
            _ => {}
        }
    }
}

pub fn analyze_locks(
    storage: &Storage,
    db_id: u32,
    sql: &str,
    isolation: Isolation,
) -> Vec<(Resource, LockMode)> {
    let Ok(parsed) = truthdb_sql::parse(sql) else {
        return Vec::new();
    };
    // A batch that switches databases mid-stream (`USE`) executes later
    // statements in the new context, but this analysis runs once, up front.
    // Resolve under EVERY database context the batch can reach and take the
    // union: over-locking is safe, under-locking is the 2PL hole. (A failed
    // USE leaves the old context — also covered, it is in the set.)
    let mut dbs = vec![db_id];
    collect_use_targets(storage, &parsed, 0, &mut dbs);
    if dbs.len() > 1 {
        let mut out: Vec<(Resource, LockMode)> = Vec::new();
        for db in dbs {
            let mut visited = std::collections::HashSet::new();
            let mut trigger_visited = std::collections::HashSet::new();
            for lock in analyze_statements_locks(
                storage,
                db,
                &parsed,
                isolation,
                &mut visited,
                &mut trigger_visited,
            ) {
                if !out.contains(&lock) {
                    out.push(lock);
                }
            }
        }
        return out;
    }
    // The visited set terminates recursive procedures. Keyed on (procedure,
    // effective analysis regime), NOT the name alone: a body's lock
    // contribution is ISOLATION-DEPENDENT (versioned RC contributes Database
    // IS; an escalated re-entry needs Table S), so a body re-entered under a
    // different regime must re-analyze — the review's HIGH showed a shared
    // body analyzed versioned first and then skipped under SERIALIZABLE,
    // executing with no Table S. The regime lattice is finite, so
    // termination survives.
    let mut visited = std::collections::HashSet::new();
    let mut trigger_visited = std::collections::HashSet::new();
    analyze_statements_locks(
        storage,
        db_id,
        &parsed,
        isolation,
        &mut visited,
        &mut trigger_visited,
    )
}

fn analyze_statements_locks(
    storage: &Storage,
    db_id: u32,
    parsed: &[Statement],
    isolation: Isolation,
    visited: &mut std::collections::HashSet<(String, Isolation)>,
    trigger_visited: &mut std::collections::HashSet<(u32, Isolation)>,
) -> Vec<(Resource, LockMode)> {
    // Flatten TRY/CATCH so the locks a batch needs are pre-acquired for the
    // statements inside its try/catch blocks too, not just the top level.
    let mut statements: Vec<&Statement> = Vec::new();
    flatten_statements(parsed, &mut statements);
    // Reads take shared locks except under READ UNCOMMITTED, which takes none.
    // A batch that raises the isolation level (e.g. `SET ISOLATION LEVEL
    // SERIALIZABLE; SELECT ...`) must lock its reads even if the session was
    // READ UNCOMMITTED on entry — otherwise the post-SET read would run
    // unlocked. We therefore take read locks unless the whole batch is READ
    // UNCOMMITTED: the session is RU and no SET raises it above RU.
    // SNAPSHOT is a versioned level, not a raise: a SET to it must not force
    // lock-based analysis (its whole point is to read without Table S).
    let escalates_reads = statements.iter().any(|s| {
        matches!(
            s,
            Statement::Set(SetStatement::IsolationLevel(level))
                if !matches!(level, IsolationLevel::ReadUncommitted | IsolationLevel::Snapshot)
        )
    });
    // A batch that SETs SNAPSHOT mid-stream still read-locks (statements
    // before the SET run at the session level, and batch analysis cannot see
    // the boundary) — but it must at least hold the Database IS fence, so an
    // RU session's `SET SNAPSHOT; SELECT` is not entirely lock-free.
    let sets_snapshot = statements.iter().any(|s| {
        matches!(
            s,
            Statement::Set(SetStatement::IsolationLevel(IsolationLevel::Snapshot))
        )
    });
    let reads_lock =
        !matches!(isolation, Isolation::ReadUncommitted) || escalates_reads || sets_snapshot;
    // Versioned reads take Database IS only — the DDL fence for the batch's
    // duration — and no Table S: READ COMMITTED under RCSI (per-statement
    // snapshots) and SNAPSHOT isolation (the transaction's snapshot). A batch
    // whose SET raises the level is analyzed lock-based (conservative: the
    // raise is seen here, the exact statement boundary is not).
    let versioned_reads = !escalates_reads
        && (matches!(isolation, Isolation::Snapshot)
            || (matches!(isolation, Isolation::ReadCommitted) && storage.rcsi_enabled()));
    // The isolation a fired trigger body (and any EXEC it makes) must be analyzed
    // under: an in-line SET that raises the level locks the body's reads too, so
    // forward a lock-based level whenever this batch locks reads — the SAME
    // correction the EXEC path applies. Without it a trigger body under a
    // versioned session (Snapshot / RCSI) would recompute versioned_reads=true
    // and drop the Table S it actually reads lock-based at runtime (a dirty read,
    // the Stage-13 seam class).
    let nested_isolation = if reads_lock {
        if matches!(isolation, Isolation::ReadCommitted | Isolation::Snapshot) && !escalates_reads {
            isolation
        } else {
            Isolation::RepeatableRead
        }
    } else {
        isolation
    };
    let mut needs: std::collections::HashMap<Resource, LockMode> = std::collections::HashMap::new();
    let mut add = |resource: Resource, mode: LockMode| {
        needs
            .entry(resource)
            .and_modify(|m| *m = m.combine(mode))
            .or_insert(mode);
    };
    for statement in statements.iter().copied() {
        match statement {
            Statement::Select(select) => {
                if !reads_lock {
                    continue;
                }
                // Lock every base table the query reads — the FROM clause AND
                // any subqueries in its expressions (WHERE/SELECT/HAVING/...).
                // CTEs are inlined first so their base tables are counted.
                let expanded = expand_ctes(select);
                let mut tables = Vec::new();
                collect_locked_tables(&expanded, &mut tables);
                for name in tables {
                    for oid in read_lock_object_ids(storage, db_id, &name.value) {
                        add(Resource::Database, LockMode::IntentShared);
                        if !versioned_reads {
                            add(Resource::Table(oid), LockMode::Shared);
                        }
                    }
                }
                // A scalar function the query calls reads tables through its
                // body; lock those up front too (2PL), or the body would read
                // with no lock held. read_lock_object_ids recurses the body.
                for oid in select_function_read_ids(storage, db_id, &expanded) {
                    add(Resource::Database, LockMode::IntentShared);
                    if !versioned_reads {
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            Statement::Insert(insert) => {
                if let Some(def) = resolve_table(storage, db_id, &insert.table.value) {
                    // Row X locks on each inserted key (two inserters of
                    // different keys then run concurrently under Table IX); a
                    // heap / IDENTITY / non-literal key falls back to Table X.
                    // A table referenced as an FK parent keeps Table X so an FK
                    // existence-read (Table IS) still serializes against it; a
                    // secondary UNIQUE index likewise needs table-granular
                    // serialization (the PK Row X does not cover its key).
                    let hashes =
                        if is_fk_parent(storage, def.database_id, &def.name) || has_secondary_unique_index(&def) {
                            None
                        } else {
                            insert_row_key_hashes(&def, insert)
                        };
                    match hashes {
                        Some(hashes) => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::IntentExclusive);
                            for hash in hashes {
                                add(Resource::Row(def.object_id, hash), LockMode::Exclusive);
                            }
                        }
                        None => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::Exclusive);
                        }
                    }
                    // A child INSERT reads its FK parents (integrity read).
                    for oid in fk_parent_object_ids(storage, &def) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    // A firing AFTER-INSERT trigger's body reads/writes further
                    // tables; hold those locks up front too (strict 2PL).
                    add_trigger_locks(
                        db_id,
                        storage,
                        def.object_id,
                        catalog::TriggerEvent::Insert,
                        nested_isolation,
                        visited,
                        trigger_visited,
                        &mut add,
                    );
                }
                // INSERT ... SELECT also reads its source tables (and any
                // subqueries in the SELECT); lock them like a SELECT so it
                // cannot read another txn's uncommitted rows (they combine to
                // SIX on the target if it is a source).
                if reads_lock && let InsertSource::Select(select) = &insert.source {
                    let expanded = expand_ctes(select);
                    let mut tables = Vec::new();
                    collect_locked_tables(&expanded, &mut tables);
                    for name in tables {
                        for oid in read_lock_object_ids(storage, db_id, &name.value) {
                            add(Resource::Database, LockMode::IntentShared);
                            add(Resource::Table(oid), LockMode::Shared);
                        }
                    }
                    for oid in select_function_read_ids(storage, db_id, &expanded) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            Statement::Update(update) => {
                if let Some(def) = resolve_table(storage, db_id, &update.table.value) {
                    // A point UPDATE (WHERE pins the whole key, no key-column
                    // write, no subquery) takes Table IX + a single Row X. An FK
                    // parent or a secondary UNIQUE index keeps Table X (see INSERT).
                    let hash =
                        if is_fk_parent(storage, def.database_id, &def.name) || has_secondary_unique_index(&def) {
                            None
                        } else {
                            update_row_key_hash(&def, update)
                        };
                    match hash {
                        Some(hash) => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::IntentExclusive);
                            add(Resource::Row(def.object_id, hash), LockMode::Exclusive);
                        }
                        None => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::Exclusive);
                        }
                    }
                    // UPDATE reads FK parents (new values) and referencing
                    // children (a changed PK must not orphan them).
                    for oid in fk_parent_object_ids(storage, &def) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    for oid in fk_child_object_ids(storage, def.database_id, &def.name) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    add_trigger_locks(
                        db_id,
                        storage,
                        def.object_id,
                        catalog::TriggerEvent::Update,
                        nested_isolation,
                        visited,
                        trigger_visited,
                        &mut add,
                    );
                }
            }
            Statement::Delete(delete) => {
                if let Some(def) = resolve_table(storage, db_id, &delete.table.value) {
                    // A point DELETE (WHERE pins the whole key, no subquery)
                    // takes Table IX + a single Row X. A table referenced as an
                    // FK parent keeps Table X (see INSERT).
                    let hash = if is_fk_parent(storage, def.database_id, &def.name) {
                        None
                    } else {
                        where_point_key_hash(&def, &delete.where_clause)
                    };
                    match hash {
                        Some(hash) => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::IntentExclusive);
                            add(Resource::Row(def.object_id, hash), LockMode::Exclusive);
                        }
                        None => {
                            add(Resource::Database, LockMode::IntentExclusive);
                            add(Resource::Table(def.object_id), LockMode::Exclusive);
                        }
                    }
                    // DELETE reads referencing children (NO ACTION check).
                    for oid in fk_child_object_ids(storage, def.database_id, &def.name) {
                        add(Resource::Database, LockMode::IntentShared);
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                    add_trigger_locks(
                        db_id,
                        storage,
                        def.object_id,
                        catalog::TriggerEvent::Delete,
                        nested_isolation,
                        visited,
                        trigger_visited,
                        &mut add,
                    );
                }
            }
            // DDL serializes against every active transaction via a
            // database-exclusive lock (it is disallowed inside a txn anyway).
            Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            // ALTER DATABASE quiesces the database: no snapshot may be live
            // and no writer mid-transaction while the options flip.
            | Statement::AlterDatabase(_)
            // CREATE/DROP DATABASE rewrite the catalog's database list; the
            // same quiesce keeps every in-flight resolution coherent.
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. } => {
                add(Resource::Database, LockMode::Exclusive);
            }
            // EXEC sp_executesql with a LITERAL statement is analyzable up
            // front: recurse into the inner text. Anything else (a variable
            // statement, an unknown procedure) cannot be analyzed before it
            // runs — lock the database exclusively rather than under-lock
            // (2PL acquires the full set up front).
            Statement::Exec(exec) => {
                // A user procedure: its stored body analyzes like literal
                // inner text, parsed with the IN-PROCEDURE grammar — a plain
                // parse would reject `RETURN <value>` (178), yield no locks,
                // and the body would run UNLOCKED (the 2PL hole class).
                if let Some(def) = resolve_table(storage, db_id, &exec.proc.value)
                    && let Some(procedure) = &def.procedure
                {
                    let inner_isolation = if reads_lock {
                        if matches!(isolation, Isolation::ReadCommitted | Isolation::Snapshot)
                            && !escalates_reads
                        {
                            isolation
                        } else {
                            Isolation::RepeatableRead
                        }
                    } else {
                        isolation
                    };
                    if visited.insert((def.name.clone(), inner_isolation))
                        && let Ok(body) = truthdb_sql::parse_procedure_body(&procedure.body)
                    {
                        // The body executes in the procedure's HOME database
                        // (run_user_procedure sets it; the body cannot USE) —
                        // analyze it there, or the two derivations diverge.
                        for (resource, mode) in analyze_statements_locks(
                            storage,
                            def.database_id,
                            &body,
                            inner_isolation,
                            visited,
                            trigger_visited,
                        ) {
                            add(resource, mode);
                        }
                    }
                    continue;
                }
                match exec_literal_sql(exec) {
                Some(inner) => {
                    // The inner text runs under the batch's EFFECTIVE
                    // isolation: a `SET ... SERIALIZABLE` before the EXEC
                    // must lock the inner reads too, so the recursion gets a
                    // read-locking level whenever this batch locks reads.
                    // (An inner SET raising isolation is seen by the
                    // recursion's own scan; it cannot outlive the EXEC — SET
                    // options revert at scope exit.)
                    //
                    // That level must be one the versioned-read path can
                    // never claim: under RCSI the recursion's own
                    // `versioned_reads` would drop Table S for a plain
                    // READ COMMITTED, while at runtime the inner statement
                    // executes under the OUTER effective level and reads
                    // lock-based — a reachable dirty read at SERIALIZABLE
                    // (caught by the adversarial review). READ COMMITTED is
                    // passed only when it truly is the effective level.
                    let inner_isolation = if reads_lock {
                        if matches!(
                            isolation,
                            Isolation::ReadCommitted | Isolation::Snapshot
                        ) && !escalates_reads
                        {
                            // Both survive the recursion faithfully: the
                            // inner analysis reaches the same versioned/
                            // lock-based decision execution will.
                            isolation
                        } else {
                            Isolation::RepeatableRead
                        }
                    } else {
                        isolation
                    };
                    if let Ok(parsed) = truthdb_sql::parse(&inner) {
                        for (resource, mode) in analyze_statements_locks(
                            storage,
                            db_id,
                            &parsed,
                            inner_isolation,
                            visited,
                            trigger_visited,
                        ) {
                            add(resource, mode);
                        }
                    }
                }
                None => add(Resource::Database, LockMode::Exclusive),
                }
            }
            // Procedure DDL rewrites the catalog: Database X, like other DDL.
            Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_)
            | Statement::SetTriggerState { .. } => {
                add(Resource::Database, LockMode::Exclusive);
            }
            // IF/WHILE conditions read tables through their subqueries —
            // locked exactly like a SELECT's tables (their bodies were
            // flattened into this list and analyze as themselves).
            Statement::If { condition, .. } | Statement::While { condition, .. } => {
                if !reads_lock {
                    continue;
                }
                let mut tables = Vec::new();
                collect_expr_tables(condition, &mut tables);
                for name in tables {
                    for oid in read_lock_object_ids(storage, db_id, &name.value) {
                        add(Resource::Database, LockMode::IntentShared);
                        if !versioned_reads {
                            add(Resource::Table(oid), LockMode::Shared);
                        }
                    }
                }
                for oid in expr_function_read_ids(storage, db_id, condition) {
                    add(Resource::Database, LockMode::IntentShared);
                    if !versioned_reads {
                        add(Resource::Table(oid), LockMode::Shared);
                    }
                }
            }
            // Transaction control, SET, and DECLARE take no data locks.
            // TRY/CATCH and plain blocks were flattened away by
            // `flatten_statements`, so their contained statements appear here
            // directly; BREAK/CONTINUE/RETURN touch nothing.
            Statement::Block { .. }
            | Statement::Break { .. }
            | Statement::Continue { .. }
            | Statement::Return { .. }
            | Statement::Goto { .. }
            | Statement::Label { .. }
            | Statement::BeginTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::SaveTransaction { .. }
            | Statement::Set(_)
            | Statement::Declare(_)
            | Statement::DeclareTableVar { .. }
            | Statement::Use { .. }
            | Statement::Throw(_)
            | Statement::RaiseError(_)
            | Statement::TryCatch { .. }
            // BACKUP takes no batch lock: it is online and manages its own
            // per-chunk storage locking. A Database X here would serialize it
            // against every writer and defeat the fuzzy design.
            | Statement::BackupDatabase { .. }
            | Statement::BackupLog { .. }
            // RESTORE VERIFYONLY/HEADERONLY/FILELISTONLY only read a backup file;
            // they touch no database object, so they take no lock.
            | Statement::Restore { .. }
            // Cursor statements take no batch lock. OPEN executes its query,
            // whose scans take their own per-slice storage locks (as every read
            // does); DECLARE/FETCH/CLOSE/DEALLOCATE touch only session state.
            | Statement::DeclareCursor { .. }
            | Statement::OpenCursor { .. }
            | Statement::FetchCursor { .. }
            | Statement::CloseCursor { .. }
            | Statement::DeallocateCursor { .. } => {}
        }
    }
    // Batch-level lock escalation: if a table accumulated more than the
    // threshold of row locks across the whole batch (many literal-key INSERTs,
    // a loop, or several point statements), replace them all with one Table X.
    // Bounds the lock set a batch can request regardless of per-statement caps.
    let mut row_counts: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
    for resource in needs.keys() {
        if let Resource::Row(oid, _) = resource {
            *row_counts.entry(*oid).or_default() += 1;
        }
    }
    let escalate: std::collections::HashSet<u32> = row_counts
        .into_iter()
        .filter(|(_, count)| *count > ROW_LOCK_ESCALATION_THRESHOLD)
        .map(|(oid, _)| oid)
        .collect();
    if !escalate.is_empty() {
        needs.retain(
            |resource, _| !matches!(resource, Resource::Row(oid, _) if escalate.contains(oid)),
        );
        for oid in escalate {
            needs
                .entry(Resource::Table(oid))
                .and_modify(|m| *m = m.combine(LockMode::Exclusive))
                .or_insert(LockMode::Exclusive);
            needs
                .entry(Resource::Database)
                .and_modify(|m| *m = m.combine(LockMode::IntentExclusive))
                .or_insert(LockMode::IntentExclusive);
        }
    }
    needs.into_iter().collect()
}

/// Parses and executes a SQL batch, returning one result per statement, or
/// the first error (discarding earlier results). Kept for tests; the server
/// uses [`execute_batch`].
#[cfg(test)]
pub fn execute(storage: &Storage, sql: &str) -> Result<Vec<StatementResult>, SqlError> {
    let mut txn_ctx = TxnContext::default();
    let outcome = execute_batch(storage, sql, &mut txn_ctx);
    match outcome.error {
        Some(error) => Err(error),
        None => Ok(outcome.results),
    }
}

impl TxnContext {
    fn scope(&mut self) -> TxnScope<'_> {
        match &mut self.txn {
            Some(txn) => TxnScope::Explicit(txn),
            None => TxnScope::Auto,
        }
    }
}

fn exec_statement(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // A doomed (uncommittable) transaction rejects log writes with 3930, but —
    // like SQL Server — still allows reads (`SELECT`), `SET`, `DECLARE`, and a
    // full `ROLLBACK`, so a CATCH block can inspect `XACT_STATE()`/`ERROR_*()`
    // and then roll back. A partial rollback to a savepoint and `SAVE` stay
    // rejected (an uncommittable transaction can only be fully rolled back).
    if txn_ctx.doomed && !doomed_allows(statement) {
        return Err(SqlError::new(
            3930,
            16,
            1,
            "The current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction.",
        ));
    }
    let result = exec_statement_dispatch(storage, statement, txn_ctx);
    // SQL Server rolls a SNAPSHOT transaction back entirely on an update
    // conflict — "transaction aborted", not statement-failed-transaction-
    // doomed. @@TRANCOUNT drops to zero and the session continues.
    if let Err(error) = &result
        && error.number == 3960
        && txn_ctx.in_txn()
    {
        txn_ctx.abort(storage);
    }
    result
}

fn exec_statement_dispatch(
    storage: &Storage,
    statement: &Statement,
    txn_ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // A session whose current database was dropped errors on every statement
    // except USE (its way out) — never silently resolving in a dead
    // namespace. Dropped ids are tombstoned (never reallocated), so this
    // check is exact. The per-batch snapshot makes it one Vec scan.
    if !matches!(statement, Statement::Use { .. })
        && !txn_ctx
            .databases_snapshot
            .iter()
            .any(|(id, _)| *id == txn_ctx.database_id())
        && !txn_ctx.databases_snapshot.is_empty()
    {
        return Err(SqlError::new(
            911,
            16,
            1,
            format!(
                "Database '{}' does not exist. Make sure that the name is entered correctly.",
                txn_ctx.database
            ),
        ));
    }
    // DDL (schema + security) requires a privileged principal (sysadmin / dbo /
    // db_owner / the internal channel). A restricted database user is refused
    // before any change is made.
    if !txn_ctx.security.bypass && is_privileged_ddl(statement) {
        return Err(SqlError::new(
            15247,
            16,
            1,
            "User does not have permission to perform this action.".to_string(),
        ));
    }
    match statement {
        Statement::BeginTransaction { .. } => exec_begin(storage, txn_ctx),
        Statement::Use { database, .. } => exec_use(storage, database, txn_ctx),
        Statement::Throw(throw) => Err(exec_throw(throw, txn_ctx)),
        Statement::CreateProcedure(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_procedure(storage, txn_ctx.database_id(), create)
        }
        Statement::DropProcedure {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_procedure(storage, txn_ctx.database_id(), name, *if_exists)
        }
        Statement::CreateFunction(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_function(storage, txn_ctx.database_id(), create)
        }
        Statement::DropFunction {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_function(storage, txn_ctx.database_id(), name, *if_exists)
        }
        Statement::CreateTrigger(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_trigger(storage, txn_ctx.database_id(), create)
        }
        Statement::DropTrigger {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_trigger(storage, txn_ctx.database_id(), name, *if_exists)
        }
        Statement::SetTriggerState {
            trigger,
            table,
            enable,
            ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_set_trigger_state(storage, txn_ctx.database_id(), trigger, table, *enable)
        }
        Statement::CreateLogin(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_login(storage, create)
        }
        Statement::DropLogin {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_login(storage, name, *if_exists)
        }
        Statement::CreateUser(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_user(storage, create)
        }
        Statement::DropUser {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_database_principal(storage, name, *if_exists, false)
        }
        Statement::CreateRole { name, .. } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_role(storage, name)
        }
        Statement::DropRole {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_database_principal(storage, name, *if_exists, true)
        }
        Statement::AlterRole {
            name,
            action,
            member,
            ..
        } => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_alter_role_member(storage, name, *action, member)
        }
        Statement::Permission(stmt) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_permission(storage, txn_ctx.database_id(), stmt, &txn_ctx.security)
        }
        Statement::BackupDatabase {
            database,
            path,
            checksum,
            copy_only,
            ..
        } => {
            // BACKUP manages its own (per-chunk) locking, so it cannot run
            // inside a transaction that holds locks, and it is a privileged
            // operation (gated by is_privileged_ddl above).
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    3021,
                    16,
                    1,
                    "Cannot perform a backup or restore operation within a transaction."
                        .to_string(),
                ));
            }
            // Any catalog database is a valid target: a backup is
            // instance-granular (it contains every database) — the name is
            // validated, not scoping.
            if storage.rel_database_id_by_name(&database.value).is_none() {
                return Err(SqlError::new(
                    911,
                    16,
                    1,
                    format!(
                        "Database '{}' does not exist. Make sure that the name is entered correctly.",
                        database.value
                    ),
                )
                .at(database.span));
            }
            storage
                .backup_full_with(std::path::Path::new(path), *checksum, *copy_only)
                .map_err(|e| {
                    SqlError::new(
                        3013,
                        16,
                        1,
                        format!("BACKUP DATABASE is terminating abnormally. {e}"),
                    )
                })?;
            Ok(StatementResult::Done)
        }
        Statement::BackupLog {
            database,
            path,
            checksum,
            copy_only,
            ..
        } => {
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    3021,
                    16,
                    1,
                    "Cannot perform a backup or restore operation within a transaction."
                        .to_string(),
                ));
            }
            if storage.rel_database_id_by_name(&database.value).is_none() {
                return Err(SqlError::new(
                    911,
                    16,
                    1,
                    format!(
                        "Database '{}' does not exist. Make sure that the name is entered correctly.",
                        database.value
                    ),
                )
                .at(database.span));
            }
            if !storage.recovery_model_full() {
                return Err(SqlError::new(
                    4208,
                    16,
                    1,
                    "The statement BACKUP LOG is not allowed while the recovery model is SIMPLE. \
                     Use BACKUP DATABASE or change the recovery model to FULL with ALTER DATABASE."
                        .to_string(),
                ));
            }
            storage
                .backup_log(std::path::Path::new(path), *checksum, *copy_only)
                .map_err(|e| {
                    SqlError::new(
                        3013,
                        16,
                        1,
                        format!("BACKUP LOG is terminating abnormally. {e}"),
                    )
                })?;
            Ok(StatementResult::Done)
        }
        Statement::Restore { mode, path, .. } => exec_restore(*mode, path, txn_ctx),
        Statement::DeclareCursor { name, select, .. } => exec_declare_cursor(txn_ctx, name, select),
        Statement::OpenCursor { name, .. } => exec_open_cursor(storage, txn_ctx, name),
        Statement::FetchCursor {
            name,
            direction,
            into,
            ..
        } => exec_fetch(storage, txn_ctx, name, direction, into),
        Statement::CloseCursor { name, .. } => exec_close_cursor(txn_ctx, name),
        Statement::DeallocateCursor { name, .. } => exec_deallocate_cursor(txn_ctx, name),
        // Executed by `run_block`'s own arms; nothing routes them here.
        Statement::Block { .. }
        | Statement::If { .. }
        | Statement::While { .. }
        | Statement::Break { .. }
        | Statement::Continue { .. }
        | Statement::Return { .. }
        | Statement::Goto { .. }
        | Statement::Label { .. } => {
            unreachable!("control flow is executed by run_block")
        }
        // Handled in `exec_statement_streamed_inner` (severity <= 10 emits an
        // INFO event, which needs the emitter); nothing else routes it here.
        Statement::RaiseError(_) => unreachable!("RAISERROR reaches only the streaming executor"),
        Statement::Commit { .. } => exec_commit(storage, txn_ctx),
        Statement::Rollback { name, .. } => exec_rollback(storage, txn_ctx, name.as_ref()),
        Statement::SaveTransaction { name, .. } => exec_save(storage, txn_ctx, name),
        Statement::Set(set) => exec_set(txn_ctx, set),
        Statement::Declare(decls) => exec_declare(txn_ctx, decls),
        Statement::DeclareTableVar {
            name,
            columns,
            primary_key,
            ..
        } => exec_declare_table_var(txn_ctx, name, columns, primary_key),
        Statement::CreateTable(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_table(storage, txn_ctx.database_id(), create)
        }
        Statement::DropTable(drop) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_table(storage, txn_ctx.database_id(), drop)
        }
        Statement::CreateView(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_view(storage, txn_ctx.database_id(), create)
        }
        Statement::DropView(drop) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_view(storage, txn_ctx.database_id(), drop)
        }
        Statement::CreateIndex(create) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_create_index(storage, txn_ctx.database_id(), create)
        }
        Statement::DropIndex(drop) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            exec_drop_index(storage, txn_ctx.database_id(), drop)
        }
        Statement::AlterTable(alter) => {
            if txn_ctx.in_txn() {
                return Err(ddl_in_txn_err());
            }
            let eval_ctx = txn_ctx.eval_context();
            exec_alter_table(storage, txn_ctx.database_id(), alter, &eval_ctx)
        }
        Statement::AlterDatabase(alter) => {
            if txn_ctx.in_txn() {
                // SQL Server 226: ALTER DATABASE is not allowed inside a
                // multi-statement transaction.
                return Err(SqlError::new(
                    226,
                    16,
                    6,
                    "ALTER DATABASE statement not allowed within multi-statement transaction.",
                ));
            }
            exec_alter_database(storage, alter, txn_ctx)
        }
        Statement::CreateDatabase { name, .. } => {
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    226,
                    16,
                    6,
                    "CREATE DATABASE statement not allowed within multi-statement transaction.",
                ));
            }
            exec_create_database(storage, name)
        }
        Statement::DropDatabase {
            name, if_exists, ..
        } => {
            if txn_ctx.in_txn() {
                return Err(SqlError::new(
                    226,
                    16,
                    6,
                    "DROP DATABASE statement not allowed within multi-statement transaction.",
                ));
            }
            exec_drop_database(storage, name, *if_exists, txn_ctx)
        }
        Statement::Insert(insert) => {
            // INSERT into a `@t` table variable is pure session memory (no
            // Storage, no lock, no WAL) — handled here where `&mut TxnContext`
            // is in hand, before the storage scope is taken.
            if insert.table.value.starts_with('@') {
                let eval_ctx = txn_ctx.eval_context();
                return exec_insert_table_var(storage, insert, txn_ctx, &eval_ctx);
            }
            let (target, after, instead_of) = triggers_for(
                storage,
                txn_ctx.database_id(),
                &insert.table.value,
                catalog::TriggerEvent::Insert,
            );
            let run_insert = |txn_ctx: &mut TxnContext| -> Result<StatementResult, SqlError> {
                let eval_ctx = txn_ctx.eval_context();
                let (result, identity) = {
                    let mut scope = txn_ctx.scope();
                    exec_insert(storage, insert, &mut scope, &eval_ctx)?
                };
                // An identity INSERT updates SCOPE_IDENTITY(); a non-identity one
                // (identity == None) leaves it unchanged.
                if let Some(value) = identity {
                    txn_ctx.scope_identity = Some(value);
                }
                Ok(result)
            };
            match target {
                Some(target) => {
                    if let Some(io) = instead_of {
                        run_instead_of(storage, txn_ctx, &target, io, |eval_ctx| {
                            instead_of_insert_images(storage, insert, &target, eval_ctx)
                        })
                    } else if !after.is_empty() {
                        run_dml_with_triggers(storage, txn_ctx, &target, after, run_insert)
                    } else {
                        run_insert(txn_ctx)
                    }
                }
                None => run_insert(txn_ctx),
            }
        }
        Statement::Update(update) => {
            let (target, after, instead_of) = triggers_for(
                storage,
                txn_ctx.database_id(),
                &update.table.value,
                catalog::TriggerEvent::Update,
            );
            let run_update = |txn_ctx: &mut TxnContext| -> Result<StatementResult, SqlError> {
                let eval_ctx = txn_ctx.eval_context();
                let mut scope = txn_ctx.scope();
                exec_update(storage, update, &mut scope, &eval_ctx)
            };
            match target {
                Some(target) => {
                    if let Some(io) = instead_of {
                        run_instead_of(storage, txn_ctx, &target, io, |eval_ctx| {
                            instead_of_update_images(storage, update, &target, eval_ctx)
                        })
                    } else if !after.is_empty() {
                        run_dml_with_triggers(storage, txn_ctx, &target, after, run_update)
                    } else {
                        run_update(txn_ctx)
                    }
                }
                None => run_update(txn_ctx),
            }
        }
        Statement::Delete(delete) => {
            let (target, after, instead_of) = triggers_for(
                storage,
                txn_ctx.database_id(),
                &delete.table.value,
                catalog::TriggerEvent::Delete,
            );
            let run_delete = |txn_ctx: &mut TxnContext| -> Result<StatementResult, SqlError> {
                let eval_ctx = txn_ctx.eval_context();
                let mut scope = txn_ctx.scope();
                exec_delete(storage, delete, &mut scope, &eval_ctx)
            };
            match target {
                Some(target) => {
                    if let Some(io) = instead_of {
                        run_instead_of(storage, txn_ctx, &target, io, |eval_ctx| {
                            instead_of_delete_images(storage, delete, &target, eval_ctx)
                        })
                    } else if !after.is_empty() {
                        run_dml_with_triggers(storage, txn_ctx, &target, after, run_delete)
                    } else {
                        run_delete(txn_ctx)
                    }
                }
                None => run_delete(txn_ctx),
            }
        }
        Statement::Select(select) => {
            if select
                .items
                .iter()
                .any(|i| matches!(i, SelectItem::Assign { .. }))
            {
                return exec_select_assign(storage, select, txn_ctx);
            }
            let eval_ctx = txn_ctx.eval_context();
            if txn_ctx.showplan_text {
                Ok(StatementResult::Rows(showplan_rows(
                    storage, select, &eval_ctx,
                )?))
            } else {
                Ok(StatementResult::Rows(exec_select(
                    storage, select, &eval_ctx,
                )?))
            }
        }
        // TRY/CATCH is control flow, handled by `run_block`, which never routes
        // it here.
        Statement::TryCatch { .. } => Err(SqlError::message_only(
            0,
            "internal error: TRY/CATCH must be executed by run_block",
        )),
        // EXEC recurses into its inner batch, handled by `run_block` too.
        Statement::Exec(_) => Err(SqlError::message_only(
            0,
            "internal error: EXEC must be executed by run_block",
        )),
    }
}

/// Statements a doomed (uncommittable) transaction still permits: reads
/// (`SELECT`, including `SELECT @v = ...`), session-state changes (`SET`,
/// `DECLARE`), and a full `ROLLBACK`. Everything else (DML/DDL, `COMMIT`,
/// `SAVE`, a partial `ROLLBACK` to a savepoint) writes to the log and is
/// rejected with 3930.
fn doomed_allows(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Select(_)
            | Statement::Set(_)
            | Statement::Declare(_)
            | Statement::Use { .. }
            | Statement::Throw(_)
            | Statement::RaiseError(_)
            | Statement::Rollback { name: None, .. }
    )
}

/// Flattens `TRY`/`CATCH` blocks into the leaf statements they contain, so lock
/// analysis (which pre-acquires every table lock a batch needs) sees the
/// statements nested inside try/catch blocks too.
fn flatten_statements<'a>(statements: &'a [Statement], out: &mut Vec<&'a Statement>) {
    for statement in statements {
        match statement {
            Statement::TryCatch {
                try_block,
                catch_block,
                ..
            } => {
                flatten_statements(try_block, out);
                flatten_statements(catch_block, out);
            }
            Statement::Block { body, .. } => flatten_statements(body, out),
            // IF/WHILE stay in the list (their CONDITIONS take read locks);
            // their bodies flatten so the leaf statements analyze as
            // themselves — a WHILE body's INSERT needs its lock up front like
            // any other, and both IF branches are analyzed (conservative:
            // which one runs is a runtime fact).
            Statement::If {
                then_branch,
                else_branch,
                ..
            } => {
                out.push(statement);
                flatten_statements(std::slice::from_ref(then_branch), out);
                if let Some(else_branch) = else_branch {
                    flatten_statements(std::slice::from_ref(else_branch), out);
                }
            }
            Statement::While { body, .. } => {
                out.push(statement);
                flatten_statements(std::slice::from_ref(body), out);
            }
            other => out.push(other),
        }
    }
}

/// Builds a one-column `SHOWPLAN_TEXT` rowset describing a SELECT's access
/// path, without executing it.
fn showplan_rows(
    storage: &Storage,
    select: &Select,
    eval_ctx: &EvalContext,
) -> Result<RowSet, SqlError> {
    let lines = match select.from.as_ref() {
        None => vec!["Constant Scan".to_string()],
        Some(TableRef::Table { name, .. })
            if !name.value.to_ascii_lowercase().starts_with("sys.") =>
        {
            match resolve_table(storage, eval_ctx.database_id, &name.value) {
                Some(def) => {
                    // The scan shape carries the covering decision (it knows
                    // which columns the query reads); other shapes never
                    // cover, so the plain choose() answer is exact for them.
                    if let Some(plan) = scan_plan(storage, select, eval_ctx) {
                        plan::plan_text(&plan.access, &def.name, plan.covering)
                    } else {
                        let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
                        // Fetched only when choose() can use it (it returns a
                        // scan outright without a predicate or indexes).
                        let row_count = if def.indexes.is_empty() || select.where_clause.is_none() {
                            None
                        } else {
                            storage.rel_row_count(def.database_id, &def.name)
                        };
                        let path = plan::choose(
                            &def,
                            &schema,
                            &select.where_clause,
                            eval_ctx,
                            None,
                            row_count,
                        );
                        plan::plan_text(&path, &def.name, false)
                    }
                }
                None => vec![format!("Table Scan({})", name.value)],
            }
        }
        Some(TableRef::Table { name, .. }) => vec![format!("Table Scan({})", name.value)],
        // A lone table-valued function call: name it honestly rather than
        // letting it fall into the join catch-all (which would invent a
        // "Nested Loops" over a phantom base table named after the function).
        Some(TableRef::Function { name, .. }) => {
            vec![format!("Table-valued Function({})", name.value)]
        }
        Some(join) => {
            // Multi-table: a nested-loop join over full scans (Stage 8).
            let mut tables = Vec::new();
            collect_table_names(join, &mut tables);
            let mut lines = vec!["Nested Loops (join)".to_string()];
            for table in tables {
                lines.push(format!("  Table Scan({})", strip_schema(&table.value)));
            }
            lines
        }
    };
    Ok(RowSet {
        columns: vec![ResultColumn {
            name: "StmtText".to_string(),
            column_type: ColumnType::NVarChar { max_len: 4000 },
        }],
        rows: lines
            .into_iter()
            .map(|line| vec![Datum::NVarChar(line)])
            .collect(),
    })
}

fn ddl_in_txn_err() -> SqlError {
    SqlError::new(
        226,
        16,
        1,
        "DDL statements are not allowed inside an explicit transaction in this version.",
    )
}

// ---- transaction control -----------------------------------------------

/// `USE <database>`: a single-database instance, so the only accepted target
/// is the session's current database — the statement exists for the
/// database-context ENVCHANGE clients (SSMS) expect back (emitted by
/// `run_block` on success).
fn exec_use(
    storage: &Storage,
    database: &Name,
    ctx: &mut TxnContext,
) -> Result<StatementResult, SqlError> {
    // ONE catalog read: a lookup-then-list pair would race a concurrent
    // DROP DATABASE into a panic between the two.
    let Some((db_id, canonical)) = storage
        .rel_databases()
        .into_iter()
        .find(|(_, name)| name.eq_ignore_ascii_case(&database.value))
    else {
        return Err(SqlError::new(
            911,
            16,
            1,
            format!(
                "Database '{}' does not exist. Make sure that the name is entered correctly.",
                database.value
            ),
        )
        .at(database.span));
    };
    ctx.set_current_database(canonical, db_id);
    Ok(StatementResult::Done)
}

/// `CREATE DATABASE <name>`: a new naming namespace (level 1 — one shared
/// log and data file; nothing physical is allocated).
fn exec_create_database(storage: &Storage, name: &Name) -> Result<StatementResult, SqlError> {
    storage
        .rel_create_database(&name.value)
        .map_err(|err| match err {
            StorageError::Constraint(msg) if msg.contains("already exists") => SqlError::new(
                1801,
                16,
                3,
                format!(
                    "Database '{}' already exists. Choose a different database name.",
                    name.value
                ),
            )
            .at(name.span),
            other => map_storage_err(other, &name.value),
        })?;
    Ok(StatementResult::Done)
}

/// `DROP DATABASE [IF EXISTS] <name>`: drops the namespace and every object
/// in it. The session's current database (3702), the default database
/// (3708), and — without IF EXISTS — a missing one (3701) are refused.
fn exec_drop_database(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
    ctx: &TxnContext,
) -> Result<StatementResult, SqlError> {
    if storage.rel_database_id_by_name(&name.value) == Some(ctx.database_id()) {
        return Err(SqlError::new(
            3702,
            16,
            4,
            format!(
                "Cannot drop database \"{}\" because it is currently in use.",
                name.value
            ),
        )
        .at(name.span));
    }
    match storage.rel_drop_database(&name.value) {
        Ok(true) => Ok(StatementResult::Done),
        Ok(false) if if_exists => Ok(StatementResult::Done),
        Ok(false) => Err(SqlError::new(
            3701,
            16,
            1,
            format!(
                "Cannot drop the database '{}', because it does not exist or you do not have permission.",
                name.value
            ),
        )
        .at(name.span)),
        Err(StorageError::Constraint(msg)) if msg.contains("system database") => {
            Err(SqlError::new(
                3708,
                16,
                5,
                format!(
                    "Cannot drop the database '{}' because it is a system database.",
                    name.value
                ),
            )
            .at(name.span))
        }
        Err(other) => Err(map_storage_err(other, &name.value)),
    }
}

/// `THROW`: builds the error to raise (the caller returns it — `run_block`
/// then applies THROW's batch-terminating rule). The bare form re-throws the
/// innermost `CATCH`'s error verbatim, severity included; the argument form
/// is always severity 16 with a user error number (>= 50000).
fn exec_throw(throw: &ThrowStatement, ctx: &TxnContext) -> SqlError {
    let Some(args) = &throw.args else {
        return match ctx.error_stack.last() {
            Some(info) => {
                SqlError::new(info.number, info.severity, info.state, info.message.clone())
            }
            None => SqlError::new(
                10704,
                16,
                1,
                "To rethrow an error, a THROW statement must be used inside a CATCH block.",
            ),
        };
    };
    let eval_ctx = ctx.eval_context();
    match exec_throw_args(args, &eval_ctx) {
        // Both sides raise: the built error, or the argument evaluation's own.
        Ok(error) | Err(error) => error,
    }
}

fn exec_throw_args(args: &ThrowArgs, eval_ctx: &EvalContext) -> Result<SqlError, SqlError> {
    let number = int_argument(&args.number, eval_ctx, "THROW", "error number")?;
    if !(50_000..=i64::from(i32::MAX)).contains(&number) {
        return Err(SqlError::new(
            35100,
            16,
            1,
            format!(
                "Error number {number} in the THROW statement is outside the valid range. \
                 Specify an error number in the valid range of 50000 to 2147483647."
            ),
        ));
    }
    let message = match eval_constant(&args.message, eval_ctx)? {
        SqlValue::Str(text) => text,
        other => {
            return Err(SqlError::new(
                102,
                15,
                1,
                format!(
                    "The THROW message must be a string, not {}.",
                    other.type_name()
                ),
            ));
        }
    };
    let state = int_argument(&args.state, eval_ctx, "THROW", "state")?;
    if !(0..=255).contains(&state) {
        return Err(SqlError::new(
            102,
            15,
            1,
            format!("The THROW state must be between 0 and 255, not {state}."),
        ));
    }
    Ok(SqlError::new(number as i32, 16, state as u8, message))
}

/// `RAISERROR(msg, severity, state, args...)`. Severity decides the shape:
/// <= 10 emits an informational message (a TDS INFO token, not an error) and
/// the statement SUCCEEDS; 11..=18 raises an ordinary error (statement-scope
/// — `run_block` exempts it from XACT_ABORT and never dooms for it);
/// 19..=25 additionally require `WITH LOG`, and >= 20 is fatal to the
/// connection. The error number is always 50000 (message-id RAISERROR needs
/// `sys.messages`, which TruthDB does not have — 18054 like an unknown id).
fn exec_raiserror(
    raise: &RaiseError,
    txn_ctx: &mut TxnContext,
    run: &mut BatchRun<'_>,
) -> Result<StatementOutcome, SqlError> {
    let eval_ctx = txn_ctx.eval_context();
    let severity = int_argument(&raise.severity, &eval_ctx, "RAISERROR", "severity")?;
    if !(0..=25).contains(&severity) {
        return Err(SqlError::new(
            2754,
            16,
            1,
            format!("Error severity {severity} is out of the range 0 through 25."),
        ));
    }
    if severity > 18 && !raise.log {
        return Err(SqlError::new(
            2754,
            16,
            1,
            "Error severity levels greater than 18 can only be specified by members of the \
             sysadmin role, using the WITH LOG option.",
        ));
    }
    // State 0 is reported as 1, as SQL Server does.
    let state = int_argument(&raise.state, &eval_ctx, "RAISERROR", "state")?;
    if !(0..=255).contains(&state) {
        return Err(SqlError::new(
            2753,
            16,
            1,
            format!("The RAISERROR state must be between 0 and 255, not {state}."),
        ));
    }
    let state = (state as u8).max(1);
    let message = match eval_constant(&raise.message, &eval_ctx)? {
        SqlValue::Str(format) => {
            let mut args = Vec::with_capacity(raise.args.len());
            for arg in &raise.args {
                args.push(eval_constant(arg, &eval_ctx)?);
            }
            format_raiserror(&format, &args)?
        }
        // A message id: there is no `sys.messages`, so no id resolves.
        SqlValue::Int(id) => {
            return Err(SqlError::new(
                18054,
                16,
                1,
                format!(
                    "Error {id}, severity {severity}, state {state} was raised, but no message \
                     with that error number was found in sys.messages."
                ),
            ));
        }
        other => {
            return Err(SqlError::new(
                102,
                15,
                1,
                format!(
                    "The RAISERROR message must be a string or a message id, not {}.",
                    other.type_name()
                ),
            ));
        }
    };
    const AD_HOC_MESSAGE_NUMBER: i32 = 50000;
    if severity <= 10 {
        // Informational: `@@ERROR` reads 0 (or 50000 under SETERROR) — set
        // here because `run_block`'s success path leaves RAISERROR's value.
        txn_ctx.last_error = if raise.seterror {
            AD_HOC_MESSAGE_NUMBER
        } else {
            0
        };
        run.info(SqlError::new(
            AD_HOC_MESSAGE_NUMBER,
            severity as u8,
            state,
            message,
        ));
        return Ok(StatementOutcome::Result(StatementResult::Done));
    }
    Err(SqlError::new(
        AD_HOC_MESSAGE_NUMBER,
        severity as u8,
        state,
        message,
    ))
}

/// An integer statement argument (THROW/RAISERROR take constants or
/// variables).
fn int_argument(
    expr: &Expr,
    eval_ctx: &EvalContext,
    statement: &str,
    what: &str,
) -> Result<i64, SqlError> {
    match eval_constant(expr, eval_ctx)? {
        SqlValue::Int(value) => Ok(value),
        other => Err(SqlError::new(
            102,
            15,
            1,
            format!(
                "The {statement} {what} must be an integer, not {}.",
                other.type_name()
            ),
        )),
    }
}

/// RAISERROR's printf subset: `%d`/`%i` (also `%u`, `%x`/`%X`, `%o`) for
/// integer arguments, `%s` for strings, `%%` for a literal percent. Anything
/// else is refused (2787), as is an argument of the wrong type or a missing
/// one (2786). Surplus arguments are ignored, as SQL Server does.
fn format_raiserror(format: &str, args: &[SqlValue]) -> Result<String, SqlError> {
    let mut out = String::with_capacity(format.len());
    let mut next_arg = 0usize;
    let mut chars = format.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        let Some(directive) = chars.next() else {
            return Err(SqlError::new(
                2787,
                16,
                1,
                "Invalid format specification: '%' at the end of the message.",
            ));
        };
        if directive == '%' {
            out.push('%');
            continue;
        }
        let argument = args.get(next_arg).ok_or_else(|| {
            SqlError::new(
                2786,
                16,
                1,
                format!(
                    "The data type of substitution parameter {} does not match the expected \
                     type of the format specification (missing argument).",
                    next_arg + 1
                ),
            )
        })?;
        let mismatch = || {
            SqlError::new(
                2786,
                16,
                1,
                format!(
                    "The data type of substitution parameter {} does not match the expected \
                     type of the format specification.",
                    next_arg + 1
                ),
            )
        };
        // A NULL argument prints "(null)" under every directive, as SQL
        // Server does. Integer arguments are int-typed (32-bit) there, so
        // the unsigned/hex forms wrap at 32 bits (-1 -> ffffffff) and a
        // value outside int range is a type mismatch (2786, the bigint
        // refusal).
        if matches!(argument, SqlValue::Null) {
            out.push_str("(null)");
            next_arg += 1;
            continue;
        }
        let int_arg = || -> Result<i32, SqlError> {
            match argument {
                SqlValue::Int(value) => i32::try_from(*value).map_err(|_| mismatch()),
                _ => Err(mismatch()),
            }
        };
        match directive {
            'd' | 'i' => out.push_str(&int_arg()?.to_string()),
            'u' => out.push_str(&(int_arg()? as u32).to_string()),
            'x' => out.push_str(&format!("{:x}", int_arg()? as u32)),
            'X' => out.push_str(&format!("{:X}", int_arg()? as u32)),
            'o' => out.push_str(&format!("{:o}", int_arg()? as u32)),
            's' => match argument {
                SqlValue::Str(value) => out.push_str(value),
                _ => return Err(mismatch()),
            },
            other => {
                return Err(SqlError::new(
                    2787,
                    16,
                    1,
                    format!("Invalid format specification: '%{other}'."),
                ));
            }
        }
        next_arg += 1;
    }
    Ok(out)
}

fn exec_begin(storage: &Storage, ctx: &mut TxnContext) -> Result<StatementResult, SqlError> {
    if ctx.txn.is_none() {
        ctx.txn = Some(storage.rel_begin().map_err(|e| map_storage_err(e, ""))?);
    }
    // Nested BEGIN only bumps the count (SQL Server semantics).
    ctx.trancount += 1;
    Ok(StatementResult::Done)
}

fn exec_commit(storage: &Storage, ctx: &mut TxnContext) -> Result<StatementResult, SqlError> {
    if ctx.trancount == 0 {
        return Err(SqlError::new(
            3902,
            16,
            1,
            "The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION.",
        ));
    }
    ctx.trancount -= 1;
    // Only the outermost COMMIT actually commits.
    if ctx.trancount == 0
        && let Some(txn) = ctx.txn.take()
    {
        ctx.savepoints.clear();
        // The transaction is over either way the commit goes.
        ctx.release_txn_snapshot(storage);
        storage
            .rel_commit(txn)
            .map_err(|e| map_storage_err(e, ""))?;
    }
    Ok(StatementResult::Done)
}

fn exec_rollback(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: Option<&Name>,
) -> Result<StatementResult, SqlError> {
    if ctx.trancount == 0 {
        return Err(SqlError::new(
            3903,
            16,
            1,
            "The ROLLBACK TRANSACTION request has no corresponding BEGIN TRANSACTION.",
        ));
    }
    // ROLLBACK <savepoint>: partial rollback — the transaction stays open and
    // @@TRANCOUNT is unchanged; only the work done since the savepoint is undone.
    if let Some(name) = name {
        let Some(savepoint) = ctx
            .savepoints
            .get(&name.value.to_ascii_lowercase())
            .copied()
        else {
            return Err(SqlError::new(
                3908,
                16,
                1,
                format!(
                    "Cannot roll back {}. No transaction or savepoint of that name was found.",
                    name.value
                ),
            ));
        };
        if let Some(txn) = ctx.txn.as_mut() {
            storage
                .rel_rollback_to(txn, savepoint)
                .map_err(|e| map_storage_err(e, ""))?;
        }
        // Savepoints taken after this one are invalidated — their undo-log suffix
        // was just discarded (the target savepoint itself remains re-usable).
        ctx.savepoints
            .retain(|_, sp| sp.undo_len <= savepoint.undo_len);
        return Ok(StatementResult::Done);
    }
    // ROLLBACK (whole transaction), regardless of nesting. Reset the session's
    // transaction counters even if the storage rollback fails (which wedges the
    // store): the transaction is over either way, so leaving @@TRANCOUNT /
    // doomed set would desync the session.
    let result = match ctx.txn.take() {
        Some(txn) => storage
            .rel_rollback(txn)
            .map_err(|e| map_storage_err(e, "")),
        None => Ok(()),
    };
    ctx.release_txn_snapshot(storage);
    ctx.trancount = 0;
    ctx.doomed = false;
    ctx.savepoints.clear();
    result.map(|()| StatementResult::Done)
}

/// `SAVE TRANSACTION <name>`: record a savepoint the transaction can later roll
/// back to. Requires an active transaction (in autocommit there is nothing to
/// save, so it is a no-op). Re-saving an existing name overwrites it, as in
/// SQL Server.
fn exec_save(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    if let Some(txn) = ctx.txn.as_ref() {
        let savepoint = storage.rel_savepoint(txn);
        ctx.savepoints
            .insert(name.value.to_ascii_lowercase(), savepoint);
    }
    Ok(StatementResult::Done)
}

/// A declared cursor: its query, and — once OPENed — the materialized result and
/// the current position (0 = before the first row; 1..=len = on a row; len+1 =
/// after the last). Static: the rows are snapshotted at OPEN.
struct CursorState {
    select: Box<Select>,
    columns: Vec<ResultColumn>,
    rows: Vec<Vec<Datum>>,
    position: i64,
    open: bool,
}

fn cursor_not_declared(name: &Name) -> SqlError {
    SqlError::new(
        16916,
        16,
        1,
        format!("A cursor with the name '{}' does not exist.", name.value),
    )
    .at(name.span)
}

fn cursor_not_open(name: &Name) -> SqlError {
    SqlError::new(16917, 16, 1, "The cursor is not open.".to_string()).at(name.span)
}

fn exec_declare_cursor(
    ctx: &mut TxnContext,
    name: &Name,
    select: &Select,
) -> Result<StatementResult, SqlError> {
    let key = name.value.to_ascii_lowercase();
    if ctx.cursors.contains_key(&key) {
        return Err(SqlError::new(
            16915,
            16,
            1,
            format!("The cursor name '{}' already exists.", name.value),
        )
        .at(name.span));
    }
    ctx.cursors.insert(
        key,
        CursorState {
            select: Box::new(select.clone()),
            columns: Vec::new(),
            rows: Vec::new(),
            position: 0,
            open: false,
        },
    );
    Ok(StatementResult::Done)
}

fn exec_open_cursor(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    let key = name.value.to_ascii_lowercase();
    let cursor = ctx
        .cursors
        .get(&key)
        .ok_or_else(|| cursor_not_declared(name))?;
    if cursor.open {
        return Err(
            SqlError::new(16905, 16, 1, "The cursor is already open.".to_string()).at(name.span),
        );
    }
    let select = cursor.select.clone();
    let eval_ctx = ctx.eval_context();
    let rowset = exec_select(storage, &select, &eval_ctx)?;
    let cursor = ctx.cursors.get_mut(&key).expect("cursor declared");
    cursor.columns = rowset.columns;
    cursor.rows = rowset.rows;
    cursor.position = 0;
    cursor.open = true;
    Ok(StatementResult::Done)
}

fn exec_close_cursor(ctx: &mut TxnContext, name: &Name) -> Result<StatementResult, SqlError> {
    let key = name.value.to_ascii_lowercase();
    let cursor = ctx
        .cursors
        .get_mut(&key)
        .ok_or_else(|| cursor_not_declared(name))?;
    if !cursor.open {
        return Err(cursor_not_open(name));
    }
    cursor.open = false;
    cursor.rows = Vec::new();
    cursor.columns = Vec::new();
    cursor.position = 0;
    Ok(StatementResult::Done)
}

fn exec_deallocate_cursor(ctx: &mut TxnContext, name: &Name) -> Result<StatementResult, SqlError> {
    let key = name.value.to_ascii_lowercase();
    if ctx.cursors.remove(&key).is_none() {
        return Err(cursor_not_declared(name));
    }
    Ok(StatementResult::Done)
}

fn exec_fetch(
    storage: &Storage,
    ctx: &mut TxnContext,
    name: &Name,
    direction: &FetchDirection,
    into: &[String],
) -> Result<StatementResult, SqlError> {
    let _ = storage;
    let key = name.value.to_ascii_lowercase();
    // Evaluate an ABSOLUTE/RELATIVE offset (it may reference variables) up front.
    let offset = match direction {
        FetchDirection::Absolute(e) | FetchDirection::Relative(e) => {
            let eval_ctx = ctx.eval_context();
            Some(match eval_constant(e, &eval_ctx)? {
                SqlValue::Int(i) => i,
                SqlValue::Null => 0,
                _ => {
                    return Err(SqlError::message_only(
                        16924,
                        "The FETCH offset must be an integer.".to_string(),
                    ));
                }
            })
        }
        _ => None,
    };
    // Compute the target 1-based position from an immutable read of the cursor.
    let (columns, fetched, new_position, in_range) = {
        let cursor = ctx
            .cursors
            .get(&key)
            .ok_or_else(|| cursor_not_declared(name))?;
        if !cursor.open {
            return Err(cursor_not_open(name));
        }
        let n = cursor.rows.len() as i64;
        let mut target = match direction {
            FetchDirection::Next => cursor.position + 1,
            FetchDirection::Prior => cursor.position - 1,
            FetchDirection::First => 1,
            FetchDirection::Last => n,
            FetchDirection::Absolute(_) => offset.unwrap_or(0),
            // Saturate: a huge offset overflows `position + offset` (i64), which
            // panics in a checked build and silently wraps in release. Saturating
            // lands off the end, where the range check below maps it to -1.
            FetchDirection::Relative(_) => cursor.position.saturating_add(offset.unwrap_or(0)),
        };
        // ABSOLUTE -1 addresses the last row, -2 the second-to-last, etc.
        if matches!(direction, FetchDirection::Absolute(_)) && target < 0 {
            target = n + target + 1;
        }
        if target >= 1 && target <= n {
            (
                cursor.columns.clone(),
                Some(cursor.rows[(target - 1) as usize].clone()),
                target,
                true,
            )
        } else {
            (cursor.columns.clone(), None, target.clamp(0, n + 1), false)
        }
    };
    ctx.cursors.get_mut(&key).expect("cursor").position = new_position;
    if !in_range {
        // Off either end: @@FETCH_STATUS = -1, no row produced.
        ctx.fetch_status = -1;
        return Ok(StatementResult::Done);
    }
    ctx.fetch_status = 0;
    let row = fetched.expect("row in range");
    if into.is_empty() {
        // No INTO: the fetched row is returned to the client as a result set.
        return Ok(StatementResult::Rows(RowSet {
            columns,
            rows: vec![row],
        }));
    }
    if into.len() != columns.len() {
        return Err(SqlError::new(
            16924,
            16,
            1,
            "The number of variables declared in the INTO list must match that of selected columns."
                .to_string(),
        )
        .at(name.span));
    }
    let types: Vec<ColumnType> = columns.iter().map(|c| c.column_type).collect();
    for (var, (value, col_type)) in into.iter().zip(row.iter().zip(&types)) {
        let var_type = ctx
            .variables
            .get(var)
            .map(|(t, _)| *t)
            .ok_or_else(|| undeclared_variable_err(var))?;
        let sql_value = value::datum_to_sql(value, col_type);
        let expr = Expr {
            kind: ExprKind::Literal(sql_value),
            span: name.span,
        };
        let eval_ctx = ctx.eval_context();
        let coerced = coerce_variable(&expr, &var_type, var, &eval_ctx)?;
        ctx.variables.insert(var.clone(), (var_type, coerced));
    }
    Ok(StatementResult::Done)
}

fn exec_set(ctx: &mut TxnContext, set: &SetStatement) -> Result<StatementResult, SqlError> {
    match set {
        SetStatement::XactAbort(on) => ctx.xact_abort = *on,
        SetStatement::IsolationLevel(level) => {
            ctx.isolation = match level {
                IsolationLevel::ReadUncommitted => Isolation::ReadUncommitted,
                IsolationLevel::ReadCommitted => Isolation::ReadCommitted,
                IsolationLevel::RepeatableRead => Isolation::RepeatableRead,
                IsolationLevel::Serializable => Isolation::Serializable,
                IsolationLevel::Snapshot => Isolation::Snapshot,
            }
        }
        SetStatement::ShowplanText(on) => ctx.showplan_text = *on,
        SetStatement::NoCount(on) => ctx.nocount = *on,
        SetStatement::Variable { name, value } => {
            // "Statements that make a simple assignment always set the
            // @@ROWCOUNT value to 1" — the Done result would reset it to 0,
            // so the assignment records its own count here.
            ctx.rowcount = 1;
            let column_type = ctx
                .variables
                .get(name)
                .map(|(t, _)| *t)
                .ok_or_else(|| undeclared_variable_err(name))?;
            let eval_ctx = ctx.eval_context();
            let coerced = coerce_variable(value, &column_type, name, &eval_ctx)?;
            ctx.variables.insert(name.clone(), (column_type, coerced));
        }
        SetStatement::Ignored => {}
    }
    Ok(StatementResult::Done)
}

/// `DECLARE @a TYPE [= expr], ...`. Each variable is added to the batch (error
/// 134 if already declared); an initializer (which may reference an earlier
/// variable) is coerced to the declared type, else the value starts NULL.
fn exec_declare(ctx: &mut TxnContext, decls: &[Declaration]) -> Result<StatementResult, SqlError> {
    for decl in decls {
        // A name occupies the scalar and table-variable stores jointly, so a
        // scalar DECLARE after a `DECLARE @t TABLE` of the same name is 134 too.
        if ctx.variables.contains_key(&decl.name) || ctx.table_variables.contains_key(&decl.name) {
            return Err(SqlError::new(
                134,
                15,
                2,
                format!(
                    "The variable name '@{}' has already been declared. Variable names must be unique within a query batch.",
                    decl.name
                ),
            ));
        }
        let column_type = data_type_to_column_type(&decl.data_type, &decl.name)?;
        let value = match &decl.initializer {
            Some(expr) => {
                let eval_ctx = ctx.eval_context();
                coerce_variable(expr, &column_type, &decl.name, &eval_ctx)?
            }
            None => SqlValue::Null,
        };
        ctx.variables
            .insert(decl.name.clone(), (column_type, value));
    }
    Ok(StatementResult::Done)
}

/// `DECLARE @t TABLE ( ... )`: registers an empty in-memory table variable. Its
/// schema is bound like a base table's columns; its declared PRIMARY KEY becomes
/// the key columns used for uniqueness at INSERT time.
fn exec_declare_table_var(
    ctx: &mut TxnContext,
    name: &str,
    columns: &[ColumnDef],
    primary_key: &[Name],
) -> Result<StatementResult, SqlError> {
    // A name occupies the scalar and table-variable stores jointly.
    if ctx.variables.contains_key(name) || ctx.table_variables.contains_key(name) {
        return Err(SqlError::new(
            134,
            15,
            2,
            format!(
                "The variable name '@{name}' has already been declared. Variable names must be \
                 unique within a query batch."
            ),
        ));
    }
    let (schema, key_columns, defaults) = build_table_var_definition(name, columns, primary_key)?;
    ctx.table_variables.insert(
        name.to_string(),
        TableVar {
            schema,
            key_columns,
            defaults,
            rows: Vec::new(),
        },
    );
    Ok(StatementResult::Done)
}

/// A table variable's built definition: its column schema, the schema indices of
/// its PRIMARY KEY columns, and the per-column DEFAULT source text (parallel to
/// the schema columns).
type TableVarDefinition = (Schema, Vec<usize>, Vec<Option<String>>);

/// Builds the schema, key-column indices, and per-column DEFAULT source text for
/// a table-variable declaration (`DECLARE @name TABLE(cols)` and the RETURNS
/// clause of a multi-statement TVF share this): unique column names (2705), PK
/// columns forced NOT NULL (8111 on explicit-NULL, MAX-key rejected), and the
/// DEFAULT texts applied per INSERT. `name` (without `@`) names the table in the
/// error messages.
fn build_table_var_definition(
    name: &str,
    columns: &[ColumnDef],
    primary_key: &[Name],
) -> Result<TableVarDefinition, SqlError> {
    // Column names within the table variable must be unique (2705), the same
    // rule a base table enforces in exec_create_table.
    let mut seen: Vec<&str> = Vec::new();
    for column in columns {
        if seen
            .iter()
            .any(|n| n.eq_ignore_ascii_case(&column.name.value))
        {
            return Err(SqlError::new(
                2705,
                16,
                3,
                format!(
                    "Column names in each table must be unique. Column name '{}' is specified more than once.",
                    column.name.value
                ),
            )
            .at(column.name.span));
        }
        seen.push(&column.name.value);
    }
    let bound = columns
        .iter()
        .map(bind_column)
        .collect::<Result<Vec<_>, _>>()?;
    let mut schema = Schema { columns: bound };
    let mut key_columns = Vec::new();
    for pk in primary_key {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&pk.value))
            .ok_or_else(|| {
                SqlError::new(
                    1911,
                    16,
                    1,
                    format!(
                        "Column name '{}' does not exist in the target table or view.",
                        pk.value
                    ),
                )
            })?;
        // A PRIMARY KEY column is implicitly NOT NULL; declaring it NULL is
        // 8111, and a MAX-typed column cannot be a key — the same rules a base
        // table enforces in exec_create_table.
        let declared_null = columns
            .iter()
            .find(|c| c.name.eq_ignore_case(&pk.value))
            .and_then(|c| c.nullable)
            == Some(true);
        if declared_null {
            return Err(SqlError::new(
                8111,
                16,
                1,
                format!(
                    "Cannot define PRIMARY KEY constraint on nullable column in table '@{name}'."
                ),
            ));
        }
        if schema.columns[index].column_type.is_max() {
            return Err(max_key_column_error(&pk.value, &format!("@{name}")).at(pk.span));
        }
        schema.columns[index].nullable = false;
        key_columns.push(index);
    }
    // Per-column DEFAULT source text (parallel to the schema columns), applied
    // at INSERT to columns left unspecified — same as a base table.
    let defaults: Vec<Option<String>> = columns.iter().map(|c| c.default.clone()).collect();
    Ok((schema, key_columns, defaults))
}

fn undeclared_variable_err(name: &str) -> SqlError {
    SqlError::new(
        137,
        15,
        2,
        format!("Must declare the scalar variable \"@{name}\"."),
    )
}

/// Evaluates a variable initializer/assignment (a constant expression that may
/// reference already-declared variables) and coerces it to the declared type.
fn coerce_variable(
    expr: &Expr,
    column_type: &ColumnType,
    name: &str,
    eval_ctx: &EvalContext,
) -> Result<SqlValue, SqlError> {
    let sql_value = eval_constant(expr, eval_ctx)?;
    let datum = value::sql_to_datum(&sql_value, column_type, name)?;
    Ok(value::datum_to_sql(&datum, column_type))
}

// ---- CREATE TABLE -------------------------------------------------------

fn exec_create_table(
    storage: &Storage,
    db_id: u32,
    create: &CreateTable,
) -> Result<StatementResult, SqlError> {
    // Strip an optional `dbo.` schema prefix so the table is stored (and
    // later resolved) under its bare name.
    let table_name = create_object_name("CREATE TABLE", &create.table)?;
    if resolve_table(storage, db_id, table_name).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{table_name}' in the database."),
        ));
    }

    let mut seen = Vec::new();
    let mut columns = Vec::with_capacity(create.columns.len());
    for column in &create.columns {
        if seen
            .iter()
            .any(|n: &String| n.eq_ignore_ascii_case(&column.name.value))
        {
            return Err(SqlError::new(
                2705,
                16,
                3,
                format!(
                    "Column names in each table must be unique. Column name '{}' is specified more than once.",
                    column.name.value
                ),
            ));
        }
        seen.push(column.name.value.clone());
        columns.push(bind_column(column)?);
    }

    // Primary key columns must exist and are implicitly NOT NULL (declaring
    // one explicitly NULL is an error, matching SQL Server 8111).
    let mut key_names = Vec::new();
    for key in &create.primary_key {
        let Some(index) = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&key.value))
        else {
            return Err(SqlError::new(
                1750,
                16,
                0,
                format!(
                    "Column '{}' in the PRIMARY KEY is not a column of the table.",
                    key.value
                ),
            )
            .at(key.span));
        };
        let declared_null = create
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_case(&key.value))
            .and_then(|c| c.nullable)
            == Some(true);
        if declared_null {
            return Err(SqlError::new(
                8111,
                16,
                1,
                format!(
                    "Cannot define PRIMARY KEY constraint on nullable column in table '{table_name}'."
                ),
            ));
        }
        if columns[index].column_type.is_max() {
            return Err(max_key_column_error(&key.value, table_name).at(key.span));
        }
        columns[index].nullable = false;
        key_names.push(columns[index].name.clone());
    }

    // Per-column DEFAULT source text (parallel to columns).
    let defaults: Vec<Option<String>> = create.columns.iter().map(|c| c.default.clone()).collect();

    // At most one IDENTITY column, on an integer type.
    let mut identity: Option<catalog::IdentitySpec> = None;
    for (index, column) in create.columns.iter().enumerate() {
        let Some(id) = column.identity else { continue };
        if identity.is_some() {
            return Err(SqlError::new(
                2744,
                16,
                2,
                format!(
                    "Multiple identity columns specified for table '{table_name}'. Only one identity column per table is allowed."
                ),
            ));
        }
        if !matches!(
            columns[index].column_type,
            ColumnType::TinyInt | ColumnType::SmallInt | ColumnType::Int | ColumnType::BigInt
        ) {
            return Err(SqlError::new(
                2749,
                16,
                2,
                format!(
                    "Identity column '{}' must be of a data type that is an integer.",
                    column.name.value
                ),
            )
            .at(column.span));
        }
        if column.default.is_some() {
            return Err(SqlError::new(
                1754,
                16,
                1,
                "Defaults cannot be created on columns with an IDENTITY attribute.".to_string(),
            )
            .at(column.span));
        }
        identity = Some(catalog::IdentitySpec {
            column: index,
            seed: id.seed,
            increment: id.increment,
            next: id.seed,
        });
    }

    // CHECK constraints (column-level + table-level): validate, name, and
    // fold into the catalog. Validation needs the bound columns.
    let check_constraints = build_check_defs(create, &columns, table_name)?;
    // FOREIGN KEY constraints: validate against the (possibly self-)referenced
    // table's primary key and order each child column to the parent's PK.
    // Constraint names are unique across kinds, so seed with the check names.
    let check_names: Vec<String> = check_constraints.iter().map(|c| c.name.clone()).collect();
    let foreign_keys =
        build_foreign_key_defs(db_id, storage, create, &columns, table_name, &check_names)?;

    // UNIQUE constraints become unique indexes. Resolve their columns now (while
    // `columns` is in hand) so an invalid column errors before the table exists.
    let mut unique_indexes: Vec<(String, Vec<(usize, bool)>)> = Vec::new();
    for (i, uc) in create.unique_constraints.iter().enumerate() {
        let mut cols = Vec::with_capacity(uc.columns.len());
        for col in &uc.columns {
            let index = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&col.value))
                .ok_or_else(|| SqlError::invalid_column(&col.value).at(col.span))?;
            cols.push((index, true));
        }
        let name = uc
            .name
            .as_ref()
            .map(|n| n.value.clone())
            .unwrap_or_else(|| format!("UQ_{table_name}_{}", i + 1));
        unique_indexes.push((name, cols));
    }

    storage
        .rel_create_table(
            db_id,
            table_name,
            columns,
            &key_names,
            defaults,
            identity,
            check_constraints,
            foreign_keys,
        )
        .map_err(|err| map_storage_err(err, table_name))?;
    for (name, cols) in unique_indexes {
        storage
            .rel_create_index(db_id, table_name, name, cols, true, Vec::new())
            .map_err(|err| map_storage_err(err, table_name))?;
    }
    Ok(StatementResult::Done)
}

/// Collects and validates a table's FOREIGN KEY constraints (column-level, then
/// table-level), assigning a name to unnamed ones. `check_names` are the names
/// already taken by the table's CHECK constraints so a FK cannot reuse one
/// (constraint names are unique across kinds).
fn build_foreign_key_defs(
    db_id: u32,
    storage: &Storage,
    create: &CreateTable,
    columns: &[Column],
    table_name: &str,
    check_names: &[String],
) -> Result<Vec<catalog::ForeignKeyDef>, SqlError> {
    let raw = create
        .columns
        .iter()
        .flat_map(|c| c.foreign_keys.iter())
        .chain(create.foreign_keys.iter());

    // The parent's primary key (name, type) per PK column, in PK order. A
    // self-reference reads it from this CREATE; otherwise from the catalog.
    let self_pk = || -> Result<Vec<(String, ColumnType)>, SqlError> {
        create
            .primary_key
            .iter()
            .map(|k| {
                let col = columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&k.value))
                    .expect("primary key column bound");
                Ok((col.name.clone(), col.column_type))
            })
            .collect()
    };

    let mut names: Vec<String> = check_names.to_vec();
    let mut defs = Vec::new();
    for fk in raw {
        let parent_bare = strip_schema(&fk.parent.value);
        let is_self = parent_bare.eq_ignore_ascii_case(table_name);
        // Parent primary key: (column name, type) in PK order.
        let parent_pk: Vec<(String, ColumnType)> = if is_self {
            self_pk()?
        } else {
            let parent = resolve_table(storage, db_id, &fk.parent.value)
                .ok_or_else(|| SqlError::invalid_object(&fk.parent.value).at(fk.parent.span))?;
            let schema = parent
                .schema()
                .map_err(|e| map_storage_err(e, &parent.name))?;
            parent
                .key_columns
                .iter()
                .map(|&i| {
                    (
                        schema.columns[i].name.clone(),
                        schema.columns[i].column_type,
                    )
                })
                .collect()
        };
        let def = bind_foreign_key(fk, columns, table_name, &parent_pk, parent_bare, &names)?;
        names.push(def.name.clone());
        defs.push(def);
    }
    Ok(defs)
}

/// Validates one FOREIGN KEY against the parent's primary key and produces a
/// [`catalog::ForeignKeyDef`] whose child column indices are ordered to match
/// the parent's PK. Referenced columns must be exactly the parent PK (SQL
/// Server requires a unique/PK target); child and parent column types and
/// counts must match.
fn bind_foreign_key(
    fk: &ForeignKey,
    columns: &[Column],
    table_name: &str,
    parent_pk: &[(String, ColumnType)],
    parent_bare: &str,
    existing_names: &[String],
) -> Result<catalog::ForeignKeyDef, SqlError> {
    let no_key = || {
        SqlError::new(
            1776,
            16,
            0,
            format!(
                "There are no primary or candidate keys in the referenced table '{parent_bare}' that match the referencing column list in the foreign key."
            ),
        )
        .at(fk.parent.span)
    };
    if parent_pk.is_empty() {
        return Err(no_key());
    }
    // Referenced parent columns (defaulting to the whole PK) paired with the
    // child columns positionally.
    let parent_cols: Vec<String> = if fk.parent_columns.is_empty() {
        parent_pk.iter().map(|(n, _)| n.clone()).collect()
    } else {
        fk.parent_columns.iter().map(|n| n.value.clone()).collect()
    };
    if fk.columns.len() != parent_cols.len() {
        return Err(SqlError::new(
            1776,
            16,
            0,
            "The number of referencing columns differs from the number of referenced columns.",
        )
        .at(fk.span));
    }
    // The referenced set must be exactly the parent PK (order-independent).
    if parent_cols.len() != parent_pk.len()
        || !parent_pk
            .iter()
            .all(|(pk, _)| parent_cols.iter().any(|c| c.eq_ignore_ascii_case(pk)))
    {
        return Err(no_key());
    }

    // Resolve child column indices and check each child/parent type matches.
    let child_index = |name: &Name| -> Result<usize, SqlError> {
        columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&name.value))
            .ok_or_else(|| SqlError::invalid_column(&name.value).at(name.span))
    };
    // For each parent PK column (in PK order), find the child column mapped to
    // it and record its index — so the stored order matches the parent PK.
    let mut ordered = Vec::with_capacity(parent_pk.len());
    for (pk_name, pk_type) in parent_pk {
        // Which referenced position names this PK column?
        let pos = parent_cols
            .iter()
            .position(|c| c.eq_ignore_ascii_case(pk_name))
            .ok_or_else(no_key)?;
        let child_col = &fk.columns[pos];
        let idx = child_index(child_col)?;
        if columns[idx].column_type != *pk_type {
            return Err(SqlError::new(
                1778,
                16,
                0,
                format!(
                    "Column '{table_name}.{}' is not the same data type as referencing column '{parent_bare}.{pk_name}' in the foreign key.",
                    columns[idx].name
                ),
            )
            .at(child_col.span));
        }
        ordered.push(idx);
    }

    let name = match &fk.name {
        Some(n) => {
            if existing_names
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&n.value))
            {
                return Err(SqlError::new(
                    2714,
                    16,
                    5,
                    format!(
                        "There is already an object named '{}' in the database.",
                        n.value
                    ),
                )
                .at(n.span));
            }
            n.value.clone()
        }
        None => {
            let mut seq = 0u32;
            loop {
                seq += 1;
                let candidate = format!("FK__{table_name}__{parent_bare}__{seq}");
                if !existing_names
                    .iter()
                    .any(|e| e.eq_ignore_ascii_case(&candidate))
                {
                    break candidate;
                }
            }
        }
    };
    Ok(catalog::ForeignKeyDef {
        name,
        columns: ordered,
        parent: parent_bare.to_string(),
    })
}

/// Collects a table's CHECK constraints (column-level, then table-level) and
/// binds each ([`bind_check`]), threading the running name list so unnamed
/// constraints get unique auto names and duplicate explicit names are caught.
fn build_check_defs(
    create: &CreateTable,
    columns: &[Column],
    table_name: &str,
) -> Result<Vec<catalog::CheckDef>, SqlError> {
    let raw = create
        .columns
        .iter()
        .flat_map(|c| c.checks.iter())
        .chain(create.check_constraints.iter());

    let mut names: Vec<String> = Vec::new();
    let mut defs = Vec::new();
    for check in raw {
        let def = bind_check(check, columns, table_name, &names)?;
        names.push(def.name.clone());
        defs.push(def);
    }
    Ok(defs)
}

/// Validates one CHECK constraint against a table's columns and its existing
/// constraint names: the predicate must parse and reference only real columns
/// (207/4104); an explicit name must not collide (2714); an unnamed check is
/// assigned the first free `CK__<table>__<n>`.
fn bind_check(
    check: &CheckConstraint,
    columns: &[Column],
    table_name: &str,
    existing_names: &[String],
) -> Result<catalog::CheckDef, SqlError> {
    let expr = truthdb_sql::parse_expr(&check.predicate)?;
    validate_check_columns(&expr, columns)?;
    let name = match &check.name {
        Some(n) => {
            if existing_names
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&n.value))
            {
                return Err(SqlError::new(
                    2714,
                    16,
                    5,
                    format!(
                        "There is already an object named '{}' in the database.",
                        n.value
                    ),
                )
                .at(n.span));
            }
            n.value.clone()
        }
        None => {
            let mut seq = 0u32;
            loop {
                seq += 1;
                let candidate = format!("CK__{table_name}__{seq}");
                if !existing_names
                    .iter()
                    .any(|e| e.eq_ignore_ascii_case(&candidate))
                {
                    break candidate;
                }
            }
        }
    };
    Ok(catalog::CheckDef {
        name,
        predicate: check.predicate.clone(),
    })
}

/// Rejects a CHECK predicate that references a column the table does not have
/// (error 207). Only column existence is checked here; type/boolean validity
/// is left to per-row evaluation.
fn validate_check_columns(expr: &Expr, columns: &[Column]) -> Result<(), SqlError> {
    match &expr.kind {
        ExprKind::Column(name) => {
            // A CHECK may only reference columns of its own table by their bare
            // name. A multi-part identifier (`t.col`) can't be resolved by the
            // bare-name enforcement resolver, so reject it here (4104) rather
            // than accept a table that then rejects every INSERT with 207.
            if name.value.contains('.') {
                return Err(SqlError::new(
                    4104,
                    16,
                    1,
                    format!(
                        "The multi-part identifier \"{}\" could not be bound.",
                        name.value
                    ),
                )
                .at(name.span));
            }
            if columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&name.value))
            {
                Ok(())
            } else {
                Err(SqlError::invalid_column(&name.value).at(name.span))
            }
        }
        ExprKind::Unary { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. } => validate_check_columns(expr, columns),
        ExprKind::Binary { left, right, .. } => {
            validate_check_columns(left, columns)?;
            validate_check_columns(right, columns)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_check_columns(expr, columns)?;
            validate_check_columns(pattern, columns)
        }
        ExprKind::InList { expr, list, .. } => {
            validate_check_columns(expr, columns)?;
            list.iter()
                .try_for_each(|e| validate_check_columns(e, columns))
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_check_columns(expr, columns)?;
            validate_check_columns(low, columns)?;
            validate_check_columns(high, columns)
        }
        ExprKind::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(op) = operand {
                validate_check_columns(op, columns)?;
            }
            for (when, then) in branches {
                validate_check_columns(when, columns)?;
                validate_check_columns(then, columns)?;
            }
            if let Some(e) = else_result {
                validate_check_columns(e, columns)?;
            }
            Ok(())
        }
        ExprKind::Function { args, .. } => args
            .iter()
            .try_for_each(|a| validate_check_columns(a, columns)),
        ExprKind::Aggregate { arg, .. } => arg
            .as_ref()
            .map_or(Ok(()), |a| validate_check_columns(a, columns)),
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_)
        | ExprKind::GlobalVar(_)
        | ExprKind::LocalVar(_) => Ok(()),
        // Subqueries are not allowed in a CHECK constraint (SQL Server 1046).
        ExprKind::Subquery(_) | ExprKind::Exists(_) | ExprKind::InSubquery { .. } => {
            Err(SqlError::new(
                1046,
                15,
                1,
                "Subqueries are not allowed in this context. Only scalar expressions are allowed.",
            ))
        }
    }
}

/// Parses a table's stored CHECK predicates once (per statement) for row
/// enforcement, pairing each with its constraint name.
fn parse_checks(def: &TableDef) -> Result<Vec<(String, Expr)>, SqlError> {
    def.check_constraints
        .iter()
        .map(|c| Ok((c.name.clone(), truthdb_sql::parse_expr(&c.predicate)?)))
        .collect()
}

/// Enforces CHECK constraints against a fully-built row (schema order). A
/// constraint passes on TRUE or UNKNOWN (NULL); FALSE is error 547.
#[allow(clippy::too_many_arguments)]
fn enforce_checks(
    storage: &Storage,
    checks: &[(String, Expr)],
    row: &[SqlValue],
    resolver: &impl ColumnResolver,
    eval_ctx: &EvalContext,
    verb: &str,
    database: &str,
    table: &str,
) -> Result<(), SqlError> {
    for (name, expr) in checks {
        // A user scalar function (or subquery) in the CHECK is folded against the
        // row before the pure evaluator runs, like the other clause positions.
        let bound;
        let expr = if expr_needs_binding(storage, eval_ctx.database_id, expr) {
            let outer = |n: &str| resolver.resolve(n);
            bound = substitute_correlated_in_expr(storage, expr, &outer, row, eval_ctx)?;
            &bound
        } else {
            expr
        };
        match eval::eval(expr, row, resolver, eval_ctx)? {
            SqlValue::Bool(false) => {
                return Err(SqlError::new(
                    547,
                    16,
                    0,
                    format!(
                        "The {verb} statement conflicted with the CHECK constraint \"{name}\". The conflict occurred in database \"{database}\", table \"dbo.{table}\".",
                    ),
                ));
            }
            SqlValue::Bool(true) | SqlValue::Null => {}
            _ => {
                return Err(SqlError::new(
                    4145,
                    15,
                    1,
                    format!(
                        "An expression of non-boolean type specified in a context where a condition is expected, near the CHECK constraint \"{name}\"."
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// A child row's referencing key for one foreign key (the FK columns in parent
/// primary-key order). `None` if any FK column is NULL — MATCH SIMPLE, which
/// skips enforcement (the NULL-FK trap).
fn fk_key(fk: &catalog::ForeignKeyDef, row: &[Datum]) -> Option<Vec<Datum>> {
    let key: Vec<Datum> = fk.columns.iter().map(|&i| row[i].clone()).collect();
    if key.iter().any(|d| matches!(d, Datum::Null)) {
        None
    } else {
        Some(key)
    }
}

/// Whether a referencing `key` (parent PK order) exists in the parent — either
/// a committed parent row, or, for a self-reference, a sibling row in `batch`
/// (whose PK columns are `child.key_columns`).
fn fk_parent_exists(
    storage: &Storage,
    fk: &catalog::ForeignKeyDef,
    key: &[Datum],
    child: &TableDef,
    batch: &[Vec<Datum>],
) -> Result<bool, SqlError> {
    if storage
        .rel_get(child.database_id, &fk.parent, key)
        .map_err(|e| map_storage_err(e, &fk.parent))?
        .is_some()
    {
        return Ok(true);
    }
    if fk.parent.eq_ignore_ascii_case(&child.name) && child.key_columns.len() == key.len() {
        // Fold both the referencing key and each sibling's PK by the parent PK
        // collation, so a case-insensitive self-reference matches a case-variant
        // sibling in the same statement — consistent with the folded `rel_get`
        // above (which handles the committed-row case).
        let key_coll: Vec<Option<String>> = child
            .key_columns
            .iter()
            .map(|&i| child.collations.get(i).cloned().flatten())
            .collect();
        let folded_key = collated_key(key, &key_coll);
        return Ok(batch.iter().any(|r| {
            let sibling: Vec<Datum> = child.key_columns.iter().map(|&i| r[i].clone()).collect();
            collated_key(&sibling, &key_coll) == folded_key
        }));
    }
    Ok(false)
}

/// The canonical name of a database id, for error text (the default
/// database's configured name when the id is unknown — a dropped database's
/// error still renders).
fn database_name_of(storage: &Storage, db_id: u32) -> String {
    storage
        .rel_databases()
        .into_iter()
        .find(|(id, _)| *id == db_id)
        .map(|(_, name)| name)
        .unwrap_or_else(|| storage.default_database_name())
}

fn fk_child_violation(database: &str, name: &str, verb: &str, parent: &str) -> SqlError {
    SqlError::new(
        547,
        16,
        0,
        format!(
            "The {verb} statement conflicted with the FOREIGN KEY constraint \"{name}\". The conflict occurred in database \"{database}\", table \"dbo.{parent}\".",
        ),
    )
}

/// Enforces this table's FOREIGN KEY constraints against a built child row:
/// each non-NULL referencing key must exist in the parent's primary key. For a
/// self-reference, a sibling row in the same statement (`batch`) also satisfies
/// it. A missing parent is error 547. `check_self_ref` skips self-referencing
/// foreign keys (an UPDATE validates those against its post-update snapshot,
/// since a pre-mutation probe would see stale rows).
fn enforce_child_fks(
    storage: &Storage,
    def: &TableDef,
    row: &[Datum],
    batch: &[Vec<Datum>],
    verb: &str,
    check_self_ref: bool,
) -> Result<(), SqlError> {
    for fk in &def.foreign_keys {
        if !check_self_ref && fk.parent.eq_ignore_ascii_case(&def.name) {
            continue;
        }
        let Some(key) = fk_key(fk, row) else {
            continue; // NULL referencing column: not enforced
        };
        if !fk_parent_exists(storage, fk, &key, def, batch)? {
            return Err(fk_child_violation(
                &database_name_of(storage, def.database_id),
                &fk.name,
                verb,
                &fk.parent,
            ));
        }
    }
    Ok(())
}

/// A child index whose leading key columns are exactly the FK's child columns,
/// usable to probe for referencing rows by seeking the removed parent key
/// instead of scanning the whole child.
fn fk_probe_index<'a>(
    child: &'a TableDef,
    fk: &catalog::ForeignKeyDef,
) -> Option<&'a catalog::IndexDef> {
    child.indexes.iter().find(|index| {
        index.columns.len() >= fk.columns.len()
            && index
                .columns
                .iter()
                .zip(&fk.columns)
                .all(|((col, _asc), &fk_col)| *col == fk_col)
    })
}

/// Whether the child FK columns and the referenced parent PK columns have the
/// same case sensitivity. The FK index fast path folds the probe key by the
/// *child* column collation (to match the child index's folded keys), while the
/// insert-time check (`rel_get`) and the scan fallback fold by the *parent* PK
/// collation; when they disagree (a mixed-collation FK) the fast path can miss a
/// reference, so it is only used when the collations match — otherwise the scan
/// fallback (parent collation, consistent with insert) handles it.
fn fk_collations_match(child: &TableDef, fk: &catalog::ForeignKeyDef, parent: &TableDef) -> bool {
    fk.columns.len() == parent.key_columns.len()
        && fk.columns.iter().zip(&parent.key_columns).all(|(&c, &p)| {
            CollationSensitivity::from_optional(child.collations.get(c).and_then(|x| x.as_deref()))
                == CollationSensitivity::from_optional(
                    parent.collations.get(p).and_then(|x| x.as_deref()),
                )
        })
}

/// The error raised when a surviving child row references a removed parent key.
fn reference_conflict(database: &str, verb: &str, fk_name: &str, child_name: &str) -> SqlError {
    SqlError::new(
        547,
        16,
        0,
        format!(
            "The {verb} statement conflicted with the REFERENCE constraint \"{fk_name}\". The conflict occurred in database \"{database}\", table \"dbo.{child_name}\"."
        ),
    )
}

/// Enforces NO ACTION on the parent side: no surviving child row may reference
/// any of `removed_keys` (parent primary-key values being deleted or vacated by
/// an UPDATE). A referencing child is error 547. When the child has an index on
/// the FK columns, each removed key is probed by an index seek; otherwise the
/// child is scanned.
fn enforce_parent_fks(
    storage: &Storage,
    parent: &TableDef,
    removed_keys: &[Vec<Datum>],
    verb: &str,
    check_self_ref: bool,
) -> Result<(), SqlError> {
    if removed_keys.is_empty() {
        return Ok(());
    }
    // Fold the removed parent keys by the parent PK collation so the scan
    // fallback matches child references case-insensitively — the same folding the
    // index fast path gets from the child index's key encoding.
    let parent_key_coll: Vec<Option<String>> = parent
        .key_columns
        .iter()
        .map(|&i| parent.collations.get(i).cloned().flatten())
        .collect();
    let removed_folded: Vec<Vec<u8>> = removed_keys
        .iter()
        .map(|k| collated_key(k, &parent_key_coll))
        .collect();
    // Children live in the parent's database — cross-database foreign keys
    // do not exist, and lock analysis (fk_child_object_ids) filters the same
    // way; the two derivations must agree.
    let children: Vec<TableDef> = storage
        .rel_tables()
        .into_iter()
        .filter(|t| {
            t.database_id == parent.database_id
                && t.foreign_keys
                    .iter()
                    .any(|fk| fk.parent.eq_ignore_ascii_case(&parent.name))
        })
        .collect();
    for child in &children {
        let self_ref = child.name.eq_ignore_ascii_case(&parent.name);
        // A self-referencing table's own FKs are validated against the
        // post-update snapshot, not the pre-mutation child scan.
        if self_ref && !check_self_ref {
            continue;
        }
        for fk in &child.foreign_keys {
            if !fk.parent.eq_ignore_ascii_case(&parent.name) {
                continue;
            }
            // Fast path: an index on the FK columns lets us seek each removed
            // parent key instead of scanning the child. Not used for a
            // self-reference (whose own being-removed rows must be excluded). If
            // a key fails to encode (unexpected type mismatch), fall back to the
            // scan rather than risk missing a reference.
            if !self_ref
                && fk_collations_match(child, fk, parent)
                && let Some(index) = fk_probe_index(child, fk)
            {
                let mut handled = true;
                for key in removed_keys {
                    match crate::relstore::index::encode_index_prefix(
                        key,
                        &index.columns,
                        &child.collations,
                    ) {
                        Ok(lower) => {
                            let upper = crate::relstore::index::prefix_upper_bound(&lower);
                            let matches = storage
                                .rel_index_scan(
                                    child.database_id,
                                    &child.name,
                                    index.object_id,
                                    Some(lower),
                                    upper,
                                    None,
                                    false,
                                    // Integrity probe: must see the current
                                    // state, never a snapshot.
                                    None,
                                )
                                .map_err(|e| map_storage_err(e, &child.name))?;
                            if !matches.is_empty() {
                                return Err(reference_conflict(
                                    &database_name_of(storage, child.database_id),
                                    verb,
                                    &fk.name,
                                    &child.name,
                                ));
                            }
                        }
                        Err(_) => {
                            handled = false;
                            break;
                        }
                    }
                }
                if handled {
                    continue;
                }
            }
            // Fallback: scan the child and compare each row's FK key.
            let child_rows = storage
                .rel_scan(child.database_id, &child.name)
                .map_err(|e| map_storage_err(e, &child.name))?;
            for row in &child_rows {
                // A self-referencing row that is itself being removed does not
                // count as a surviving reference.
                if self_ref {
                    let pk: Vec<Datum> =
                        parent.key_columns.iter().map(|&i| row[i].clone()).collect();
                    if removed_folded.contains(&collated_key(&pk, &parent_key_coll)) {
                        continue;
                    }
                }
                let Some(key) = fk_key(fk, row) else {
                    continue;
                };
                if removed_folded.contains(&collated_key(&key, &parent_key_coll)) {
                    return Err(reference_conflict(
                        &database_name_of(storage, child.database_id),
                        verb,
                        &fk.name,
                        &child.name,
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The primary-key values of a row (in key-column order).
fn pk_of(def: &TableDef, row: &[Datum]) -> Vec<Datum> {
    def.key_columns.iter().map(|&i| row[i].clone()).collect()
}

/// A key's collation-canonical bytes (`collations` parallel to `values`), for
/// comparing keys by value — the FK scan fallback and the self-reference checks.
///
/// This encodes exactly as the index key does, so "equal" here means what it
/// means to a seek: two keys match when the collation says they do, including
/// case- and accent-insensitively. Comparing the encoded bytes rather than the
/// values is what keeps the two definitions from drifting apart.
fn collated_key(values: &[Datum], collations: &[Option<String>]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, value) in values.iter().enumerate() {
        // A key column always encodes; a type error here would mean the row did
        // not come from this table.
        let _ = crate::relstore::key::encode_datum_collated(
            value,
            collations.get(i).and_then(|c| c.as_deref()),
            &mut out,
        );
    }
    out
}

/// Maps a parsed [`DataType`] to a storage [`ColumnType`], validating length
/// bounds. `name` is only used for the length-overflow error message.
fn data_type_to_column_type(data_type: &DataType, name: &str) -> Result<ColumnType, SqlError> {
    Ok(match data_type {
        DataType::TinyInt => ColumnType::TinyInt,
        DataType::SmallInt => ColumnType::SmallInt,
        DataType::Int => ColumnType::Int,
        DataType::BigInt => ColumnType::BigInt,
        DataType::Bit => ColumnType::Bit,
        DataType::Real => ColumnType::Real,
        DataType::Float => ColumnType::Float,
        DataType::Decimal { precision, scale } => ColumnType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Date => ColumnType::Date,
        DataType::Time => ColumnType::Time,
        DataType::DateTime2 => ColumnType::DateTime2,
        DataType::UniqueIdentifier => ColumnType::UniqueIdentifier,
        DataType::VarChar(n) => ColumnType::VarChar {
            max_len: length(*n, name)?,
        },
        DataType::NVarChar(n) => ColumnType::NVarChar {
            max_len: length(*n, name)?,
        },
        DataType::VarBinary(n) => ColumnType::VarBinary {
            max_len: length(*n, name)?,
        },
        DataType::VarCharMax => ColumnType::VarCharMax,
        DataType::NVarCharMax => ColumnType::NVarCharMax,
        DataType::VarBinaryMax => ColumnType::VarBinaryMax,
    })
}

/// Binds a declared column. A character column left without an explicit
/// `COLLATE` keeps `None` here and is resolved to the database default by
/// `rel_create_table`, the one point every CREATE TABLE passes through.
fn bind_column(column: &ColumnDef) -> Result<Column, SqlError> {
    let column_type = data_type_to_column_type(&column.data_type, &column.name.value)?;
    // A COLLATE clause is only meaningful on character columns.
    if column.collation.is_some()
        && !matches!(
            column_type,
            ColumnType::VarChar { .. } | ColumnType::NVarChar { .. }
        )
    {
        return Err(SqlError::new(
            4536,
            16,
            1,
            format!(
                "COLLATE clause cannot be used on column '{}' because its data type is not character based.",
                column.name.value
            ),
        )
        .at(column.span));
    }
    // Columns are nullable by default (SQL Server ANSI default), PK columns
    // and explicit NOT NULL are not.
    let nullable = column.nullable.unwrap_or(!column.primary_key);
    Ok(Column {
        name: column.name.value.clone(),
        column_type,
        nullable,
        collation: column.collation.clone(),
    })
}

fn length(n: u32, name: &str) -> Result<u16, SqlError> {
    u16::try_from(n).map_err(|_| {
        SqlError::new(
            131,
            15,
            2,
            format!("The size for column '{name}' exceeds the maximum."),
        )
    })
}

// ---- DROP TABLE ---------------------------------------------------------

fn exec_drop_table(
    storage: &Storage,
    db_id: u32,
    drop: &DropTable,
) -> Result<StatementResult, SqlError> {
    // DROP TABLE does not drop a view or a procedure (use the matching DROP).
    // The object exists but is the wrong type, so error even under IF EXISTS
    // rather than silently no-op — the review showed DROP TABLE silently
    // DESTROYING a procedure through the shared catalog path.
    if resolve_table(storage, db_id, &drop.table.value)
        .is_some_and(|d| d.is_view() || d.is_procedure() || d.is_function() || d.is_trigger())
    {
        return Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the table '{}', because it does not exist or you do not have permission.",
                drop.table.value
            ),
        ));
    }
    let resolved = resolve_table(storage, db_id, &drop.table.value);
    match resolved {
        Some(def) => {
            // Everything below acts on the RESOLVED table's database — a
            // three-part DROP TABLE names another database's table.
            let (target_db, name, parent_oid) = (def.database_id, def.name, def.object_id);
            // A table still referenced by another table's foreign key cannot be
            // dropped (SQL Server 3726) — it would leave a dangling reference.
            if let Some(child) = storage.rel_tables().into_iter().find(|t| {
                t.database_id == target_db
                    && !t.name.eq_ignore_ascii_case(&name)
                    && t.foreign_keys
                        .iter()
                        .any(|fk| fk.parent.eq_ignore_ascii_case(&name))
            }) {
                let referencing = child
                    .foreign_keys
                    .iter()
                    .find(|fk| fk.parent.eq_ignore_ascii_case(&name))
                    .map(|fk| fk.name.clone())
                    .unwrap_or_default();
                return Err(SqlError::new(
                    3726,
                    16,
                    1,
                    format!(
                        "Could not drop object '{name}' because it is referenced by a FOREIGN KEY constraint '{referencing}'."
                    ),
                ));
            }
            // Cascade-drop the table's triggers — a trigger outlives its parent
            // table nowhere in SQL Server, and an orphan would permanently block
            // its own name (and dangle in sys.triggers).
            let orphan_triggers: Vec<String> = storage
                .rel_tables()
                .into_iter()
                .filter(|d| {
                    d.trigger
                        .as_ref()
                        .is_some_and(|t| t.parent_object_id == parent_oid)
                })
                .map(|d| d.name)
                .collect();
            for trigger_name in orphan_triggers {
                storage
                    .rel_drop_table(target_db, &trigger_name)
                    .map_err(|err| map_storage_err(err, &trigger_name))?;
            }
            storage
                .rel_drop_table(target_db, &name)
                .map_err(|err| map_storage_err(err, &drop.table.value))?;
            Ok(StatementResult::Done)
        }
        None if drop.if_exists => Ok(StatementResult::Done),
        None => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the table '{}', because it does not exist or you do not have permission.",
                drop.table.value
            ),
        )),
    }
}

// ---- CREATE / DROP VIEW -------------------------------------------------

/// Parses a stored view definition back into its `SELECT`. The text was
/// validated at CREATE, so this only fails on catalog corruption.
fn parse_view_query(text: &str, view_name: &str) -> Result<Select, SqlError> {
    match truthdb_sql::parse(text)?.into_iter().next() {
        Some(Statement::Select(select)) => Ok(select),
        _ => Err(SqlError::message_only(
            208,
            format!("The definition of view '{view_name}' is not a SELECT."),
        )),
    }
}

fn exec_create_view(
    storage: &Storage,
    db_id: u32,
    create: &CreateView,
) -> Result<StatementResult, SqlError> {
    let bare = create_object_name("CREATE VIEW", &create.name)?;
    if resolve_table(storage, db_id, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    // Validate the definition parses as a SELECT now; base-table and column
    // resolution is deferred to query time (SQL Server-style deferred name
    // resolution — a view over a not-yet-created table is allowed).
    parse_view_query(&create.query_text, bare)?;
    storage
        .rel_create_view(db_id, bare, &create.query_text)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// A parameter default must be a CONSTANT (SQL Server rejects at CREATE):
/// literals, NULL, and a signed literal — never variables or functions,
/// which would otherwise evaluate against each CALLER's scope and drift.
fn constant_default(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::Null
        | ExprKind::Int(_)
        | ExprKind::Number(_)
        | ExprKind::Str(_)
        | ExprKind::Bool(_)
        | ExprKind::Literal(_) => true,
        ExprKind::Unary { expr, .. } => constant_default(expr),
        _ => false,
    }
}

fn exec_create_procedure(
    storage: &Storage,
    db_id: u32,
    create: &CreateProcedure,
) -> Result<StatementResult, SqlError> {
    let bare = create_object_name("CREATE PROCEDURE", &create.name)?;
    // The builtin dispatcher checks `sp_executesql` BEFORE the catalog, so a
    // user procedure with that name would execute as the builtin while lock
    // ANALYSIS resolved the catalog first — an unanalyzed inner batch (the
    // review's shadow finding). Refuse the shadow outright.
    if bare.eq_ignore_ascii_case("sp_executesql") {
        return Err(SqlError::new(
            2714,
            16,
            6,
            "The name 'sp_executesql' is reserved for the system procedure.",
        ));
    }
    let params = create
        .params
        .iter()
        .map(|p| -> Result<ProcParamDef, SqlError> {
            // The declared type round-trips through the column-type spec
            // parser, exactly like table columns.
            let column_type = data_type_to_column_type(&p.data_type, &p.name)?;
            if let Some(text) = &p.default_text {
                let expr = truthdb_sql::parse_expr(text)?;
                if !constant_default(&expr) {
                    return Err(SqlError::new(
                        102,
                        15,
                        1,
                        format!(
                            "The default for parameter '@{}' must be a constant.",
                            p.name
                        ),
                    )
                    .at(p.span));
                }
            }
            Ok(ProcParamDef {
                name: p.name.clone(),
                type_spec: column_type.name(),
                default: p.default_text.clone(),
                output: p.output,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let procedure = ProcedureDef {
        params,
        body: create.body.clone(),
    };
    if create.alter {
        match resolve_table(storage, db_id, &create.name.value) {
            Some(def) if def.is_procedure() => {
                storage
                    .rel_alter_procedure(def.database_id, &def.name, procedure)
                    .map_err(|e| map_storage_err(e, &create.name.value))?;
                return Ok(StatementResult::Done);
            }
            _ => {
                return Err(SqlError::invalid_object(bare).at(create.name.span));
            }
        }
    }
    if resolve_table(storage, db_id, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    storage
        .rel_create_procedure(db_id, bare, procedure)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

fn exec_drop_procedure(
    storage: &Storage,
    db_id: u32,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, db_id, &name.value) {
        Some(def) if def.is_procedure() => {
            storage
                .rel_drop_table(def.database_id, &def.name)
                .map_err(|e| map_storage_err(e, &def.name))?;
            Ok(StatementResult::Done)
        }
        Some(_) | None if if_exists => Ok(StatementResult::Done),
        _ => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the procedure '{}', because it does not exist or you do not have \
                 permission.",
                name.value
            ),
        )),
    }
}

fn exec_create_function(
    storage: &Storage,
    db_id: u32,
    create: &CreateFunction,
) -> Result<StatementResult, SqlError> {
    let bare = create_object_name("CREATE FUNCTION", &create.name)?;
    let params = create
        .params
        .iter()
        .map(|p| -> Result<ProcParamDef, SqlError> {
            let column_type = data_type_to_column_type(&p.data_type, &p.name)?;
            if let Some(text) = &p.default_text {
                let expr = truthdb_sql::parse_expr(text)?;
                if !constant_default(&expr) {
                    return Err(SqlError::new(
                        102,
                        15,
                        1,
                        format!(
                            "The default for parameter '@{}' must be a constant.",
                            p.name
                        ),
                    )
                    .at(p.span));
                }
            }
            Ok(ProcParamDef {
                name: p.name.clone(),
                type_spec: column_type.name(),
                default: p.default_text.clone(),
                output: false,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let returns = match &create.returns {
        ReturnsClause::Scalar(return_type) => {
            let return_type = data_type_to_column_type(return_type, bare)?;
            // Validate the body: side-effect-free, ending in RETURN <expr> (SQL
            // Server's function-body rules). Re-parse under the function grammar.
            let body = truthdb_sql::parse_function_body(&create.body)?;
            validate_scalar_function_body(&body)?;
            FunctionReturns::Scalar {
                type_spec: return_type.name(),
                body: create.body.clone(),
            }
        }
        ReturnsClause::InlineTable => {
            // The body is a single SELECT expanded like a parameterized view —
            // validate it parses (no side-effect body check: it is a query).
            parse_view_query(&create.body, bare)?;
            FunctionReturns::InlineTable {
                select_text: create.body.clone(),
            }
        }
        ReturnsClause::MultiTable {
            var_name,
            columns_text,
        } => {
            // Validate the RETURNS table declaration builds (mirrors DECLARE @t
            // TABLE) and the body parses under the multi-statement TVF rules (may
            // populate the result / local table variables but not touch real
            // tables; must end in RETURN). Both are stored as text, re-parsed and
            // re-built per call.
            let (columns, primary_key) = truthdb_sql::parse_table_var_columns(columns_text)?;
            build_table_var_definition(var_name, &columns, &primary_key)?;
            let body = truthdb_sql::parse_table_function_body(&create.body)?;
            validate_multi_tvf_body(&body)?;
            FunctionReturns::MultiStatementTable {
                returns_var: var_name.clone(),
                columns_text: columns_text.clone(),
                body: create.body.clone(),
            }
        }
    };
    let function = FunctionDef { params, returns };
    if create.alter {
        match resolve_table(storage, db_id, &create.name.value) {
            Some(def) if def.is_function() => {
                storage
                    .rel_alter_function(def.database_id, &def.name, function)
                    .map_err(|e| map_storage_err(e, &create.name.value))?;
                return Ok(StatementResult::Done);
            }
            _ => {
                return Err(SqlError::invalid_object(bare).at(create.name.span));
            }
        }
    }
    if resolve_table(storage, db_id, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    storage
        .rel_create_function(db_id, bare, function)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// Validates a scalar function's body against SQL Server's rules: every
/// statement must be side-effect-free (443 otherwise; a data-returning SELECT is
/// 444), and the last statement must be a `RETURN <expr>` (455).
fn validate_scalar_function_body(statements: &[Statement]) -> Result<(), SqlError> {
    for statement in statements {
        check_function_statement(statement)?;
    }
    match last_effective_statement(statements) {
        Some(Statement::Return { value: Some(_), .. }) => Ok(()),
        _ => Err(SqlError::new(
            455,
            16,
            2,
            "The last statement included within a function must be a return statement.",
        )),
    }
}

/// The body's terminal statement, unwrapping a trailing `BEGIN...END` block —
/// SQL Server's 455 check looks at the last statement of the body block.
fn last_effective_statement(statements: &[Statement]) -> Option<&Statement> {
    match statements.last() {
        Some(Statement::Block { body, .. }) => last_effective_statement(body),
        other => other,
    }
}

/// Rejects a statement a function body may not contain. Side-effecting
/// statements (DML, DDL, EXEC, transaction control, THROW/RAISERROR) are 443; a
/// data-returning SELECT is 444; control flow recurses.
fn check_function_statement(statement: &Statement) -> Result<(), SqlError> {
    match statement {
        Statement::Declare(_)
        | Statement::Set(_)
        | Statement::Return { .. }
        | Statement::Break { .. }
        | Statement::Continue { .. } => Ok(()),
        Statement::Block { body, .. } => {
            for inner in body {
                check_function_statement(inner)?;
            }
            Ok(())
        }
        Statement::If {
            then_branch,
            else_branch,
            ..
        } => {
            check_function_statement(then_branch)?;
            if let Some(else_branch) = else_branch {
                check_function_statement(else_branch)?;
            }
            Ok(())
        }
        Statement::While { body, .. } => check_function_statement(body),
        // An assignment SELECT (`SELECT @x = …`) is allowed — it returns no
        // rows. A SELECT that produces a result set cannot (444).
        Statement::Select(select)
            if select
                .items
                .iter()
                .all(|i| matches!(i, SelectItem::Assign { .. })) =>
        {
            Ok(())
        }
        Statement::Select(_) => Err(SqlError::new(
            444,
            16,
            2,
            "Select statements included within a function cannot return data to a client.",
        )),
        _ => Err(SqlError::new(
            443,
            16,
            1,
            "Invalid use of a side-effecting operator within a function.",
        )),
    }
}

/// Validates a multi-statement TVF body: like a scalar function it is
/// side-effect-free against the database, but it MAY populate table variables
/// (its result and any locals it declares), and its last statement must be a
/// (valueless) RETURN.
fn validate_multi_tvf_body(statements: &[Statement]) -> Result<(), SqlError> {
    for statement in statements {
        check_multi_tvf_statement(statement)?;
    }
    match last_effective_statement(statements) {
        Some(Statement::Return { .. }) => Ok(()),
        _ => Err(SqlError::new(
            455,
            16,
            2,
            "The last statement included within a function must be a return statement.",
        )),
    }
}

/// Rejects a statement a multi-statement TVF body may not contain. The only
/// difference from a scalar body (`check_function_statement`) is that DML into a
/// table variable (an `@`-target) is allowed — that is how the result is built.
fn check_multi_tvf_statement(statement: &Statement) -> Result<(), SqlError> {
    match statement {
        // INSERT into a table variable (the result or a local) is how a
        // multi-statement TVF produces rows.
        Statement::Insert(insert) if insert.table.value.starts_with('@') => Ok(()),
        Statement::DeclareTableVar { .. } => Ok(()),
        Statement::Block { body, .. } => {
            for inner in body {
                check_multi_tvf_statement(inner)?;
            }
            Ok(())
        }
        Statement::If {
            then_branch,
            else_branch,
            ..
        } => {
            check_multi_tvf_statement(then_branch)?;
            if let Some(else_branch) = else_branch {
                check_multi_tvf_statement(else_branch)?;
            }
            Ok(())
        }
        Statement::While { body, .. } => check_multi_tvf_statement(body),
        // Everything else defers to the scalar-body rules (DECLARE/SET/RETURN/
        // assignment-SELECT allowed; real-table DML/EXEC/DDL 443; data SELECT
        // 444).
        other => check_function_statement(other),
    }
}

fn exec_drop_function(
    storage: &Storage,
    db_id: u32,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, db_id, &name.value) {
        Some(def) if def.is_function() => {
            storage
                .rel_drop_table(def.database_id, &def.name)
                .map_err(|e| map_storage_err(e, &def.name))?;
            Ok(StatementResult::Done)
        }
        Some(_) | None if if_exists => Ok(StatementResult::Done),
        _ => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the function '{}', because it does not exist or you do not have \
                 permission.",
                name.value
            ),
        )),
    }
}

/// `CREATE|ALTER TRIGGER <name> ON <table> AFTER <events> AS <body>`: registers
/// an AFTER DML trigger as a catalog object attached to its target table.
fn exec_create_trigger(
    storage: &Storage,
    db_id: u32,
    create: &CreateTrigger,
) -> Result<StatementResult, SqlError> {
    let bare = create_object_name("CREATE TRIGGER", &create.name)?;
    // The target must be an existing base table (not a view/procedure/function/
    // trigger). SQL Server 4929-class.
    let target = resolve_table(storage, db_id, &create.target.value)
        .ok_or_else(|| SqlError::invalid_object(&create.target.value).at(create.target.span))?;
    if target.is_view() || target.is_procedure() || target.is_function() || target.is_trigger() {
        return Err(SqlError::new(
            4929,
            16,
            1,
            format!(
                "Cannot create trigger '{bare}' because its target '{}' is not a base table.",
                target.name
            ),
        )
        .at(create.target.span));
    }
    // Validate the body parses under the in-procedure grammar (re-parsed per
    // firing). inserted/deleted resolve at firing time, not here.
    truthdb_sql::parse_procedure_body(&create.body)?;
    let events: Vec<catalog::TriggerEvent> = create
        .events
        .iter()
        .map(|e| match e {
            ast::TriggerEvent::Insert => catalog::TriggerEvent::Insert,
            ast::TriggerEvent::Update => catalog::TriggerEvent::Update,
            ast::TriggerEvent::Delete => catalog::TriggerEvent::Delete,
        })
        .collect();
    // A table may have at most one INSTEAD OF trigger per action (SQL Server).
    if create.instead_of {
        for def in storage.rel_tables() {
            if let Some(t) = &def.trigger
                && t.is_instead_of
                && t.parent_object_id == target.object_id
                && !def.name.eq_ignore_ascii_case(bare)
                && t.events.iter().any(|e| events.contains(e))
            {
                return Err(SqlError::new(
                    2113,
                    16,
                    1,
                    format!(
                        "Cannot create INSTEAD OF trigger '{bare}' on table '{}' because there is \
                         already an INSTEAD OF trigger '{}' for the same action.",
                        target.name, def.name
                    ),
                )
                .at(create.name.span));
            }
        }
    }
    let trigger = TriggerDef {
        parent_object_id: target.object_id,
        events,
        body: create.body.clone(),
        is_disabled: false,
        is_instead_of: create.instead_of,
    };
    if create.alter {
        match resolve_table(storage, db_id, &create.name.value) {
            Some(def) if def.is_trigger() => {
                storage
                    .rel_alter_trigger(def.database_id, &def.name, trigger)
                    .map_err(|e| map_storage_err(e, &create.name.value))?;
                return Ok(StatementResult::Done);
            }
            _ => {
                return Err(SqlError::invalid_object(bare).at(create.name.span));
            }
        }
    }
    if resolve_table(storage, db_id, &create.name.value).is_some() {
        return Err(SqlError::new(
            2714,
            16,
            6,
            format!("There is already an object named '{bare}' in the database."),
        ));
    }
    storage
        .rel_create_trigger(db_id, bare, trigger)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

fn exec_drop_trigger(
    storage: &Storage,
    db_id: u32,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, db_id, &name.value) {
        Some(def) if def.is_trigger() => {
            storage
                .rel_drop_table(def.database_id, &def.name)
                .map_err(|e| map_storage_err(e, &def.name))?;
            Ok(StatementResult::Done)
        }
        Some(_) | None if if_exists => Ok(StatementResult::Done),
        _ => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the trigger '{}', because it does not exist or you do not have \
                 permission.",
                name.value
            ),
        )),
    }
}

/// `{ENABLE | DISABLE} TRIGGER {<name> | ALL} ON <table>`: flips the disabled
/// flag on one trigger (or every trigger on the table). A disabled trigger stays
/// in the catalog but does not fire.
fn exec_set_trigger_state(
    storage: &Storage,
    db_id: u32,
    trigger: &Option<Name>,
    table: &Name,
    enable: bool,
) -> Result<StatementResult, SqlError> {
    let target = resolve_table(storage, db_id, &table.value)
        .ok_or_else(|| SqlError::invalid_object(&table.value).at(table.span))?;
    if target.is_view() || target.is_procedure() || target.is_function() || target.is_trigger() {
        return Err(SqlError::invalid_object(&table.value).at(table.span));
    }
    let set_one = |def: &TableDef| -> Result<(), SqlError> {
        let mut td = def.trigger.clone().expect("is_trigger");
        td.is_disabled = !enable;
        storage
            .rel_alter_trigger(def.database_id, &def.name, td)
            .map_err(|e| map_storage_err(e, &def.name))
    };
    match trigger {
        Some(name) => {
            let def = resolve_table(storage, db_id, &name.value)
                .filter(|d| d.is_trigger())
                .filter(|d| {
                    d.trigger.as_ref().map(|t| t.parent_object_id) == Some(target.object_id)
                })
                .ok_or_else(|| {
                    SqlError::new(
                        3701,
                        11,
                        5,
                        format!(
                            "Cannot {} the trigger '{}', because it does not exist on table \
                             '{}' or you do not have permission.",
                            if enable { "enable" } else { "disable" },
                            name.value,
                            table.value
                        ),
                    )
                    .at(name.span)
                })?;
            set_one(&def)?;
        }
        None => {
            for def in storage.rel_tables() {
                if def.is_trigger()
                    && def.trigger.as_ref().map(|t| t.parent_object_id) == Some(target.object_id)
                {
                    set_one(&def)?;
                }
            }
        }
    }
    Ok(StatementResult::Done)
}

/// `CREATE|ALTER LOGIN <name> WITH PASSWORD = '<pw>'` / `ALTER LOGIN <name>
/// {ENABLE | DISABLE}`. Logins are server principals in their own namespace
/// (disjoint from schema objects); the password is hashed here (on the worker —
/// CREATE/ALTER LOGIN is rare admin DDL, unlike verification which runs off the
/// worker per connection).
fn exec_create_login(storage: &Storage, create: &CreateLogin) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    if create.alter {
        let Some(existing) = storage.rel_login(bare) else {
            return Err(SqlError::new(
                15151,
                16,
                1,
                format!(
                    "Cannot alter the login '{bare}', because it does not exist or you do not have permission."
                ),
            )
            .at(create.name.span));
        };
        let mut principal = existing
            .principal
            .clone()
            .expect("rel_login returns a login");
        if let Some(password) = &create.password {
            principal.password_blob = crate::auth::hash_password(password);
        }
        if let Some(disable) = create.disable {
            principal.is_disabled = disable;
        }
        storage
            .rel_alter_login(bare, principal)
            .map_err(|e| map_storage_err(e, &create.name.value))?;
        return Ok(StatementResult::Done);
    }
    if storage.rel_login(bare).is_some() {
        return Err(SqlError::new(
            15025,
            16,
            1,
            format!("The server principal '{bare}' already exists."),
        )
        .at(create.name.span));
    }
    let password = create
        .password
        .as_ref()
        .expect("CREATE LOGIN carries a password (parser-enforced)");
    let principal = PrincipalDef::login(
        crate::auth::hash_password(password),
        create.disable.unwrap_or(false),
    );
    storage
        .rel_create_login(bare, principal)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

fn exec_drop_login(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    let dropped = storage
        .rel_drop_login(bare)
        .map_err(|e| map_storage_err(e, &name.value))?;
    if !dropped && !if_exists {
        return Err(SqlError::new(
            15151,
            16,
            1,
            format!(
                "Cannot drop the login '{bare}', because it does not exist or you do not have permission."
            ),
        )
        .at(name.span));
    }
    Ok(StatementResult::Done)
}

/// `CREATE USER <name> [FOR LOGIN <login>]`. A database principal in its own
/// namespace (out of the object namespace), optionally mapped to a login.
fn exec_create_user(storage: &Storage, create: &CreateUser) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    if storage.rel_database_principal(bare).is_some()
        || crate::storage::fixed_principal_by_name(bare).is_some()
    {
        return Err(SqlError::new(
            15023,
            16,
            1,
            format!("User, group, or role '{bare}' already exists in the current database."),
        )
        .at(create.name.span));
    }
    let login_sid = match &create.for_login {
        Some(login) => {
            let login_bare = strip_schema(&login.value);
            let Some(def) = storage.rel_login(login_bare) else {
                return Err(SqlError::new(
                    15007,
                    16,
                    1,
                    format!("'{login_bare}' is not a valid login or you do not have permission."),
                )
                .at(login.span));
            };
            Some(def.object_id)
        }
        None => None,
    };
    storage
        .rel_create_database_principal(bare, PrincipalDef::user(login_sid))
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// `CREATE ROLE <name>`.
fn exec_create_role(storage: &Storage, name: &Name) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    if storage.rel_database_principal(bare).is_some()
        || crate::storage::fixed_principal_by_name(bare).is_some()
    {
        return Err(SqlError::new(
            15023,
            16,
            1,
            format!("User, group, or role '{bare}' already exists in the current database."),
        )
        .at(name.span));
    }
    storage
        .rel_create_database_principal(bare, PrincipalDef::role())
        .map_err(|e| map_storage_err(e, &name.value))?;
    Ok(StatementResult::Done)
}

/// `DROP USER`/`DROP ROLE`. `expect_role` selects which kind is being dropped;
/// a mismatch (DROP USER on a role, or vice versa) reports not-found for the
/// requested kind, as SQL Server does.
fn exec_drop_database_principal(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
    expect_role: bool,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    let kind = if expect_role { "role" } else { "user" };
    match storage.rel_database_principal(bare) {
        Some(def) if def.is_role() == expect_role => {}
        _ if if_exists => return Ok(StatementResult::Done),
        _ => {
            return Err(SqlError::new(
                15151,
                16,
                1,
                format!(
                    "Cannot drop the {kind} '{bare}', because it does not exist or you do not have permission."
                ),
            )
            .at(name.span));
        }
    }
    storage
        .rel_drop_database_principal(bare)
        .map_err(|e| map_storage_err(e, &name.value))?;
    Ok(StatementResult::Done)
}

/// `ALTER ROLE <role> ADD|DROP MEMBER <member>`.
fn exec_alter_role_member(
    storage: &Storage,
    role: &Name,
    action: RoleMemberAction,
    member: &Name,
) -> Result<StatementResult, SqlError> {
    let role_bare = strip_schema(&role.value);
    let member_bare = strip_schema(&member.value);
    match action {
        RoleMemberAction::Add => storage.rel_add_role_member(role_bare, member_bare),
        RoleMemberAction::Drop => storage.rel_drop_role_member(role_bare, member_bare),
    }
    .map_err(|e| map_storage_err(e, &role.value))?;
    Ok(StatementResult::Done)
}

/// Maps a parsed permission action to its catalog form.
fn map_perm_action(action: PermissionAction) -> PermAction {
    match action {
        PermissionAction::Select => PermAction::Select,
        PermissionAction::Insert => PermAction::Insert,
        PermissionAction::Update => PermAction::Update,
        PermissionAction::Delete => PermAction::Delete,
        PermissionAction::Execute => PermAction::Execute,
        PermissionAction::References => PermAction::References,
        PermissionAction::Alter => PermAction::Alter,
    }
}

/// `GRANT|DENY|REVOKE <actions> ON <object> TO|FROM <grantees>`. The authority to
/// manage permissions is enforced by the DDL privilege gate in the dispatcher
/// (a bypassing principal — sysadmin / dbo / db_owner / internal). Here we just
/// resolve the securable and apply each (grantee, action).
fn exec_permission(
    storage: &Storage,
    db_id: u32,
    stmt: &PermissionStatement,
    _sec: &SecurityContext,
) -> Result<StatementResult, SqlError> {
    // The securable must be a schema object (table, view, procedure, function).
    let Some(def) = resolve_table(storage, db_id, &stmt.object.value) else {
        return Err(SqlError::invalid_object(&stmt.object.value).at(stmt.object.span));
    };
    if def.is_trigger() {
        return Err(SqlError::invalid_object(&stmt.object.value).at(stmt.object.span));
    }
    let object = def.name.clone(); // the canonical name = the rel.tables key
    for grantee in &stmt.grantees {
        let grantee_bare = strip_schema(&grantee.value);
        for action in &stmt.actions {
            let catalog_action = map_perm_action(*action);
            match stmt.kind {
                PermissionKind::Grant => storage.rel_grant_object(
                    def.database_id,
                    &object,
                    grantee_bare,
                    catalog_action,
                    false,
                ),
                PermissionKind::Deny => storage.rel_grant_object(
                    def.database_id,
                    &object,
                    grantee_bare,
                    catalog_action,
                    true,
                ),
                PermissionKind::Revoke => storage.rel_revoke_object(
                    def.database_id,
                    &object,
                    grantee_bare,
                    catalog_action,
                ),
            }
            .map_err(|e| map_storage_err(e, &grantee.value).at(grantee.span))?;
        }
    }
    Ok(StatementResult::Done)
}

/// Schema and security DDL a non-privileged principal may not run. (GRANT/DENY/
/// REVOKE — `Permission` — is included: only a privileged principal manages
/// permissions.) Fine-grained database-scoped CREATE grants and the db_ddladmin
/// role are deferred: today any DDL requires bypass privilege.
fn is_privileged_ddl(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::AlterDatabase(_)
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
            | Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_)
            | Statement::SetTriggerState { .. }
            | Statement::BackupDatabase { .. }
            | Statement::BackupLog { .. }
            | Statement::Restore { .. }
    )
}

/// Resolves the AFTER triggers to fire for a DML on `target_name` for `event`,
/// plus the target's definition (for the pseudo-table schema). Empty when no
/// trigger exists anywhere (the cheap `rel_has_triggers` gate keeps the common
/// path free) or the target is not a base table.
/// The triggers on `target_name` for `event`, split into AFTER triggers (fired
/// after the DML) and the at-most-one INSTEAD OF trigger (fired in place of it).
fn triggers_for(
    storage: &Storage,
    db_id: u32,
    target_name: &str,
    event: catalog::TriggerEvent,
) -> (Option<TableDef>, Vec<TableDef>, Option<TableDef>) {
    if !storage.rel_has_triggers() {
        return (None, Vec::new(), None);
    }
    match resolve_table(storage, db_id, target_name) {
        Some(def)
            if def.trigger.is_none()
                && def.procedure.is_none()
                && def.function.is_none()
                && def.view_query.is_none() =>
        {
            let (instead, after): (Vec<TableDef>, Vec<TableDef>) = storage
                .rel_triggers_for(def.object_id, event)
                .into_iter()
                .partition(|t| t.trigger.as_ref().is_some_and(|d| d.is_instead_of));
            (Some(def), after, instead.into_iter().next())
        }
        _ => (None, Vec::new(), None),
    }
}

/// Runs a DML statement (via `dml`) and fires its AFTER triggers atomically.
/// Under autocommit an implicit transaction is opened so the DML stages rather
/// than commits, so DML + triggers share one transaction (a trigger ROLLBACK
/// undoes the DML) and a trigger that ends the transaction raises 3609.
fn run_dml_with_triggers(
    storage: &Storage,
    txn_ctx: &mut TxnContext,
    target_def: &TableDef,
    triggers: Vec<TableDef>,
    dml: impl FnOnce(&mut TxnContext) -> Result<StatementResult, SqlError>,
) -> Result<StatementResult, SqlError> {
    let schema = target_def
        .schema()
        .map_err(|e| map_storage_err(e, &target_def.name))?;
    let implicit = !txn_ctx.in_txn();
    if implicit {
        exec_begin(storage, txn_ctx)?;
    }
    let tc_before = txn_ctx.trancount;
    // Arm the row-image capture, run the DML (staged on the transaction), then
    // take the captured images for the trigger bodies.
    TRIGGER_CAPTURE.with(|c| *c.borrow_mut() = Some(CapturedImages::default()));
    let dml_result = dml(txn_ctx);
    let images = TRIGGER_CAPTURE
        .with(|c| c.borrow_mut().take())
        .unwrap_or_default();
    let result = match dml_result {
        Ok(r) => r,
        Err(e) => {
            if implicit {
                txn_ctx.abort(storage);
            }
            return Err(e);
        }
    };
    let tables = std::rc::Rc::new(TriggerTables {
        schema,
        inserted: images.inserted,
        deleted: images.deleted,
        updated: images.updated,
    });
    // Fire each trigger once, in creation order, even for an empty image set.
    for trig_def in &triggers {
        let fired = fire_one_trigger(storage, txn_ctx, trig_def, &tables);
        // A trigger body that changed @@TRANCOUNT — a ROLLBACK/COMMIT that
        // reduced it or an unbalanced BEGIN that raised it — ENDED the
        // transaction (3609). This is checked BEFORE the error branch so the
        // idiomatic `ROLLBACK; RAISERROR` abort pattern does not doom a
        // transaction the trigger already tore down (which would wedge the
        // session doomed with no open transaction). `abort` normalizes the
        // state; surface the trigger's own error if it raised one, else 3609.
        if txn_ctx.trancount != tc_before {
            txn_ctx.abort(storage);
            return Err(fired.err().unwrap_or_else(|| {
                SqlError::new(
                    3609,
                    16,
                    1,
                    "The transaction ended in the trigger. The batch has been aborted.",
                )
            }));
        }
        // A trigger error with the transaction still open makes it
        // uncommittable. Roll back the IMPLICIT (autocommit) transaction opened
        // here; DOOM the caller's EXPLICIT one (leave it open, @@TRANCOUNT
        // intact, XACT_STATE() = -1) — SQL Server's uncommittable-transaction
        // semantics, so a TRY/CATCH sees the doomed state and must ROLLBACK
        // (its writes hit the 3930 guard), and an uncaught error terminates the
        // batch (statement_error_ladder does not continue past a doomed txn).
        // The doomed transaction's staged rows can never commit.
        if let Err(e) = fired {
            if implicit {
                txn_ctx.abort(storage);
            } else {
                txn_ctx.doomed = true;
            }
            return Err(e);
        }
    }
    if implicit {
        exec_commit(storage, txn_ctx)?;
    }
    Ok(result)
}

/// The `(inserted, deleted)` row images an INSTEAD OF trigger's body sees.
type TriggerImages = (Vec<Vec<Datum>>, Vec<Vec<Datum>>);

/// Fires an INSTEAD OF trigger in place of the DML: it runs the trigger body over
/// the *proposed* `inserted`/`deleted` images (the base operation and its
/// constraints are bypassed — the body decides what actually happens). Reuses the
/// DML+trigger transaction/firing/error machinery with a DML step that only
/// computes and captures the images, writing nothing.
fn run_instead_of(
    storage: &Storage,
    txn_ctx: &mut TxnContext,
    target: &TableDef,
    trigger: TableDef,
    images: impl FnOnce(&EvalContext) -> Result<TriggerImages, SqlError>,
) -> Result<StatementResult, SqlError> {
    run_dml_with_triggers(storage, txn_ctx, target, vec![trigger], |txn_ctx| {
        let eval_ctx = txn_ctx.eval_context();
        let (inserted, deleted) = images(&eval_ctx)?;
        let count = inserted.len().max(deleted.len()) as u64;
        capture_trigger_images(|| (inserted, deleted));
        Ok(StatementResult::RowsAffected(count))
    })
}

/// The `inserted` image an INSTEAD OF INSERT trigger sees: the proposed rows with
/// DEFAULTs applied and the identity column left NULL (the body's own insert
/// generates it). Constraints are not enforced here.
fn instead_of_insert_images(
    storage: &Storage,
    insert: &Insert,
    def: &TableDef,
    eval_ctx: &EvalContext,
) -> Result<TriggerImages, SqlError> {
    enforce_object_permission(storage, def, &eval_ctx.security, PermAction::Insert)
        .map_err(|e| e.at(insert.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let ncols = schema.columns.len();
    let identity_col = def.identity.map(|s| s.column);
    let target: Vec<usize> = match &insert.columns {
        Some(names) => {
            let mut indices = Vec::with_capacity(names.len());
            for n in names {
                indices.push(
                    column_index(&schema, &n.value)
                        .ok_or_else(|| SqlError::invalid_column(&n.value).at(n.span))?,
                );
            }
            indices
        }
        None => (0..ncols).filter(|i| Some(*i) != identity_col).collect(),
    };
    let input_rows = insert_input_rows(storage, &insert.source, target.len(), eval_ctx)?;
    let mut inserted = Vec::with_capacity(input_rows.len());
    for input in &input_rows {
        let mut values = vec![Datum::Null; ncols];
        for (position, sql_value) in target.iter().zip(input) {
            let column = &schema.columns[*position];
            values[*position] = value::sql_to_datum(sql_value, &column.column_type, &column.name)?;
        }
        for (index, column) in schema.columns.iter().enumerate() {
            if !values[index].is_null() || target.contains(&index) || Some(index) == identity_col {
                continue;
            }
            if let Some(text) = def.default_for(index) {
                let sql_value = eval_default(text, eval_ctx)?;
                values[index] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
            }
        }
        inserted.push(values);
    }
    capture_trigger_updated((0..ncols).collect());
    Ok((inserted, Vec::new()))
}

/// The (`inserted` = post-update, `deleted` = pre-update) images an INSTEAD OF
/// UPDATE trigger sees for the rows matching the WHERE clause. Constraints are
/// not enforced here.
fn instead_of_update_images(
    storage: &Storage,
    update: &Update,
    def: &TableDef,
    eval_ctx: &EvalContext,
) -> Result<TriggerImages, SqlError> {
    enforce_object_permission(storage, def, &eval_ctx.security, PermAction::Update)
        .map_err(|e| e.at(update.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = SchemaScope { schema: &schema };
    let types = schema_types(&schema);
    let mut assignments: Vec<(usize, &Expr)> = Vec::with_capacity(update.assignments.len());
    for a in &update.assignments {
        let index = column_index(&schema, &a.column.value)
            .ok_or_else(|| SqlError::invalid_column(&a.column.value).at(a.column.span))?;
        assignments.push((index, &a.value));
    }
    let mut old_rows = Vec::new();
    let mut new_rows = Vec::new();
    for row in storage
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?
    {
        check_cancelled()?;
        if !predicate_true(&update.where_clause, &row, &types, &resolver, eval_ctx)? {
            continue;
        }
        let old_scope = row_values(&row, &types);
        let mut new_row = row.clone();
        for (index, expr) in &assignments {
            let column = &schema.columns[*index];
            let value = eval::eval(expr, &old_scope, &resolver, eval_ctx)?;
            new_row[*index] = value::sql_to_datum(&value, &column.column_type, &column.name)?;
        }
        old_rows.push(row);
        new_rows.push(new_row);
    }
    capture_trigger_updated(assignments.iter().map(|(i, _)| *i).collect());
    Ok((new_rows, old_rows))
}

/// The `deleted` image an INSTEAD OF DELETE trigger sees: the rows matching the
/// WHERE clause (none are actually removed).
fn instead_of_delete_images(
    storage: &Storage,
    delete: &Delete,
    def: &TableDef,
    eval_ctx: &EvalContext,
) -> Result<TriggerImages, SqlError> {
    enforce_object_permission(storage, def, &eval_ctx.security, PermAction::Delete)
        .map_err(|e| e.at(delete.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = SchemaScope { schema: &schema };
    let types = schema_types(&schema);
    let mut deleted = Vec::new();
    for row in storage
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?
    {
        check_cancelled()?;
        if predicate_true(&delete.where_clause, &row, &types, &resolver, eval_ctx)? {
            deleted.push(row);
        }
    }
    Ok((Vec::new(), deleted))
}

/// Fires one trigger body: parses it, runs it in the firing statement's
/// transaction (procedure posture — shared txn, fresh variable scope) with the
/// `inserted`/`deleted` view armed, bounded by the nesting cap. Direct
/// self-recursion is suppressed (recursive triggers OFF).
fn fire_one_trigger(
    storage: &Storage,
    txn_ctx: &mut TxnContext,
    trig_def: &TableDef,
    tables: &std::rc::Rc<TriggerTables>,
) -> Result<(), SqlError> {
    let trigger = trig_def.trigger.as_ref().expect("caller passes a trigger");
    // Recursive triggers OFF (the default) suppresses only DIRECT recursion: a
    // trigger whose own body re-fires itself (it is the currently-executing
    // trigger — top of the firing stack). Indirect recursion (a fires b fires a,
    // where a is deeper in the stack, not the top) stays enabled and is bounded
    // by the nesting cap, matching "nested triggers ON".
    if FIRING_TRIGGERS.with(|f| f.borrow().last() == Some(&trig_def.object_id)) {
        return Ok(());
    }
    let statements = truthdb_sql::parse_procedure_body(&trigger.body)?;
    // A trigger body ownership-chains: its object reads are not re-checked.
    let _chain = ChainGuard::enter();
    let depth = EXEC_DEPTH.with(|d| {
        let v = d.get() + 1;
        d.set(v);
        v
    });
    if depth > 32 {
        EXEC_DEPTH.with(|d| d.set(d.get() - 1));
        return Err(SqlError::new(
            217,
            16,
            1,
            "Maximum stored procedure, function, trigger, or view nesting level exceeded (limit 32).",
        ));
    }
    // Procedure posture: fresh variable/table-variable scope, shared transaction.
    let outer_vars = std::mem::take(&mut txn_ctx.variables);
    let outer_table_vars = std::mem::take(&mut txn_ctx.table_variables);
    FIRING_TRIGGERS.with(|f| f.borrow_mut().push(trig_def.object_id));
    let result = {
        let _trigger_scope = TriggerScope::enter(std::rc::Rc::clone(tables));
        let mut emitter = DiscardEmitter;
        let mut run = BatchRun {
            emitter: &mut emitter,
            deferred: Vec::new(),
            rowset_open: false,
            durability_failed: false,
            committed: false,
            last_error: None,
            function_return_type: None,
        };
        let flow = run_block(storage, &statements, txn_ctx, &mut run, false);
        // An error raised in the trigger body — a terminating one (Err), or a
        // non-terminating RAISERROR/THROW/failed-statement (severity >= 11) that
        // run_block records in last_error and NOT caught by an inner TRY/CATCH —
        // aborts the firing statement: SQL Server rolls back the DML and returns
        // the error. (A successful CATCH clears last_error, so a trigger that
        // handles its own error still succeeds.)
        flow.and_then(end_of_scope)
            .and_then(|()| match run.last_error.take() {
                Some(err) => Err(err),
                None => Ok(()),
            })
    };
    FIRING_TRIGGERS.with(|f| {
        f.borrow_mut().pop();
    });
    EXEC_DEPTH.with(|d| d.set(d.get() - 1));
    txn_ctx.variables = outer_vars;
    txn_ctx.table_variables = outer_table_vars;
    result
}

fn exec_drop_view(
    storage: &Storage,
    db_id: u32,
    drop: &DropView,
) -> Result<StatementResult, SqlError> {
    match resolve_table(storage, db_id, &drop.name.value) {
        Some(def) if def.is_view() => {
            storage
                .rel_drop_table(def.database_id, &def.name)
                .map_err(|e| map_storage_err(e, &def.name))?;
            Ok(StatementResult::Done)
        }
        // The object exists but is a base table, not a view.
        Some(_) => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the view '{}', because it does not exist or you do not have permission.",
                drop.name.value
            ),
        )),
        None if drop.if_exists => Ok(StatementResult::Done),
        None => Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the view '{}', because it does not exist or you do not have permission.",
                drop.name.value
            ),
        )),
    }
}

// ---- CREATE / DROP INDEX ------------------------------------------------

/// SQL Server 1919: a (MAX)-class column cannot be an index/key column.
fn max_key_column_error(column: &str, table: &str) -> SqlError {
    SqlError::new(
        1919,
        16,
        1,
        format!(
            "Column '{column}' in table '{table}' is of a type that is invalid for use as a \
             key column in an index."
        ),
    )
}

fn exec_create_index(
    storage: &Storage,
    db_id: u32,
    create: &CreateIndex,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, db_id, &create.table.value)
        .ok_or_else(|| SqlError::invalid_object(&create.table.value).at(create.table.span))?;
    reject_view_as_table(&def)?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let mut columns = Vec::with_capacity(create.columns.len());
    for col in &create.columns {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col.name.value))
            .ok_or_else(|| index_column_missing(&col.name.value, &def.name).at(col.name.span))?;
        if schema.columns[index].column_type.is_max() {
            return Err(max_key_column_error(&col.name.value, &def.name).at(col.name.span));
        }
        columns.push((index, col.ascending));
    }
    // INCLUDE columns: resolved against the schema, no duplicates (1909, as
    // SQL Server). A *key* column may be INCLUDEd — a deliberate divergence
    // from SQL Server, which rejects that: our index keys are one-way
    // collation sort keys, so a query reading the key column itself can only
    // be covered by also storing its original value.
    let mut include = Vec::with_capacity(create.include.len());
    for col in &create.include {
        let index = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col.value))
            .ok_or_else(|| index_column_missing(&col.value, &def.name).at(col.span))?;
        // (MAX) columns cannot be INCLUDEd either — a divergence from SQL
        // Server (whose row-overflow indexes can carry them): our include
        // payloads live in ordinary index leaf cells under the tree cell cap.
        if schema.columns[index].column_type.is_max() {
            return Err(max_key_column_error(&col.value, &def.name).at(col.span));
        }
        if include.contains(&index) {
            return Err(SqlError::new(
                1909,
                16,
                1,
                format!(
                    "Cannot use duplicate column names in index. Column name '{}' listed more than once.",
                    col.value
                ),
            )
            .at(col.span));
        }
        include.push(index);
    }
    storage
        .rel_create_index(
            def.database_id,
            &def.name,
            create.name.value.clone(),
            columns,
            create.unique,
            include,
        )
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// SQL Server's 1911 for a `CREATE INDEX` column (key or `INCLUDE`) that does
/// not exist on the target table — where most statements answer 207.
fn index_column_missing(column: &str, table: &str) -> SqlError {
    SqlError::new(
        1911,
        16,
        1,
        format!("Column name '{column}' does not exist in the target table or view '{table}'."),
    )
}

fn exec_drop_index(
    storage: &Storage,
    db_id: u32,
    drop: &DropIndex,
) -> Result<StatementResult, SqlError> {
    // Resolve the table so the index lookup is scoped to it (index names are
    // per-table; two tables may share an index name).
    let table = resolve_table(storage, db_id, &drop.table.value)
        .ok_or_else(|| SqlError::invalid_object(&drop.table.value).at(drop.table.span))?;
    let existed = storage
        .rel_drop_index(table.database_id, &table.name, &drop.name.value)
        .map_err(|e| map_storage_err(e, &drop.name.value))?;
    if !existed {
        return Err(SqlError::new(
            3701,
            11,
            5,
            format!(
                "Cannot drop the index '{}', because it does not exist or you do not have permission.",
                drop.name.value
            ),
        ));
    }
    Ok(StatementResult::Done)
}

// ---- ALTER TABLE --------------------------------------------------------

/// `ALTER DATABASE {name | CURRENT} SET READ_COMMITTED_SNAPSHOT /
/// ALLOW_SNAPSHOT_ISOLATION {ON|OFF}`. The batch's Database X lock has
/// quiesced the store: no snapshot is live, no writer is mid-transaction.
fn exec_alter_database(
    storage: &Storage,
    alter: &AlterDatabase,
    txn_ctx: &TxnContext,
) -> Result<StatementResult, SqlError> {
    if let Some(name) = &alter.name
        && storage.rel_database_id_by_name(&name.value).is_none()
    {
        return Err(SqlError::new(
            911,
            16,
            1,
            format!(
                "Database '{}' does not exist. Make sure that the name is entered correctly.",
                name.value
            ),
        )
        .at(name.span));
    }
    // FAILOVER (standby promotion) is offline-only, like RESTORE DATABASE: the
    // in-flight-transaction undo and the epoch bump run against a stopped
    // server. Checked before anything else — the pointer to the CLI is the
    // whole answer.
    if alter
        .options
        .iter()
        .any(|(option, _)| *option == DatabaseOption::Failover)
    {
        return Err(SqlError::new(
            3101,
            16,
            1,
            "Exclusive access could not be obtained because the database is in use. TruthDB \
             promotes a standby offline: stop the server and run `truthdb-cli promote`."
                .to_string(),
        ));
    }
    // A SNAPSHOT transaction idle between batches holds no locks, so the
    // batch's Database X does not prove no snapshot is live. Flipping the
    // options under one would reset (or stop publishing to) the store its
    // reads depend on; SQL Server waits the transactions out, TruthDB
    // refuses and lets the operator retry.
    if storage.has_registered_snapshots() {
        return Err(SqlError::new(
            5061,
            16,
            1,
            format!(
                "ALTER DATABASE failed because a lock could not be placed on database '{}'. \
                 Try again later.",
                txn_ctx.database
            ),
        ));
    }
    let mut rcsi = None;
    let mut allow_snapshot = None;
    let mut recovery_full = None;
    for (option, on) in &alter.options {
        match option {
            DatabaseOption::ReadCommittedSnapshot => rcsi = Some(*on),
            DatabaseOption::AllowSnapshotIsolation => allow_snapshot = Some(*on),
            // For Recovery the bool is the mode: true = FULL, false = SIMPLE.
            DatabaseOption::Recovery => recovery_full = Some(*on),
            // Returned as 3101 above, before this loop runs.
            DatabaseOption::Failover => unreachable!("failover is rejected before options apply"),
        }
    }
    storage
        .rel_set_db_options(rcsi, allow_snapshot, recovery_full)
        .map_err(|err| map_storage_err(err, &txn_ctx.database))?;
    Ok(StatementResult::Done)
}

fn exec_alter_table(
    storage: &Storage,
    db_id: u32,
    alter: &AlterTable,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, db_id, &alter.table.value)
        .ok_or_else(|| SqlError::invalid_object(&alter.table.value).at(alter.table.span))?;
    reject_view_as_table(&def)?;
    match &alter.action {
        AlterAction::AddColumn(column) => alter_add_column(storage, &def, column, eval_ctx),
        AlterAction::AddCheck(check) => alter_add_check(storage, &def, check, eval_ctx),
        AlterAction::AddForeignKey(fk) => alter_add_foreign_key(storage, &def, fk),
        AlterAction::DropConstraint(name) => alter_drop_constraint(storage, &def, name),
    }
}

/// `ALTER TABLE ... ADD [CONSTRAINT name] FOREIGN KEY (...) REFERENCES ...`.
/// Validates the constraint and every existing row (WITH CHECK): a child row
/// referencing a missing parent is 547 and the constraint is not added.
fn alter_add_foreign_key(
    storage: &Storage,
    def: &TableDef,
    fk: &ForeignKey,
) -> Result<StatementResult, SqlError> {
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let parent_bare = strip_schema(&fk.parent.value);
    let parent_pk: Vec<(String, ColumnType)> = if parent_bare.eq_ignore_ascii_case(&def.name) {
        def.key_columns
            .iter()
            .map(|&i| {
                (
                    schema.columns[i].name.clone(),
                    schema.columns[i].column_type,
                )
            })
            .collect()
    } else {
        let parent = resolve_table(storage, def.database_id, &fk.parent.value)
            .ok_or_else(|| SqlError::invalid_object(&fk.parent.value).at(fk.parent.span))?;
        let pschema = parent
            .schema()
            .map_err(|e| map_storage_err(e, &parent.name))?;
        parent
            .key_columns
            .iter()
            .map(|&i| {
                (
                    pschema.columns[i].name.clone(),
                    pschema.columns[i].column_type,
                )
            })
            .collect()
    };
    let existing_names: Vec<String> = def
        .check_constraints
        .iter()
        .map(|c| c.name.clone())
        .chain(def.foreign_keys.iter().map(|f| f.name.clone()))
        .collect();
    let new_def = bind_foreign_key(
        fk,
        &schema.columns,
        &def.name,
        &parent_pk,
        parent_bare,
        &existing_names,
    )?;

    // WITH CHECK: every existing child row must satisfy the new foreign key
    // (its sibling rows count for a self-reference).
    let rows = storage
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    for row in &rows {
        if let Some(key) = fk_key(&new_def, row)
            && !fk_parent_exists(storage, &new_def, &key, def, &rows)?
        {
            return Err(fk_child_violation(
                &database_name_of(storage, def.database_id),
                &new_def.name,
                "ALTER TABLE",
                &new_def.parent,
            ));
        }
    }

    let mut fks = def.foreign_keys.clone();
    fks.push(new_def);
    storage
        .rel_set_foreign_keys(def.database_id, &def.name, fks)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ADD <column>`: appends the column to the catalog and
/// rewrites every existing row under the new schema. The row codec is
/// positional (every offset derives from the schema, with no per-row version
/// stamp), so a metadata-only ADD cannot exist — the rewrite is the honest
/// implementation, one transactional statement under the ALTER's exclusive
/// lock. Existing rows take a FROZEN fill: NULL, or the DEFAULT evaluated
/// once now (SQL Server freezes it the same way); later INSERTs evaluate the
/// live default text per row like any other column.
fn alter_add_column(
    storage: &Storage,
    def: &catalog::TableDef,
    column: &ColumnDef,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    if def
        .columns
        .iter()
        .any(|(name, _, _)| name.eq_ignore_ascii_case(&column.name.value))
    {
        return Err(SqlError::new(
            2705,
            16,
            4,
            format!(
                "Column names in each table must be unique. Column name '{}' is specified more than once.",
                column.name.value
            ),
        )
        .at(column.name.span));
    }
    // The plan's scope: a plain column with nullability, DEFAULT and COLLATE.
    // Constraint-carrying additions are their own statements in T-SQL anyway.
    if column.primary_key
        || column.unique
        || column.identity.is_some()
        || !column.checks.is_empty()
        || !column.foreign_keys.is_empty()
    {
        return Err(SqlError::new(
            40510,
            16,
            1,
            "ALTER TABLE ADD supports a plain column (with NULL/NOT NULL, DEFAULT and COLLATE); add constraints with their own ALTER TABLE ADD CONSTRAINT statements.",
        )
        .at(column.span));
    }
    let bound = bind_column(column)?;
    // An authoritative emptiness probe (one-row scan under the ALTER's
    // exclusive lock) — the row counter is a statistic and must not become
    // load-bearing here: an under-count would let NULL fills into a NOT NULL
    // column, and a pre-upgrade table without a counter would 4901 even when
    // empty.
    let has_rows = {
        let mut probe = Vec::new();
        storage
            .rel_scan_slice(
                def.database_id,
                &def.name,
                ScanCursor::start(),
                1,
                None,
                &mut probe,
            )
            .map_err(|err| map_storage_err(err, &def.name))?;
        !probe.is_empty()
    };
    // The frozen fill existing rows take.
    let fill = match &column.default {
        Some(text) => {
            let sql_value = eval_default(text, eval_ctx)?;
            value::sql_to_datum(&sql_value, &bound.column_type, &bound.name)?
        }
        None => Datum::Null,
    };
    if !bound.nullable && fill.is_null() && has_rows {
        return Err(SqlError::new(
            4901,
            16,
            1,
            format!(
                "ALTER TABLE only allows columns to be added that can contain nulls, or have a DEFAULT definition specified, or the column being added is an identity or timestamp column, or alternatively if none of the previous conditions are satisfied the table must be empty to allow addition of this column. Column '{}' cannot be added to non-empty table '{}' because it does not satisfy these conditions.",
                bound.name, def.name
            ),
        )
        .at(column.span));
    }
    storage
        .rel_alter_add_column(
            def.database_id,
            &def.name,
            bound,
            column.default.clone(),
            fill,
        )
        .map_err(|err| map_storage_err(err, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ... ADD [CONSTRAINT name] CHECK (expr)`. Validates the new
/// constraint against every existing row (SQL Server's default WITH CHECK); a
/// violating row is error 547 and the constraint is not added.
fn alter_add_check(
    storage: &Storage,
    def: &TableDef,
    check: &CheckConstraint,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    // Constraint names are unique across kinds (CHECK and FOREIGN KEY).
    let existing: Vec<String> = def
        .check_constraints
        .iter()
        .map(|c| c.name.clone())
        .chain(def.foreign_keys.iter().map(|f| f.name.clone()))
        .collect();
    let new_def = bind_check(check, &schema.columns, &def.name, &existing)?;

    // WITH CHECK: no existing row may violate the new constraint.
    let compiled = vec![(
        new_def.name.clone(),
        truthdb_sql::parse_expr(&new_def.predicate)?,
    )];
    let resolver = SchemaScope { schema: &schema };
    let types = schema_types(&schema);
    let rows = storage
        .rel_scan(def.database_id, &def.name)
        .map_err(|e| map_storage_err(e, &def.name))?;
    for row in &rows {
        let scope = row_values(row, &types);
        enforce_checks(
            storage,
            &compiled,
            &scope,
            &resolver,
            eval_ctx,
            "ALTER TABLE",
            &database_name_of(storage, def.database_id),
            &def.name,
        )?;
    }

    let mut checks = def.check_constraints.clone();
    checks.push(new_def);
    storage
        .rel_set_check_constraints(def.database_id, &def.name, checks)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::Done)
}

/// `ALTER TABLE ... DROP CONSTRAINT name`. Removes a CHECK or FOREIGN KEY
/// constraint by name (case-insensitive); an unknown name is error 3728.
fn alter_drop_constraint(
    storage: &Storage,
    def: &TableDef,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    if def
        .check_constraints
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(&name.value))
    {
        let checks: Vec<catalog::CheckDef> = def
            .check_constraints
            .iter()
            .filter(|c| !c.name.eq_ignore_ascii_case(&name.value))
            .cloned()
            .collect();
        storage
            .rel_set_check_constraints(def.database_id, &def.name, checks)
            .map_err(|e| map_storage_err(e, &def.name))?;
        return Ok(StatementResult::Done);
    }
    if def
        .foreign_keys
        .iter()
        .any(|f| f.name.eq_ignore_ascii_case(&name.value))
    {
        let fks: Vec<catalog::ForeignKeyDef> = def
            .foreign_keys
            .iter()
            .filter(|f| !f.name.eq_ignore_ascii_case(&name.value))
            .cloned()
            .collect();
        storage
            .rel_set_foreign_keys(def.database_id, &def.name, fks)
            .map_err(|e| map_storage_err(e, &def.name))?;
        return Ok(StatementResult::Done);
    }
    Err(SqlError::new(
        3728,
        16,
        1,
        format!("'{}' is not a constraint.", name.value),
    )
    .at(name.span))
}

// ---- INSERT -------------------------------------------------------------

fn exec_insert(
    storage: &Storage,
    insert: &Insert,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<(StatementResult, Option<i64>), SqlError> {
    let def = resolve_table(storage, eval_ctx.database_id, &insert.table.value)
        .ok_or_else(|| SqlError::invalid_object(&insert.table.value).at(insert.table.span))?;
    reject_dml_on_view(&def)?;
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Insert)
        .map_err(|e| e.at(insert.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let ncols = schema.columns.len();
    let identity_col = def.identity.map(|s| s.column);
    let increment = def.identity.map(|s| s.increment).unwrap_or(0);

    // CHECK constraints are parsed once and evaluated against each built row.
    let checks = parse_checks(&def)?;
    let check_resolver = SchemaScope { schema: &schema };
    let check_types = schema_types(&schema);

    // Target column indices. An explicit list may not name the identity column
    // (8101) or repeat a column (264); an omitted list targets every
    // non-identity column in order (identity is server-generated).
    let target: Vec<usize> = match &insert.columns {
        Some(names) => {
            let mut indices = Vec::with_capacity(names.len());
            for n in names {
                let index = column_index(&schema, &n.value)
                    .ok_or_else(|| SqlError::invalid_column(&n.value).at(n.span))?;
                if Some(index) == identity_col {
                    return Err(SqlError::new(
                        8101,
                        16,
                        1,
                        format!(
                            "An explicit value for the identity column in table '{}' can only be specified when a column list is used and IDENTITY_INSERT is ON.",
                            def.name
                        ),
                    )
                    .at(n.span));
                }
                if indices.contains(&index) {
                    return Err(SqlError::new(
                        264,
                        16,
                        1,
                        format!(
                            "The column name '{}' is specified more than once in the SET clause or column list of an INSERT.",
                            n.value
                        ),
                    )
                    .at(n.span));
                }
                indices.push(index);
            }
            indices
        }
        None => (0..ncols).filter(|i| Some(*i) != identity_col).collect(),
    };

    // Gather the input rows (each of length `target.len()`) from either the
    // VALUES tuples or a SELECT. A SELECT is fully materialized before any
    // insert, so `INSERT INTO t SELECT ... FROM t` is Halloween-safe.
    let input_rows = insert_input_rows(storage, &insert.source, target.len(), eval_ctx)?;

    // Reserve identity values for the whole batch up front. A failed insert
    // consumes them (a gap), but a value is never reused (SQL Server-faithful).
    let identity_first = if identity_col.is_some() {
        storage
            .rel_reserve_identity(def.database_id, &def.name, input_rows.len())
            .map_err(|e| map_storage_err(e, &def.name))?
    } else {
        None
    };

    // Build every row up front; insert them as one atomic statement.
    let mut rows = Vec::with_capacity(input_rows.len());
    for (row_no, input) in input_rows.iter().enumerate() {
        check_cancelled()?;
        // Full row in schema order: unspecified columns start NULL.
        let mut values = vec![Datum::Null; ncols];
        for (position, sql_value) in target.iter().zip(input) {
            let column = &schema.columns[*position];
            if sql_value.is_null() && !column.nullable {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &insert.table.value,
                ));
            }
            values[*position] = value::sql_to_datum(sql_value, &column.column_type, &column.name)?;
        }
        // Server-generated identity value for this row.
        if let (Some(col), Some(first)) = (identity_col, identity_first) {
            let v = first.saturating_add((row_no as i64).saturating_mul(increment));
            values[col] = identity_datum(&schema.columns[col].column_type, v)?;
        }
        // DEFAULTs for columns that were neither targeted nor identity.
        for (index, column) in schema.columns.iter().enumerate() {
            if !values[index].is_null() || target.contains(&index) || Some(index) == identity_col {
                continue;
            }
            if let Some(text) = def.default_for(index) {
                let sql_value = eval_default(text, eval_ctx)?;
                values[index] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
            }
        }
        // NOT NULL enforcement after defaults/identity are applied.
        for (index, column) in schema.columns.iter().enumerate() {
            if !column.nullable && values[index].is_null() {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &insert.table.value,
                ));
            }
        }
        if !checks.is_empty() {
            let scope = row_values(&values, &check_types);
            enforce_checks(
                storage,
                &checks,
                &scope,
                &check_resolver,
                eval_ctx,
                "INSERT",
                &database_name_of(storage, def.database_id),
                &def.name,
            )?;
        }
        rows.push(values);
    }

    // FOREIGN KEY (child side): each new row must reference an existing parent
    // (a sibling row in this batch counts for a self-reference).
    if !def.foreign_keys.is_empty() {
        for row in &rows {
            enforce_child_fks(storage, &def, row, &rows, "INSERT", true)?;
        }
    }

    // Capture the new row images for an AFTER trigger's `inserted` table (only
    // when a capture is armed — the no-trigger path clones nothing). Every column
    // counts as updated for an INSERT (SQL Server's UPDATE() semantics).
    capture_trigger_images(|| (rows.clone(), Vec::new()));
    capture_trigger_updated((0..ncols).collect());
    let inserted = rows.len() as u64;
    storage
        .rel_insert_many(def.database_id, &def.name, rows, scope)
        .map_err(|err| map_storage_err(err, &def.name))?;
    // The last identity value generated (for SCOPE_IDENTITY()): the reserved
    // first value plus the increment for each subsequent row. `None` when the
    // table has no identity column or no rows were inserted.
    let last_identity = match (identity_col, identity_first) {
        (Some(_), Some(first)) if inserted > 0 => {
            Some(first.saturating_add((inserted as i64 - 1).saturating_mul(increment)))
        }
        _ => None,
    };
    Ok((StatementResult::RowsAffected(inserted), last_identity))
}

/// `INSERT [INTO] @t ...`: appends rows to an in-memory table variable. No
/// Storage, no lock, no WAL, no identity/default/CHECK/FK (deferred) — just the
/// declared column coercion, NOT NULL, and PRIMARY KEY uniqueness, all in memory
/// so a ROLLBACK leaves the rows intact (SQL Server table-variable semantics).
fn exec_insert_table_var(
    storage: &Storage,
    insert: &Insert,
    ctx: &mut TxnContext,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let key = insert
        .table
        .value
        .trim_start_matches('@')
        .to_ascii_lowercase();
    let (schema, key_columns, defaults) = {
        let tv = ctx
            .table_variables
            .get(&key)
            .ok_or_else(|| must_declare_table_var(&insert.table.value).at(insert.table.span))?;
        (
            tv.schema.clone(),
            tv.key_columns.clone(),
            tv.defaults.clone(),
        )
    };
    let ncols = schema.columns.len();
    // Target columns: an explicit list resolves against the declared schema (264
    // for a repeat); an omitted list targets every column in order.
    let target: Vec<usize> = match &insert.columns {
        Some(names) => {
            let mut indices = Vec::with_capacity(names.len());
            for n in names {
                let index = column_index(&schema, &n.value)
                    .ok_or_else(|| SqlError::invalid_column(&n.value).at(n.span))?;
                if indices.contains(&index) {
                    return Err(SqlError::new(
                        264,
                        16,
                        1,
                        format!(
                            "The column name '{}' is specified more than once in the SET clause or column list of an INSERT.",
                            n.value
                        ),
                    )
                    .at(n.span));
                }
                indices.push(index);
            }
            indices
        }
        None => (0..ncols).collect(),
    };
    // A SELECT source is fully materialized here before any append, so
    // `INSERT @t SELECT ... FROM @t` reads @t's pre-insert rows (Halloween-safe).
    let input_rows = insert_input_rows(storage, &insert.source, target.len(), eval_ctx)?;
    let mut new_rows = Vec::with_capacity(input_rows.len());
    for input in &input_rows {
        check_cancelled()?;
        let mut values = vec![Datum::Null; ncols];
        for (position, sql_value) in target.iter().zip(input) {
            let column = &schema.columns[*position];
            values[*position] = value::sql_to_datum(sql_value, &column.column_type, &column.name)?;
        }
        // DEFAULTs fill columns that were not targeted and are still NULL,
        // before the NOT NULL check — so `c INT NOT NULL DEFAULT 5` inserts 5,
        // not a spurious 515.
        for (index, column) in schema.columns.iter().enumerate() {
            if !values[index].is_null() || target.contains(&index) {
                continue;
            }
            if let Some(text) = &defaults[index] {
                let sql_value = eval_default(text, eval_ctx)?;
                values[index] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
            }
        }
        // NOT NULL after defaults applied; unspecified columns without a
        // default remain NULL.
        for (index, column) in schema.columns.iter().enumerate() {
            if !column.nullable && values[index].is_null() {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &insert.table.value,
                ));
            }
        }
        new_rows.push(values);
    }
    let tv = ctx.table_variables.get_mut(&key).expect("checked above");
    // PRIMARY KEY uniqueness (collation-aware, against existing and same-batch
    // rows). Checked before any append, so a violation appends nothing.
    if !key_columns.is_empty() {
        let mut seen: std::collections::HashSet<Vec<u8>> = tv
            .rows
            .iter()
            .filter_map(|r| crate::relstore::key::encode_key(&schema, &key_columns, r).ok())
            .collect();
        for row in &new_rows {
            let encoded = crate::relstore::key::encode_key(&schema, &key_columns, row)
                .map_err(|e| SqlError::message_only(245, e.to_string()))?;
            if !seen.insert(encoded) {
                return Err(SqlError::new(
                    2627,
                    14,
                    1,
                    "Violation of PRIMARY KEY constraint. Cannot insert duplicate key in a table variable.",
                ));
            }
        }
    }
    let inserted = new_rows.len() as u64;
    tv.rows.extend(new_rows);
    Ok(StatementResult::RowsAffected(inserted))
}

/// SQL Server 1087: a `@t` table variable used before it was declared.
fn must_declare_table_var(name: &str) -> SqlError {
    SqlError::new(
        1087,
        15,
        2,
        format!("Must declare the table variable \"{name}\"."),
    )
}

/// Produces the input rows an INSERT supplies, each already in target-column
/// order and as [`SqlValue`]s: `VALUES` tuples are evaluated as constants; a
/// `SELECT` is executed and its rows converted. Rejects an arity mismatch
/// against the target column count (110 for VALUES, 120/121 for SELECT).
fn insert_input_rows(
    storage: &Storage,
    source: &InsertSource,
    target_len: usize,
    eval_ctx: &EvalContext,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    match source {
        InsertSource::Values(rows) => {
            let mut out = Vec::with_capacity(rows.len());
            for exprs in rows {
                if exprs.len() != target_len {
                    return Err(SqlError::new(
                        110,
                        15,
                        1,
                        "There are fewer or more columns in the INSERT statement than values specified in the VALUES clause.",
                    ));
                }
                let mut vals = Vec::with_capacity(target_len);
                for expr in exprs {
                    vals.push(eval_constant(expr, eval_ctx)?);
                }
                out.push(vals);
            }
            Ok(out)
        }
        InsertSource::Select(select) => {
            let rowset = exec_select(storage, select, eval_ctx)?;
            if rowset.columns.len() != target_len {
                let (number, more_or_fewer) = if rowset.columns.len() < target_len {
                    (120, "fewer")
                } else {
                    (121, "more")
                };
                return Err(SqlError::new(
                    number,
                    15,
                    1,
                    format!(
                        "The select list for the INSERT statement contains {more_or_fewer} items than the insert list. The number of SELECT values must match the number of INSERT columns."
                    ),
                ));
            }
            let types: Vec<ColumnType> = rowset.columns.iter().map(|c| c.column_type).collect();
            Ok(rowset
                .rows
                .iter()
                .map(|row| row_values(row, &types))
                .collect())
        }
    }
}

/// Evaluates a column DEFAULT (re-parsed from its stored source text).
fn eval_default(text: &str, eval_ctx: &EvalContext) -> Result<SqlValue, SqlError> {
    let expr = truthdb_sql::parse_expr(text)?;
    eval_constant(&expr, eval_ctx)
}

/// Coerces a generated identity value to its column's integer type, erroring
/// on overflow.
fn identity_datum(column_type: &ColumnType, v: i64) -> Result<Datum, SqlError> {
    let overflow = || {
        SqlError::new(
            8115,
            16,
            1,
            format!(
                "Arithmetic overflow error converting IDENTITY to data type {}.",
                column_type.name()
            ),
        )
    };
    match column_type {
        ColumnType::TinyInt => u8::try_from(v).map(Datum::TinyInt).map_err(|_| overflow()),
        ColumnType::SmallInt => i16::try_from(v)
            .map(Datum::SmallInt)
            .map_err(|_| overflow()),
        ColumnType::Int => i32::try_from(v).map(Datum::Int).map_err(|_| overflow()),
        ColumnType::BigInt => Ok(Datum::BigInt(v)),
        // Non-integer identity columns are rejected at CREATE TABLE.
        _ => Ok(Datum::Null),
    }
}

// ---- UPDATE / DELETE ----------------------------------------------------

/// The DML target scan: current rows under lock-based isolation; under
/// SNAPSHOT isolation (the statement's thread-local snapshot is set), the
/// transaction-snapshot rows instead, each carrying a conflict mark when its
/// current state was changed or deleted by a writer the snapshot cannot see.
/// Targeting a marked row is SQL Server's 3960 update conflict.
fn scan_located_for_dml(
    storage: &Storage,
    def: &TableDef,
) -> Result<Vec<(RowLocator, Vec<Datum>, bool)>, SqlError> {
    match current_snapshot() {
        Some(snap) => storage
            .rel_scan_located_snapshot(def.database_id, &def.name, snap)
            .map_err(|e| map_storage_err(e, &def.name)),
        None => Ok(storage
            .rel_scan_located(def.database_id, &def.name)
            .map_err(|e| map_storage_err(e, &def.name))?
            .into_iter()
            .map(|(locator, row)| (locator, row, false))
            .collect()),
    }
}

/// SQL Server 3960: a SNAPSHOT transaction tried to write a row a later
/// committed transaction already changed. The whole transaction is rolled
/// back (see `exec_statement`'s 3960 handling), as SQL Server does.
fn update_conflict_error(table: &str, database: &str) -> SqlError {
    SqlError::new(
        3960,
        16,
        1,
        format!(
            "Snapshot isolation transaction aborted due to update conflict. You cannot use \
             snapshot isolation to access table '{table}' directly or indirectly in database \
             '{database}' to update, delete, or insert the row that has been modified or \
             deleted by another transaction. Retry the transaction or change the isolation \
             level for the update/delete statement."
        ),
    )
}

fn exec_update(
    storage: &Storage,
    update: &Update,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, eval_ctx.database_id, &update.table.value)
        .ok_or_else(|| SqlError::invalid_object(&update.table.value).at(update.table.span))?;
    reject_dml_on_view(&def)?;
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Update)
        .map_err(|e| e.at(update.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = SchemaScope { schema: &schema };
    let identity_col = def.identity.map(|s| s.column);
    let checks = parse_checks(&def)?;

    // Resolve each SET target once; an IDENTITY column cannot be updated.
    let mut assignments: Vec<(usize, &Expr)> = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let index = column_index(&schema, &assignment.column.value).ok_or_else(|| {
            SqlError::invalid_column(&assignment.column.value).at(assignment.column.span)
        })?;
        if Some(index) == identity_col {
            return Err(SqlError::new(
                8102,
                16,
                1,
                format!(
                    "Cannot update identity column '{}'.",
                    assignment.column.value
                ),
            )
            .at(assignment.column.span));
        }
        if assignments.iter().any(|(i, _)| *i == index) {
            return Err(SqlError::new(
                264,
                16,
                1,
                format!(
                    "The column name '{}' is specified more than once in the SET clause or column list of an INSERT. A column cannot be assigned more than one value in the same clause.",
                    assignment.column.value
                ),
            )
            .at(assignment.column.span));
        }
        assignments.push((index, &assignment.value));
    }

    // Materialize the whole table (Halloween-safe), filter, and compute new
    // rows before any mutation.
    let located = scan_located_for_dml(storage, &def)?;
    let types = schema_types(&schema);
    let mut updates = Vec::new();
    for (locator, row, conflict) in located {
        check_cancelled()?;
        if !predicate_true(&update.where_clause, &row, &types, &resolver, eval_ctx)? {
            continue;
        }
        if conflict {
            return Err(update_conflict_error(&def.name, &eval_ctx.database));
        }
        // Every SET expression sees the pre-update row; keep the old values
        // for secondary-index maintenance.
        let old_values = row.clone();
        let old_scope = row_values(&row, &types);
        let mut new_row = row;
        for (index, expr) in &assignments {
            let column = &schema.columns[*index];
            let sql_value = eval::eval(expr, &old_scope, &resolver, eval_ctx)?;
            if sql_value.is_null() && !column.nullable {
                return Err(SqlError::null_into_not_null(
                    &column.name,
                    &update.table.value,
                ));
            }
            new_row[*index] = value::sql_to_datum(&sql_value, &column.column_type, &column.name)?;
        }
        if !checks.is_empty() {
            let scope = row_values(&new_row, &types);
            enforce_checks(
                storage,
                &checks,
                &scope,
                &resolver,
                eval_ctx,
                "UPDATE",
                &database_name_of(storage, def.database_id),
                &def.name,
            )?;
        }
        updates.push((locator, old_values, new_row));
    }

    // FOREIGN KEY (child side): each updated row must still reference a valid
    // parent. Self-referencing FKs are validated separately below.
    if !def.foreign_keys.is_empty() {
        for (_, _, new_row) in &updates {
            enforce_child_fks(storage, &def, new_row, &[], "UPDATE", false)?;
        }
    }
    // FOREIGN KEY (parent side, other tables): a row whose primary key changes
    // vacates its old key; no surviving child in ANOTHER table may still
    // reference it (NO ACTION). Self-references are handled by the snapshot.
    if def.is_tree() {
        let removed: Vec<Vec<Datum>> = updates
            .iter()
            .filter_map(|(_, old, new)| {
                let old_pk = pk_of(&def, old);
                (old_pk != pk_of(&def, new)).then_some(old_pk)
            })
            .collect();
        enforce_parent_fks(storage, &def, &removed, "UPDATE", false)?;
    }
    // FOREIGN KEY (self-reference): a self-referencing table's own foreign keys
    // must hold against the state the UPDATE produces — a pre-mutation probe
    // sees stale rows. Every surviving row's non-NULL self-FK key must match a
    // surviving primary key.
    if def.is_tree()
        && def
            .foreign_keys
            .iter()
            .any(|fk| fk.parent.eq_ignore_ascii_case(&def.name))
    {
        let old_pks: Vec<Vec<Datum>> = updates.iter().map(|(_, old, _)| pk_of(&def, old)).collect();
        let mut post_rows: Vec<Vec<Datum>> = storage
            .rel_scan(def.database_id, &def.name)
            .map_err(|e| map_storage_err(e, &def.name))?
            .into_iter()
            .filter(|r| !old_pks.contains(&pk_of(&def, r)))
            .collect();
        post_rows.extend(updates.iter().map(|(_, _, new)| new.clone()));
        // Fold the surviving PKs and each FK reference by the (self-referenced)
        // PK collation, so a case-insensitive self-reference matches a case-
        // variant sibling — consistent with the INSERT batch path
        // (`fk_parent_exists`) and the DELETE path (`enforce_parent_fks`).
        let key_coll: Vec<Option<String>> = def
            .key_columns
            .iter()
            .map(|&i| def.collations.get(i).cloned().flatten())
            .collect();
        let post_pks: Vec<Vec<u8>> = post_rows
            .iter()
            .map(|r| collated_key(&pk_of(&def, r), &key_coll))
            .collect();
        for r in &post_rows {
            for fk in def
                .foreign_keys
                .iter()
                .filter(|fk| fk.parent.eq_ignore_ascii_case(&def.name))
            {
                if let Some(key) = fk_key(fk, r)
                    && !post_pks.contains(&collated_key(&key, &key_coll))
                {
                    return Err(fk_child_violation(
                        &database_name_of(storage, def.database_id),
                        &fk.name,
                        "UPDATE",
                        &fk.parent,
                    ));
                }
            }
        }
    }

    // Capture the old/new images for an AFTER trigger's `deleted`/`inserted`
    // tables (a row that did not change still appears in both, as SQL Server
    // does — every matched row is in `updates`).
    capture_trigger_images(|| {
        (
            updates.iter().map(|(_, _, new)| new.clone()).collect(),
            updates.iter().map(|(_, old, _)| old.clone()).collect(),
        )
    });
    capture_trigger_updated(assignments.iter().map(|(i, _)| *i).collect());
    let count = storage
        .rel_update_located(def.database_id, &def.name, updates, scope)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::RowsAffected(count as u64))
}

fn exec_delete(
    storage: &Storage,
    delete: &Delete,
    scope: &mut TxnScope,
    eval_ctx: &EvalContext,
) -> Result<StatementResult, SqlError> {
    let def = resolve_table(storage, eval_ctx.database_id, &delete.table.value)
        .ok_or_else(|| SqlError::invalid_object(&delete.table.value).at(delete.table.span))?;
    reject_dml_on_view(&def)?;
    enforce_object_permission(storage, &def, &eval_ctx.security, PermAction::Delete)
        .map_err(|e| e.at(delete.table.span))?;
    let schema = def.schema().map_err(|e| map_storage_err(e, &def.name))?;
    let resolver = SchemaScope { schema: &schema };

    let types = schema_types(&schema);
    let located = scan_located_for_dml(storage, &def)?;
    let mut targets = Vec::new();
    for (locator, row, conflict) in located {
        check_cancelled()?;
        if predicate_true(&delete.where_clause, &row, &types, &resolver, eval_ctx)? {
            if conflict {
                return Err(update_conflict_error(&def.name, &eval_ctx.database));
            }
            // Keep the row values for secondary-index maintenance.
            targets.push((locator, row));
        }
    }

    // FOREIGN KEY (parent side): no surviving child may reference a deleted row
    // (a self-referencing row that is itself deleted does not count).
    if def.is_tree() {
        let removed: Vec<Vec<Datum>> = targets.iter().map(|(_, row)| pk_of(&def, row)).collect();
        enforce_parent_fks(storage, &def, &removed, "DELETE", true)?;
    }

    // Capture the deleted images for an AFTER trigger's `deleted` table.
    capture_trigger_images(|| {
        (
            Vec::new(),
            targets.iter().map(|(_, row)| row.clone()).collect(),
        )
    });
    let count = storage
        .rel_delete_located(def.database_id, &def.name, targets, scope)
        .map_err(|e| map_storage_err(e, &def.name))?;
    Ok(StatementResult::RowsAffected(count as u64))
}

/// Resolver over a single table's schema columns, carrying per-column collation.
/// UPDATE/DELETE/CHECK predicate evaluation must go through this (not a bare
/// `Vec<String>`, whose `ColumnResolver::collation` reports the case-insensitive
/// default for *every* column) so an explicit `_CS`/`_BIN` column compares
/// case-sensitively — otherwise a `DELETE ... WHERE cs_col = 'abc'` would fold
/// case and remove case-variant rows it must keep.
struct SchemaScope<'a> {
    schema: &'a Schema,
}

impl truthdb_sql::eval::ColumnResolver for SchemaScope<'_> {
    fn resolve(&self, name: &str) -> Option<usize> {
        self.schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    fn collation(&self, index: usize) -> CollationSensitivity {
        CollationSensitivity::from_optional(
            self.schema
                .columns
                .get(index)
                .and_then(|c| c.collation.as_deref()),
        )
    }
}

fn schema_types(schema: &Schema) -> Vec<ColumnType> {
    schema.columns.iter().map(|c| c.column_type).collect()
}

/// Evaluates an optional WHERE predicate against a row. Absent WHERE matches
/// all rows; a NULL/UNKNOWN result does not match; a non-boolean predicate is
/// error 4145 (same rule as SELECT).
fn predicate_true(
    where_clause: &Option<Expr>,
    row: &[Datum],
    types: &[ColumnType],
    resolver: &impl ColumnResolver,
    eval_ctx: &EvalContext,
) -> Result<bool, SqlError> {
    let Some(predicate) = where_clause else {
        return Ok(true);
    };
    match eval::eval(predicate, &row_values(row, types), resolver, eval_ctx)? {
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
