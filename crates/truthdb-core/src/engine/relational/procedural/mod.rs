use super::prelude::*;

/// The inner SQL text of an `EXEC sp_executesql N'...'` whose statement
/// argument is a string LITERAL — the analyzable case. `None` for any other
/// procedure or a non-literal statement argument.
/// Runs a statement list, recursing into `TRY`/`CATCH`. `in_try` is true while
/// executing inside a `TRY` block, where a statement error transfers control to
/// the matching `CATCH` (returned as `Err`) instead of applying the normal
/// batch policy. Returns `Err` when the enclosing context must stop: a cancel,
/// an error that propagates to a `CATCH`, or a dooming/terminating error at the
/// top level.
pub(super) fn exec_literal_sql(exec: &ExecStatement) -> Option<String> {
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
    pub(super) static EXEC_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Ownership-chaining depth for object-permission checks: how many OWNED
    /// stored-object bodies (procedure, scalar UDF, multi-statement TVF, trigger)
    /// enclose the current statement. Distinct from [`EXEC_DEPTH`] because
    /// `sp_executesql` bumps that but does NOT chain — dynamic SQL runs in the
    /// caller's own permission context. Permission checks fire only where this
    /// (and `VIEW_DEPTH`) is 0.
    pub(super) static CHAIN_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// RAII guard entered when running an OWNED stored-object body (procedure,
/// scalar UDF, multi-statement TVF, trigger): it raises the ownership-chaining
/// depth so the body's object reads are not re-permission-checked (the caller's
/// permission on the object suffices — single `dbo` owner).
pub(super) struct ChainGuard;

impl ChainGuard {
    pub(super) fn enter() -> Self {
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
pub(super) struct DynamicScope(u32);

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
pub(super) enum ExecError {
    Own(SqlError),
    Inner(SqlError),
}

/// Applies the standard doom rule to an error raised outside any statement's
/// own execution — `run_exec`'s validation and depth errors, which no inner
/// `run_block` arm will see. The decision is made here, at the source, so the
/// TRY boundary never has to re-derive it (it cannot know the error's origin).
pub(super) fn doom_per_rule(txn_ctx: &mut TxnContext, error: SqlError) -> SqlError {
    if txn_ctx.in_txn() && (txn_ctx.xact_abort || error.level >= XACT_ABORT_SEVERITY) {
        txn_ctx.doomed = true;
    }
    error
}

mod control;
mod dynamic;
mod errors;
mod routines;

pub(super) use control::*;
pub(super) use dynamic::*;
pub(super) use errors::*;
pub(super) use routines::*;
