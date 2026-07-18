//! Regression probes from the adversarial review for the Stage 14 SSMS commits (57e8e2b +
//! b4e7be2). Engine-level: @@ROWCOUNT edges, NOCOUNT on the buffered
//! (native) path, sys.databases/sys.configurations shapes, SERVERPROPERTY
//! edge arguments, USE under transactions and TRY/CATCH.

use std::path::{Path, PathBuf};

use truthdb_core::engine::Engine;
use truthdb_core::rel::{StatementResult, TxnContext};
use truthdb_core::relstore::types::Datum;
use truthdb_core::storage::{Storage, StorageOptions};

fn temp_path(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("truthdb-poc-{label}-{nanos}.db"));
    path
}

fn new_engine(path: &Path) -> Engine {
    let opts = StorageOptions {
        size_gib: 1,
        wal_ratio: 0.05,
        metadata_ratio: 0.08,
        snapshot_ratio: 0.02,
        allocator_ratio: 0.02,
        reserved_ratio: 0.17,
        default_collation: None,
    };
    let storage = Storage::create(path.to_path_buf(), opts).expect("create storage");
    Engine::new(storage).expect("engine")
}

fn ctx() -> TxnContext {
    let mut ctx = TxnContext::default();
    ctx.set_session_identity("truthdb".into(), "sa".into(), 1, "dbo".into(), 0, 0);
    ctx
}

/// Runs a batch, asserts no error, and returns the LAST result set's single
/// INT cell (the shape every probe below uses).
fn probe_int(engine: &Engine, ctx: &mut TxnContext, sql: &str) -> i64 {
    let outcome = engine.sql_batch(sql, ctx).expect("sql_batch");
    assert!(outcome.error.is_none(), "batch error: {:?}", outcome.error);
    let rows = outcome
        .results
        .iter()
        .rev()
        .find_map(|r| match r {
            StatementResult::Rows(rowset) => Some(&rowset.rows),
            _ => None,
        })
        .expect("a result set");
    match &rows[0][0] {
        Datum::Int(v) => *v as i64,
        Datum::BigInt(v) => *v,
        Datum::Bit(b) => *b as i64,
        other => panic!("not an int: {other:?}"),
    }
}

fn ok(engine: &Engine, ctx: &mut TxnContext, sql: &str) {
    let outcome = engine.sql_batch(sql, ctx).expect("sql_batch");
    assert!(outcome.error.is_none(), "batch error: {:?}", outcome.error);
}

/// PASS expected: @@ROWCOUNT after `EXEC sp_executesql N'INSERT ...'` is the
/// inner statement's count (the EXEC itself neither resets nor double-sets).
#[test]
fn rowcount_after_exec_is_the_inner_count() {
    let path = temp_path("exec-rc");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    ok(
        &engine,
        &mut ctx,
        "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)",
    );
    ok(
        &engine,
        &mut ctx,
        "EXEC sp_executesql N'INSERT INTO t1 VALUES (1), (2)'",
    );
    assert_eq!(probe_int(&engine, &mut ctx, "SELECT @@ROWCOUNT"), 2);
    let _ = std::fs::remove_file(&path);
}

/// PASS expected: a statement that fails inside TRY resets @@ROWCOUNT to 0
/// before the CATCH block runs.
#[test]
fn rowcount_in_catch_after_a_failed_statement_is_zero() {
    let path = temp_path("catch-rc");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    ok(
        &engine,
        &mut ctx,
        "CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY)",
    );
    ok(&engine, &mut ctx, "INSERT INTO t2 VALUES (1), (2), (3)");
    let got = probe_int(
        &engine,
        &mut ctx,
        "BEGIN TRY INSERT INTO t2 VALUES (1) END TRY BEGIN CATCH SELECT @@ROWCOUNT END CATCH",
    );
    assert_eq!(got, 0, "@@ROWCOUNT inside CATCH after a failed INSERT");
    let _ = std::fs::remove_file(&path);
}

/// FINDING probe: an EXEC that fails BEFORE its inner batch runs (parse error
/// in the dynamic SQL, unknown procedure) must also reset @@ROWCOUNT to 0 —
/// the commit's own contract ("failed statements reset it to 0"). The Exec
/// branch of run_block bypasses the Err-arm reset.
#[test]
fn rowcount_in_catch_after_a_failed_exec_is_zero() {
    let path = temp_path("exec-fail-rc");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    ok(
        &engine,
        &mut ctx,
        "CREATE TABLE t3 (id INT NOT NULL PRIMARY KEY)",
    );
    ok(&engine, &mut ctx, "INSERT INTO t3 VALUES (1), (2), (3)");
    // Inner text is unparseable: run_exec fails before run_block.
    let got = probe_int(
        &engine,
        &mut ctx,
        "BEGIN TRY EXEC sp_executesql N'SELEC 1' END TRY BEGIN CATCH SELECT @@ROWCOUNT END CATCH",
    );
    assert_eq!(got, 0, "@@ROWCOUNT after a failed EXEC (inner parse error)");

    ok(&engine, &mut ctx, "INSERT INTO t3 VALUES (4), (5), (6)");
    // Unknown procedure: 2812 before anything runs.
    let got = probe_int(
        &engine,
        &mut ctx,
        // (The semicolon matters: an argless EXEC otherwise consumes END as
        // an argument expression — a pre-existing parser quirk.)
        "BEGIN TRY EXEC no_such_proc; END TRY BEGIN CATCH SELECT @@ROWCOUNT END CATCH",
    );
    assert_eq!(got, 0, "@@ROWCOUNT after a failed EXEC (2812)");
    let _ = std::fs::remove_file(&path);
}

/// NOCOUNT silences counts on EVERY client protocol, the resolved semantics
/// (the review flagged the doc claiming otherwise; the doc was corrected):
/// the native envelope's INSERT becomes a bare done — the CLI then prints
/// no "(n rows affected)" line, exactly as sqlcmd goes quiet against SQL
/// Server — while @@ROWCOUNT still reports the true count.
#[test]
fn nocount_silences_the_native_count_envelope_too() {
    let path = temp_path("nocount-native");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    ok(
        &engine,
        &mut ctx,
        "CREATE TABLE t4 (id INT NOT NULL PRIMARY KEY)",
    );
    let outcome = engine
        .sql_batch("SET NOCOUNT ON; INSERT INTO t4 VALUES (1)", &mut ctx)
        .expect("sql_batch");
    assert!(outcome.error.is_none());
    assert!(
        matches!(
            outcome.results.as_slice(),
            [StatementResult::Done, StatementResult::Done]
        ),
        "NOCOUNT drops the count envelope: {:?}",
        outcome.results
    );
    assert_eq!(
        probe_int(&engine, &mut ctx, "SELECT @@ROWCOUNT"),
        1,
        "@@ROWCOUNT is untouched by NOCOUNT"
    );
    let _ = std::fs::remove_file(&path);
}

/// PASS expected: the new sys views behave as relational sources — join,
/// WHERE on a BIT column, ORDER BY, projection pruning.
#[test]
fn sys_views_join_filter_and_order_like_tables() {
    let path = temp_path("sysviews");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    // WHERE on a BIT column.
    let got = probe_int(
        &engine,
        &mut ctx,
        "SELECT COUNT(*) FROM sys.databases WHERE is_read_committed_snapshot_on = 0",
    );
    assert_eq!(got, 1);
    // Join across the two views with projection pruning.
    let got = probe_int(
        &engine,
        &mut ctx,
        "SELECT COUNT(*) FROM sys.databases d JOIN sys.configurations c \
         ON c.configuration_id = 16384 WHERE d.database_id = 1",
    );
    assert_eq!(got, 1);
    // ORDER BY + TOP over sys.configurations.
    let got = probe_int(
        &engine,
        &mut ctx,
        "SELECT TOP 1 configuration_id FROM sys.configurations ORDER BY configuration_id",
    );
    assert_eq!(got, 1539);
    // WHERE on NVARCHAR with the all-None collations vec.
    let got = probe_int(
        &engine,
        &mut ctx,
        "SELECT is_dynamic FROM sys.configurations WHERE name = 'user options'",
    );
    assert_eq!(got, 1);
    let _ = std::fs::remove_file(&path);
}

/// PASS expected: SERVERPROPERTY edge arguments — NULL, non-string, nested
/// in another function, used in WHERE.
#[test]
fn serverproperty_edge_arguments() {
    let path = temp_path("svrprop");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    let one = |ctx: &mut TxnContext, sql: &str| -> Datum {
        let outcome = engine.sql_batch(sql, ctx).expect("sql_batch");
        assert!(
            outcome.error.is_none(),
            "error for {sql}: {:?}",
            outcome.error
        );
        match &outcome.results[0] {
            StatementResult::Rows(rowset) => rowset.rows[0][0].clone(),
            other => panic!("no rows for {sql}: {other:?}"),
        }
    };
    assert_eq!(one(&mut ctx, "SELECT SERVERPROPERTY(NULL)"), Datum::Null);
    assert_eq!(one(&mut ctx, "SELECT SERVERPROPERTY(1)"), Datum::Null);
    assert_eq!(
        one(&mut ctx, "SELECT UPPER(SERVERPROPERTY('Edition'))"),
        Datum::NVarChar("TRUTHDB EDITION (64-BIT)".into()),
    );
    let got = probe_int(
        &engine,
        &mut ctx,
        "SELECT COUNT(*) FROM sys.databases WHERE SERVERPROPERTY('EngineEdition') = 3",
    );
    assert_eq!(got, 1);
    let _ = std::fs::remove_file(&path);
}

/// PASS expected: USE inside an open transaction and inside TRY/CATCH; a
/// failed USE inside TRY is caught and leaves @@ROWCOUNT 0.
#[test]
fn use_in_transaction_and_try_catch() {
    let path = temp_path("use-txn");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    ok(&engine, &mut ctx, "BEGIN TRANSACTION; USE truthdb; COMMIT");
    ok(
        &engine,
        &mut ctx,
        "BEGIN TRY USE truthdb END TRY BEGIN CATCH SELECT 1 END CATCH",
    );
    let got = probe_int(
        &engine,
        &mut ctx,
        "BEGIN TRY USE somewhere_else END TRY BEGIN CATCH SELECT @@ROWCOUNT END CATCH",
    );
    assert_eq!(got, 0, "a caught failed USE resets @@ROWCOUNT");
    let _ = std::fs::remove_file(&path);
}

/// PASS expected: @@ROWCOUNT is session state — it survives batch
/// boundaries (SQL Server: the next batch's first read sees it).
#[test]
fn rowcount_persists_across_batches() {
    let path = temp_path("rc-batches");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    ok(
        &engine,
        &mut ctx,
        "CREATE TABLE t5 (id INT NOT NULL PRIMARY KEY)",
    );
    ok(&engine, &mut ctx, "INSERT INTO t5 VALUES (1), (2), (3)");
    assert_eq!(probe_int(&engine, &mut ctx, "SELECT @@ROWCOUNT"), 3);
    let _ = std::fs::remove_file(&path);
}

/// Bare DECLARE preserves @@ROWCOUNT (SQL Server's documented reset list —
/// USE, SET options, transaction control — does not include it; resolved
/// from the review's finding 4).
#[test]
fn bare_declare_preserves_rowcount() {
    let path = temp_path("declare-rc");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    ok(
        &engine,
        &mut ctx,
        "CREATE TABLE td (id INT NOT NULL PRIMARY KEY)",
    );
    ok(&engine, &mut ctx, "INSERT INTO td VALUES (1), (2), (3)");
    let got = probe_int(&engine, &mut ctx, "DECLARE @x INT; SELECT @@ROWCOUNT");
    assert_eq!(got, 3, "bare DECLARE must not reset @@ROWCOUNT");
    let _ = std::fs::remove_file(&path);
}

/// PASS expected: SERVERPROPERTY inside a view definition, and
/// describe_first_result_set over the new sys views answers 11514 exactly
/// as the pre-existing sys views do (scan_plan rejects sys.* sources).
#[test]
fn serverproperty_in_view_and_describe_sys_views() {
    let path = temp_path("svrprop-view");
    let engine = new_engine(&path);
    let mut ctx = ctx();
    ok(
        &engine,
        &mut ctx,
        "CREATE VIEW vprop AS SELECT SERVERPROPERTY('Edition') AS e",
    );
    let outcome = engine
        .sql_batch("SELECT e FROM vprop", &mut ctx)
        .expect("sql_batch");
    assert!(
        outcome.error.is_none(),
        "view over SERVERPROPERTY: {:?}",
        outcome.error
    );
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(
                rowset.rows[0][0],
                Datum::NVarChar("TruthDB Edition (64-bit)".into())
            );
        }
        other => panic!("no rows: {other:?}"),
    }
    let err = engine
        .describe_first_result_set("SELECT name FROM sys.databases")
        .expect_err("sys views are not statically derivable");
    assert_eq!(err.number, 11514, "same as the pre-existing sys views");
    let err = engine
        .describe_first_result_set("SELECT name FROM sys.tables")
        .expect_err("precedent: sys.tables");
    assert_eq!(err.number, 11514);
    let _ = std::fs::remove_file(&path);
}
