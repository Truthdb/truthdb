use super::*;

#[test]
fn procedure_ddl_round_trips_and_survives_reopen() {
    // CREATE/ALTER/DROP PROCEDURE: catalog persistence (the body is
    // stored text), name collision (2714), the first-statement rule
    // (111), and RETURN <value> legal only inside a body.
    let path = unique_temp_path("proc-ddl");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE add_pair @a INT, @b INT = 7 OUTPUT AS \
             SET @b = @a + @b; RETURN 0",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    // Name collision with any object class.
    let out = batch(&engine, &mut ctx, "CREATE PROC add_pair AS SELECT 1");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(2714));
    // Not the first statement in the batch: 111.
    let out = batch(&engine, &mut ctx, "SELECT 1; CREATE PROC late AS SELECT 1");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(111));
    // RETURN <value> stays illegal OUTSIDE a body.
    let out = batch(&engine, &mut ctx, "RETURN 3");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(178));
    // ALTER replaces; ALTER of a missing procedure errors.
    let out = batch(
        &engine,
        &mut ctx,
        "ALTER PROCEDURE add_pair @a INT AS RETURN @a",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "ALTER PROC no_such AS SELECT 1");
    assert!(out.error.is_some(), "ALTER of a missing procedure fails");
    drop(engine);

    // The definition survives a reopen; DROP removes it.
    let engine = {
        let storage = Storage::open(path.clone()).expect("reopen");
        Engine::new(storage).expect("engine")
    };
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "CREATE PROC add_pair AS SELECT 1");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(2714),
        "the procedure survived the reopen"
    );
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE add_pair");
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE add_pair");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3701));
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE IF EXISTS add_pair");
    assert!(out.error.is_none(), "IF EXISTS swallows the miss");
    let _ = std::fs::remove_file(path);
}

#[test]
fn exec_user_procedure_binds_returns_and_copies_output() {
    let path = unique_temp_path("proc-exec");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE ins_and_double @a INT, @b INT = 10, @twice INT OUTPUT AS \
             INSERT INTO t VALUES (@a); \
             SET @twice = (@a + @b) * 2; \
             SELECT @a AS n; \
             RETURN @a + 1",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    // Positional + named + OUTPUT + @rc, default filling @b.
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @d INT, @rc INT; \
             EXEC @rc = ins_and_double 5, @twice = @d OUTPUT; \
             SELECT @rc AS n; SELECT @d AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let firsts: Vec<i64> = out
        .results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                Datum::Int(v) => Some(i64::from(v)),
                Datum::BigInt(v) => Some(v),
                ref other => panic!("expected int, got {other:?}"),
            },
            _ => None,
        })
        .collect();
    assert_eq!(
        firsts,
        vec![5, 6, 30],
        "body SELECT streamed; @rc = RETURN @a+1; @twice = (5+10)*2"
    );
    let out = batch(&engine, &mut ctx, "SELECT id FROM t");
    assert_eq!(ids(&out), vec![5], "the body's INSERT landed");
    let _ = std::fs::remove_file(path);
}

#[test]
fn exec_user_procedure_argument_errors() {
    let path = unique_temp_path("proc-args");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE p2 @a INT, @o INT OUTPUT AS SET @o = @a",
    );
    let case = |sql: &str| -> Option<i32> {
        let mut c = TxnContext::default();
        batch(&engine, &mut c, sql).error.map(|e| e.number)
    };
    assert_eq!(case("EXEC p2"), Some(201), "missing @a");
    assert_eq!(
        case("DECLARE @x INT; EXEC p2 1, @x OUTPUT, 3"),
        Some(8144),
        "too many arguments"
    );
    assert_eq!(
        case("DECLARE @x INT; EXEC p2 @a = 1, @nope = 2"),
        Some(8145),
        "unknown named parameter"
    );
    assert_eq!(
        case("DECLARE @x INT; EXEC p2 @a = @x OUTPUT, @o = @x"),
        Some(8162),
        "OUTPUT on a non-OUTPUT parameter"
    );
    assert_eq!(
        case("EXEC p2 1, 2 OUTPUT"),
        Some(179),
        "OUTPUT with a constant"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn recursive_procedure_hits_the_nesting_cap() {
    let path = unique_temp_path("proc-recurse");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE recur AS SELECT CAST(@@NESTLEVEL AS INT) AS n; EXEC recur",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC recur");
    // The batch surfaces 217 when the recursion exceeds depth 32...
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(217));
    // ...and the streamed @@NESTLEVEL values count 1, 2, 3, ...
    let levels: Vec<i64> = out
        .results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                Datum::Int(v) => Some(i64::from(v)),
                Datum::BigInt(v) => Some(v),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(levels.first(), Some(&1));
    assert_eq!(levels.last(), Some(&32), "depth 32 ran; 33 was refused");
    let _ = std::fs::remove_file(path);
}

#[test]
fn error_procedure_names_the_failing_procedure() {
    let path = unique_temp_path("proc-errproc");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE boomer AS RAISERROR('inside', 16, 1)",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY EXEC boomer; END TRY \
             BEGIN CATCH SELECT ERROR_PROCEDURE() AS p, ERROR_NUMBER() AS n; END CATCH",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected rows");
    };
    assert_eq!(
        rowset.rows[0][0],
        Datum::NVarChar("boomer".into()),
        "ERROR_PROCEDURE() names the proc"
    );
    // Outside any procedure, ERROR_PROCEDURE() stays NULL.
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE u (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO u VALUES (1)");
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY INSERT INTO u VALUES (1); END TRY \
             BEGIN CATCH SELECT ERROR_PROCEDURE() AS p; END CATCH",
    );
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected rows");
    };
    assert_eq!(rowset.rows[0][0], Datum::Null, "NULL in an ad-hoc batch");
    let _ = std::fs::remove_file(path);
}

#[test]
fn output_and_return_skipped_when_the_body_aborts() {
    let path = unique_temp_path("proc-abort-output");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE fails @o INT OUTPUT AS SET @o = 99; THROW 50001, 'die', 1",
    );
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @v INT = 7, @rc INT = -1; \
             EXEC @rc = fails @o = @v OUTPUT; \
             SELECT @v AS n; SELECT @rc AS n",
    );
    // The THROW terminated the batch too, so nothing after the EXEC ran —
    // and neither copy-back nor @rc assignment happened.
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(50001));
    assert!(
        !out.results
            .iter()
            .any(|r| matches!(r, StatementResult::Rows(_))),
        "the post-EXEC selects never ran: {:?}",
        out.results
    );
    let _ = std::fs::remove_file(path);
}

// ---- adversarial review probes: Stage 15 stored procedures ----------

/// First cell of every streamed rowset, as i64 (panics on non-integers).
fn review_first_cells(out: &BatchOutcome) -> Vec<i64> {
    out.results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                Datum::Int(v) => Some(i64::from(v)),
                Datum::BigInt(v) => Some(v),
                ref other => panic!("expected int, got {other:?}"),
            },
            _ => None,
        })
        .collect()
}

/// SQL Server refuses a positional argument after a named one (error
/// 119). The current binder accepts it and binds BOTH: the positional
/// value lands on the parameter by position and a named argument for the
/// same parameter is silently discarded — a silent misbind.
#[test]
fn review_poc_positional_after_named_is_refused() {
    let path = unique_temp_path("proc-pos-after-named");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE pan @a INT, @b INT AS SELECT @a * 100 + @b AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC pan @b = 1, 2");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(119),
        "a positional argument after a named one is error 119; instead: \
             error {:?}, streamed {:?}",
        out.error,
        review_first_cells(&out)
    );
    let _ = std::fs::remove_file(path);
}

/// SQL Server coerces each argument to the declared parameter type at
/// bind time ('7' for an INT parameter arrives as int 7; an unconvertible
/// string is a conversion error at the EXEC). The current binder stores
/// the raw value with the declared type tag and never converts, so the
/// body sees a string where it declared an INT.
#[test]
fn review_poc_exec_argument_coerced_to_declared_type() {
    let path = unique_temp_path("proc-arg-coerce");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE echoi @a INT AS SELECT @a AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC echoi '7'");
    assert!(out.error.is_none(), "{:?}", out.error);
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected rows, got {:?}", out.results);
    };
    assert!(
        matches!(rowset.rows[0][0], Datum::Int(7) | Datum::BigInt(7)),
        "'7' bound to @a INT must arrive as int 7 (DECLARE/SET coerce; \
             EXEC binding must too), got {:?}",
        rowset.rows[0][0]
    );
    // An unconvertible string is a conversion error at the EXEC.
    let out = batch(&engine, &mut ctx, "EXEC echoi 'nope'");
    assert!(
        out.error.is_some(),
        "'nope' for @a INT must fail conversion at bind, streamed {:?}",
        out.results
    );
    let _ = std::fs::remove_file(path);
}

/// `EXEC @rc = p` with an UNDECLARED @rc is error 137 in SQL Server. The
/// current code inserts the variable into the caller scope unconditionally,
/// silently creating it.
#[test]
fn review_poc_undeclared_return_status_variable_is_137() {
    let path = unique_temp_path("proc-undeclared-rc");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "CREATE PROCEDURE r4 AS RETURN 4");
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC @rc = r4; SELECT @rc AS n");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(137),
        "an undeclared return-status variable must be 137; instead: \
             error {:?}, streamed {:?}",
        out.error,
        review_first_cells(&out)
    );
    let _ = std::fs::remove_file(path);
}

/// A proc error caught by the CALLER's TRY terminated the body: neither
/// OUTPUT copy-back nor the @rc assignment happens, but the batch
/// continues in the CATCH. (The committed abort test cannot observe this
/// — its THROW ends the whole batch before anything reads @v/@rc.)
#[test]
fn review_poc_output_and_rc_skipped_when_proc_error_is_caught() {
    let path = unique_temp_path("proc-caught-output");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE failo @o INT OUTPUT AS \
             SET @o = 99; RAISERROR('die', 16, 1); SET @o = 98",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @v INT = 7, @rc INT = -1; \
             BEGIN TRY EXEC @rc = failo @o = @v OUTPUT; END TRY \
             BEGIN CATCH SELECT ERROR_NUMBER() AS n; END CATCH; \
             SELECT @v AS n; SELECT @rc AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        review_first_cells(&out),
        vec![50000, 7, -1],
        "caught proc error: no copy-back (7 stays), no @rc (-1 stays)"
    );
    let _ = std::fs::remove_file(path);
}

/// A statement-scope RAISERROR 16 with no TRY anywhere does NOT terminate
/// the proc body: the body runs to completion, so OUTPUT copy-back DOES
/// happen — with the value assigned after the error.
#[test]
fn review_poc_statement_scope_raiserror_completes_body_and_copies_output() {
    let path = unique_temp_path("proc-warn-output");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE warns @o INT OUTPUT AS \
             SET @o = 1; RAISERROR('w', 16, 1); SET @o = 2",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @v INT = 0; EXEC warns @o = @v OUTPUT; SELECT @v AS n",
    );
    assert_eq!(
        review_first_cells(&out),
        vec![2],
        "the body completed past the statement-scope error; copy-back ran"
    );
    let _ = std::fs::remove_file(path);
}

/// Nested procedures: each frame's RETURN status is its own. An inner
/// EXEC's status never bleeds into the outer frame's `EXEC @rc =`, even
/// when the outer body's LAST action is that inner EXEC.
#[test]
fn review_poc_nested_return_statuses_do_not_bleed() {
    let path = unique_temp_path("proc-nested-rc");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE PROCEDURE five AS RETURN 5",
        "CREATE PROCEDURE seven AS EXEC five; RETURN 7",
        "CREATE PROCEDURE tail AS EXEC five",
        "CREATE PROCEDURE captures AS DECLARE @x INT; EXEC @x = five; SELECT @x AS n",
    ] {
        let out = batch(&engine, &mut ctx, sql);
        assert!(out.error.is_none(), "{sql}: {:?}", out.error);
    }
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @a INT, @b INT, @c INT; \
             EXEC @a = seven; EXEC @b = tail; EXEC @c = captures; \
             SELECT @a AS n; SELECT @b AS n; SELECT @c AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        review_first_cells(&out),
        vec![5, 7, 0, 0],
        "captures streamed inner's 5; then @a=7 (outer RETURN wins), \
             @b=0 (tail's inner EXEC consumed its own status), @c=0"
    );
    let _ = std::fs::remove_file(path);
}

/// The 217 depth-cap error path unwinds EXEC_DEPTH all the way: a
/// subsequent EXEC in the same session starts at nest level 1 again.
#[test]
fn review_poc_exec_depth_unwinds_after_217() {
    let path = unique_temp_path("proc-depth-unwind");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    for sql in [
        "CREATE PROCEDURE recur2 AS EXEC recur2",
        "CREATE PROCEDURE shallow AS SELECT CAST(@@NESTLEVEL AS INT) AS n",
    ] {
        let out = batch(&engine, &mut ctx, sql);
        assert!(out.error.is_none(), "{sql}: {:?}", out.error);
    }
    let out = batch(&engine, &mut ctx, "EXEC recur2");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(217));
    let out = batch(&engine, &mut ctx, "EXEC shallow");
    assert!(
        out.error.is_none(),
        "depth leaked past the 217: {:?}",
        out.error
    );
    assert_eq!(
        review_first_cells(&out),
        vec![1],
        "@@NESTLEVEL restarts at 1 after the failed recursion"
    );
    let _ = std::fs::remove_file(path);
}

/// A parameter named like a caller variable is a separate slot: the body
/// mutates its own @a; the caller's @a is untouched after the EXEC.
#[test]
fn review_poc_param_scope_isolated_from_caller_variable() {
    let path = unique_temp_path("proc-shadow");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE shadow @a INT AS SET @a = @a + 1; SELECT @a AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "DECLARE @a INT = 1; EXEC shadow @a = @a; SELECT @a AS n",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        review_first_cells(&out),
        vec![2, 1],
        "inside: 2; caller's @a unchanged: 1"
    );
    let _ = std::fs::remove_file(path);
}

/// ERROR_PROCEDURE() precision under nested CATCHes: a second error in
/// the ad-hoc CATCH pushes its own frame (procedure NULL, its own
/// number); when that inner CATCH exits, the outer CATCH's ERROR_*()
/// resolve to the procedure error again.
#[test]
fn review_poc_error_procedure_survives_second_error_in_catch() {
    let path = unique_temp_path("proc-errproc-nested");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE boom2 AS RAISERROR('inside', 16, 1)",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY EXEC boom2; END TRY \
             BEGIN CATCH \
               SELECT ERROR_PROCEDURE() AS p; \
               BEGIN TRY SELECT 1/0 AS d; END TRY \
               BEGIN CATCH SELECT ERROR_PROCEDURE() AS p; SELECT ERROR_NUMBER() AS n; END CATCH; \
               SELECT ERROR_PROCEDURE() AS p; \
             END CATCH",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let cells: Vec<Datum> = out
        .results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rowset) => Some(rowset.rows[0][0].clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        cells,
        vec![
            Datum::NVarChar("boom2".into()),
            Datum::Null,
            Datum::BigInt(8134),
            Datum::NVarChar("boom2".into()),
        ],
        "outer: boom2; inner: NULL + 8134; outer again: boom2"
    );
    let _ = std::fs::remove_file(path);
}

/// DROP TABLE of a procedure must be refused (SQL Server 3701: the
/// procedure namespace is not the table namespace), exactly as DROP TABLE
/// of a view already is. The current arm only guards views, so DROP TABLE
/// silently destroys a procedure.
#[test]
fn review_poc_drop_table_does_not_drop_a_procedure() {
    let path = unique_temp_path("proc-drop-table");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "CREATE PROCEDURE keepp AS SELECT 1 AS n");
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "DROP TABLE keepp");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(3701),
        "DROP TABLE of a procedure must fail, got {:?}",
        out.error
    );
    let out = batch(&engine, &mut ctx, "EXEC keepp");
    assert!(
        out.error.is_none(),
        "the procedure survived the wrong-type DROP: {:?}",
        out.error
    );
    let _ = std::fs::remove_file(path);
}

/// DML against a procedure name errors cleanly (the object is not a
/// table); none of these may succeed or panic.
#[test]
fn review_poc_dml_on_a_procedure_name_errors_cleanly() {
    let path = unique_temp_path("proc-dml");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "CREATE PROCEDURE nott AS SELECT 1 AS n");
    assert!(out.error.is_none(), "{:?}", out.error);
    // Observed today: SELECT * silently streams an EMPTY rowset and
    // DELETE silently reports 0 rows affected; only INSERT (110) and
    // UPDATE (207) error, for incidental column reasons.
    for sql in [
        "SELECT * FROM nott",
        "INSERT INTO nott VALUES (1)",
        "UPDATE nott SET x = 1",
        "DELETE FROM nott",
    ] {
        let out = batch(&engine, &mut ctx, sql);
        assert!(
            out.error.is_some(),
            "{sql}: must error (a procedure is not a table), streamed {:?}",
            out.results
        );
    }
    let _ = std::fs::remove_file(path);
}

/// SQL Server requires procedure parameter defaults to be CONSTANTS
/// (literals or NULL). The current code stores the default's source text
/// and evaluates it at EXEC against the CALLER's scope — so `@b INT = @a`
/// captures whatever @a happens to be in each caller, and even a niladic
/// function default drifts per call.
#[test]
fn review_poc_non_constant_parameter_default_rejected_at_create() {
    let path = unique_temp_path("proc-nonconst-default");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE PROCEDURE dflt @b INT = @a AS SELECT @b AS n",
    );
    assert!(
        out.error.is_some(),
        "a variable-referencing parameter default must be rejected at \
             CREATE (SQL Server: defaults are constants)"
    );
    let _ = std::fs::remove_file(path);
}

/// The stored body text round-trips exactly through sys.sql_modules:
/// embedded quotes, a line comment, a newline, a trailing statement.
#[test]
fn review_poc_body_text_round_trips_through_sys_sql_modules() {
    let path = unique_temp_path("proc-body-roundtrip");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let body = "SELECT 'it''s' AS s -- trailing comment\n; SELECT 2 AS n";
    let out = batch(
        &engine,
        &mut ctx,
        &format!("CREATE PROCEDURE qbody AS {body}"),
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(
        &engine,
        &mut ctx,
        "SELECT m.definition FROM sys.sql_modules m \
             JOIN sys.procedures p ON m.object_id = p.object_id \
             WHERE p.name = 'qbody'",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let StatementResult::Rows(rowset) = &out.results[0] else {
        panic!("expected rows, got {:?}", out.results);
    };
    assert_eq!(
        rowset.rows[0][0],
        Datum::NVarChar(body.into()),
        "the definition is the verbatim body text"
    );
    // And the stored text still executes: both rowsets stream.
    let out = batch(&engine, &mut ctx, "EXEC qbody");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        out.results
            .iter()
            .filter(|r| matches!(r, StatementResult::Rows(_)))
            .count(),
        2,
        "both body statements ran: {:?}",
        out.results
    );
    let _ = std::fs::remove_file(path);
}

/// CREATE PROCEDURE as dynamic SQL: legal (it is the first statement of
/// the inner batch, as SQL Server requires), and the DDL-in-transaction
/// gate still applies through the dynamic path.
#[test]
fn review_poc_create_procedure_inside_dynamic_sql() {
    let path = unique_temp_path("proc-dyn-create");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "EXEC sp_executesql N'CREATE PROCEDURE dynp AS SELECT 42 AS n'",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "EXEC dynp");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(review_first_cells(&out), vec![42]);
    // The DDL-in-txn gate is not bypassed by the dynamic path.
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRAN; \
             EXEC sp_executesql N'CREATE PROCEDURE dynp2 AS SELECT 1 AS n';",
    );
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(226),
        "DDL inside an explicit transaction stays refused via dynamic SQL"
    );
    batch(&engine, &mut ctx, "IF @@TRANCOUNT > 0 ROLLBACK");
    let out = batch(&engine, &mut ctx, "EXEC dynp2");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(2812),
        "dynp2 was never created"
    );
    let _ = std::fs::remove_file(path);
}

/// DROP PROCEDURE IF EXISTS of a TABLE name is a silent no-op (the
/// procedure namespace has no such object, IF EXISTS suppresses the
/// miss) and the table survives; without IF EXISTS it is 3701.
#[test]
fn review_poc_drop_procedure_if_exists_of_a_table_is_a_noop() {
    let path = unique_temp_path("proc-die-table");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE keept (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO keept VALUES (1)");
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE IF EXISTS keept");
    assert!(out.error.is_none(), "{:?}", out.error);
    let out = batch(&engine, &mut ctx, "SELECT id FROM keept");
    assert_eq!(ids(&out), vec![1], "the table survived");
    let out = batch(&engine, &mut ctx, "DROP PROCEDURE keept");
    assert_eq!(out.error.as_ref().map(|e| e.number), Some(3701));
    let _ = std::fs::remove_file(path);
}

#[test]
fn try_catch_nested_inner_handles_outer_continues() {
    // The inner CATCH handles the inner error; because it does not re-raise,
    // the outer TRY continues and the outer CATCH never runs.
    let path = unique_temp_path("try-catch-nested");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY)",
    );
    batch(&engine, &mut ctx, "INSERT INTO t VALUES (1)");
    let out = batch(
        &engine,
        &mut ctx,
        "BEGIN TRY \
               BEGIN TRY INSERT INTO t VALUES (1); END TRY \
               BEGIN CATCH SELECT ERROR_NUMBER() AS n; END CATCH; \
               SELECT 777 AS n; \
             END TRY \
             BEGIN CATCH SELECT 999 AS n; END CATCH",
    );
    assert!(out.error.is_none());
    assert_eq!(
        all_int_rows(&out),
        vec![vec![2627], vec![777]],
        "inner CATCH ran (2627), outer TRY continued (777), outer CATCH skipped"
    );
    let _ = std::fs::remove_file(path);
}
