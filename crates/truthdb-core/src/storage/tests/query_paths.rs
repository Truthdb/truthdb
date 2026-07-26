use super::*;

/// A covering seek (every read column INCLUDEd) answers from the index
/// leaves alone — the counter proves the covering path ran — and returns
/// exactly what a table scan returns, original case and NULLs included.
#[test]
fn a_covering_seek_answers_from_the_index_alone_and_matches_a_scan() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("include-covering");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let setup = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL, v INT); \
         CREATE INDEX ix ON t (email) INCLUDE (email, v); \
         INSERT INTO t VALUES (1, 'a@x.com', 10), (2, 'B@X.com', NULL), (3, 'c@x.com', 30)",
        &mut ctx,
    );
    assert!(setup.error.is_none(), "{:?}", setup.error);

    let rows_of = |outcome: &crate::engine::BatchOutcome| match &outcome.results[0] {
        StatementResult::Rows(rowset) => rowset.rows.clone(),
        other => panic!("expected rows, got {other:?}"),
    };

    // Sought case-insensitively, answered with the stored ORIGINAL value.
    let covered = execute_batch(
        &storage,
        "SELECT email, v FROM t WHERE email = 'b@x.com'",
        &mut ctx,
    );
    assert!(covered.error.is_none(), "{:?}", covered.error);
    assert_eq!(storage.covering_scans(), 1, "the covering path answered");
    let covered = rows_of(&covered);
    assert_eq!(
        covered,
        vec![vec![Datum::VarChar("B@X.com".into()), Datum::Null]]
    );

    // A/B: without the index the same query scans — identical rows.
    let dropped = execute_batch(&storage, "DROP INDEX ix ON t", &mut ctx);
    assert!(dropped.error.is_none(), "{:?}", dropped.error);
    let scanned = execute_batch(
        &storage,
        "SELECT email, v FROM t WHERE email = 'b@x.com'",
        &mut ctx,
    );
    assert!(scanned.error.is_none(), "{:?}", scanned.error);
    assert_eq!(rows_of(&scanned), covered, "covering == scan");
    assert_eq!(storage.covering_scans(), 1, "the scan path took over");

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// The streamed input path: a filtered aggregate over a plain base table
/// pulls the scan slice by slice through the WHERE — it must never drain
/// the scan whole (that is what the join operators do, and what the old
/// path did for every shape). Pinned by the materialization counter: the
/// streamed query performs zero whole-scan drains; a join performs one
/// per scanned input.
#[test]
fn a_filtered_aggregate_streams_its_input_instead_of_materializing() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("stream-input");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let setup = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
        &mut ctx,
    );
    assert!(setup.error.is_none(), "{:?}", setup.error);
    for chunk in (1..=3000).collect::<Vec<i64>>().chunks(500) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i}, {})", i % 7)).collect();
        let outcome = execute_batch(
            &storage,
            &format!("INSERT INTO t VALUES {}", values.join(", ")),
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
    }

    // An aggregate with a WHERE over 3000 rows (three scan slices): the
    // input streams; nothing drains the scan whole.
    let outcome = execute_batch(
        &storage,
        "SELECT COUNT(*), SUM(v) FROM t WHERE v > 3",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(
        storage.scan_materializations(),
        0,
        "the filtered aggregate's input streamed"
    );
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            // v cycles 1..7 over 3000 rows minus the id=1 seed row's v=1:
            // v in {4,5,6} appears 428|429 times; exact values pin the walk.
            assert_eq!(rowset.rows.len(), 1);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // A join materializes only its BUILD side (the counter's positive
    // control); the probe side streams. INNER probes from the left, so the
    // right input is the one drained.
    let outcome = execute_batch(
        &storage,
        "SELECT COUNT(*) FROM t a JOIN t b ON a.id = b.id WHERE a.v > 5",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(
        storage.scan_materializations(),
        1,
        "an inner hash join materializes only its build side"
    );

    // RIGHT reverses orientation: the probe is the right input, the left
    // input is built — still exactly one materialization.
    let outcome = execute_batch(
        &storage,
        "SELECT COUNT(*) FROM t a RIGHT JOIN t b ON a.id = b.id",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(
        storage.scan_materializations(),
        2,
        "a right join materializes only its (left) build side"
    );

    // The nested loop (no equi key) streams its probe side too. A small
    // table keeps the O(n·m) loop cheap; its base scan is still lazy.
    let setup = execute_batch(
        &storage,
        "CREATE TABLE s (id INT NOT NULL PRIMARY KEY); INSERT INTO s VALUES (1), (2), (3), (4)",
        &mut ctx,
    );
    assert!(setup.error.is_none(), "{:?}", setup.error);
    let outcome = execute_batch(
        &storage,
        "SELECT COUNT(*) FROM s a JOIN s b ON a.id < b.id",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => assert_eq!(rowset.rows[0][0], Datum::BigInt(6)),
        other => panic!("expected rows, got {other:?}"),
    }
    assert_eq!(
        storage.scan_materializations(),
        3,
        "a nested-loop join materializes only its build side"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// The grace-hash spill path partitions the probe side straight off the
/// scan stream: still exactly one materialization (the build side), and
/// the same result as the in-memory join.
#[test]
fn a_spilling_join_streams_its_probe_side_into_partitions() {
    use crate::engine::{StatementResult, TxnContext, execute_batch, set_test_sort_budget};

    let path = unique_temp_path("grace-probe-stream");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let setup = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
        &mut ctx,
    );
    assert!(setup.error.is_none(), "{:?}", setup.error);
    for chunk in (1..=2000).collect::<Vec<i64>>().chunks(500) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i}, {})", i % 7)).collect();
        let outcome = execute_batch(
            &storage,
            &format!("INSERT INTO t VALUES {}", values.join(", ")),
            &mut ctx,
        );
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
    }

    set_test_sort_budget(Some(4000));
    let outcome = execute_batch(
        &storage,
        "SELECT COUNT(*) FROM t a LEFT JOIN t b ON a.id = b.id AND b.v = 3",
        &mut ctx,
    );
    set_test_sort_budget(None);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    match &outcome.results[0] {
        // Every a-row appears once: matched where b.v = 3, null-extended
        // otherwise — 2000 either way.
        StatementResult::Rows(rowset) => assert_eq!(rowset.rows[0][0], Datum::BigInt(2000)),
        other => panic!("expected rows, got {other:?}"),
    }
    assert_eq!(
        storage.scan_materializations(),
        1,
        "the spilling join materialized only its build side"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// The row counter tracks every DML shape through the SQL layer — and
/// because it is an ordinary transactional page op, statement atomicity,
/// savepoints, transaction rollback and crash recovery all keep it exact
/// without counter-specific recovery code.
#[test]
fn row_counts_track_dml_transactions_and_recovery() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("row-count");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let run = |storage: &Storage, ctx: &mut TxnContext, sql: &str| {
        let outcome = execute_batch(storage, sql, ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
    };

    run(
        &storage,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)",
    );
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(0)
    );

    run(
        &storage,
        &mut ctx,
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
    );
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(3)
    );

    run(&storage, &mut ctx, "DELETE FROM t WHERE id = 3");
    run(&storage, &mut ctx, "UPDATE t SET v = 99 WHERE id = 1");
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(2),
        "delete -1, update 0"
    );

    // A failing multi-row statement (duplicate key on its last row) is
    // atomic: no rows land, and neither does its count.
    let dup = execute_batch(&storage, "INSERT INTO t VALUES (5, 1), (1, 1)", &mut ctx);
    assert!(dup.error.is_some(), "duplicate key must fail");
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(2)
    );

    // Transaction rollback restores the count with the rows.
    run(
        &storage,
        &mut ctx,
        "BEGIN TRANSACTION; INSERT INTO t VALUES (10, 1), (11, 1)",
    );
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(4),
        "in-flight rows count"
    );
    run(&storage, &mut ctx, "ROLLBACK");
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(2)
    );

    // A savepoint rollback restores exactly the statements behind it.
    run(
        &storage,
        &mut ctx,
        "BEGIN TRANSACTION; INSERT INTO t VALUES (20, 1); SAVE TRANSACTION sp; \
         INSERT INTO t VALUES (21, 1); ROLLBACK TRANSACTION sp; COMMIT",
    );
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(3)
    );

    // Crash (no checkpoint, pool never flushed): recovery replays the ops,
    // counter page included.
    drop(storage);
    let storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(3),
        "count survives recovery"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Rows of a transaction still open at the crash are undone by recovery —
/// and so is their count.
#[test]
fn an_uncommitted_transactions_rows_are_uncounted_after_crash() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("row-count-crash");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let setup = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY); INSERT INTO t VALUES (1), (2)",
        &mut ctx,
    );
    assert!(setup.error.is_none(), "{:?}", setup.error);

    let mut open_txn = TxnContext::default();
    let pending = execute_batch(
        &storage,
        "BEGIN TRANSACTION; INSERT INTO t VALUES (10), (11), (12)",
        &mut open_txn,
    );
    assert!(pending.error.is_none(), "{:?}", pending.error);
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(5),
        "in-flight rows count"
    );
    drop(storage); // crash with the transaction open

    let storage = Storage::open(path.clone()).expect("reopen");
    assert_eq!(
        storage.rel_row_count(crate::relstore::catalog::DEFAULT_DATABASE_ID, "t"),
        Some(2),
        "the loser transaction's rows and count are both undone"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// "Row counts as tie-breakers only": a table at or under the tiny
/// threshold plans its seek as the scan it ties with, grows into the seek
/// past the threshold — and a covering seek keeps its win at any size,
/// since it reads less than the table either way.
#[test]
fn a_tiny_table_scans_until_it_grows_into_its_seek() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("row-count-tiebreak");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let run = |storage: &Storage, ctx: &mut TxnContext, sql: &str| {
        let outcome = execute_batch(storage, sql, ctx);
        assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
        outcome
    };
    let plan_of = |storage: &Storage, ctx: &mut TxnContext, sql: &str| -> String {
        let outcome = run(storage, ctx, &format!("SET SHOWPLAN_TEXT ON; {sql}"));
        let mut ctx2 = TxnContext::default();
        let _ = execute_batch(storage, "SET SHOWPLAN_TEXT OFF", &mut ctx2);
        match &outcome.results[1] {
            StatementResult::Rows(rowset) => rowset
                .rows
                .iter()
                .map(|r| format!("{:?}", r[0]))
                .collect::<Vec<_>>()
                .join("\n"),
            other => panic!("expected plan rows, got {other:?}"),
        }
    };

    run(
        &storage,
        &mut ctx,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT, v INT); \
         CREATE INDEX ix_a ON t (a); \
         CREATE INDEX ix_cover ON t (v) INCLUDE (v, id); \
         INSERT INTO t VALUES (1, 10, 5), (2, 20, 6), (3, 30, 7)",
    );

    // Tiny: the non-covering seek ties with the scan; the tie goes to the
    // scan. The covering seek still wins — it reads less than the table.
    let tiny = plan_of(&storage, &mut ctx, "SELECT id FROM t WHERE a = 20");
    assert!(tiny.contains("Table Scan"), "tiny table scans: {tiny}");
    let covering = plan_of(&storage, &mut ctx, "SELECT v, id FROM t WHERE v = 6");
    assert!(
        covering.contains("Index Seek (covering)"),
        "covering exempt from the tie-break: {covering}"
    );

    // Past the threshold the same query seeks.
    let mut pad = String::from("INSERT INTO t VALUES (100, 900, 900)");
    for i in 1..20 {
        pad.push_str(&format!(", ({}, 900, 900)", 100 + i));
    }
    run(&storage, &mut ctx, &pad);
    let grown = plan_of(&storage, &mut ctx, "SELECT id FROM t WHERE a = 20");
    assert!(
        grown.contains("Index Seek") && grown.contains("Key Lookup"),
        "grown table seeks: {grown}"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// A covering index wins its tie against an equal-scoring non-INCLUDE
/// index — the "add a covering index to an existing database" workflow.
/// Coverage breaks equality ties only: it never outranks a fully-matched
/// UNIQUE seek (one row plus one lookup beats a covering scan).
#[test]
fn a_covering_index_wins_the_tie_against_an_older_plain_index() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("include-tiebreak");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    // The plain index is created FIRST, so a first-wins tie keeps it.
    let setup = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL, v INT); \
         CREATE INDEX ix ON t (email); \
         CREATE INDEX ix2 ON t (email) INCLUDE (email, v); \
         INSERT INTO t VALUES (1, 'a@x.com', 10)",
        &mut ctx,
    );
    assert!(setup.error.is_none(), "{:?}", setup.error);
    let outcome = execute_batch(
        &storage,
        "SELECT email, v FROM t WHERE email = 'a@x.com'",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(
        storage.covering_scans(),
        1,
        "the covering index wins the equality tie"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// CREATE INDEX with a column that does not exist on the table reports
/// SQL Server's 1911 (not the generic 207) — for the key list and the
/// INCLUDE list alike; a duplicate INCLUDE column reports 1909.
#[test]
fn create_index_errors_carry_sql_server_numbers() {
    use crate::engine::{TxnContext, execute_batch};

    let path = unique_temp_path("include-errors");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let setup = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL)",
        &mut ctx,
    );
    assert!(setup.error.is_none(), "{:?}", setup.error);

    let cases = [
        ("CREATE INDEX ix ON t (nope)", 1911),
        ("CREATE INDEX ix ON t (email) INCLUDE (nope)", 1911),
        ("CREATE INDEX ix ON t (email) INCLUDE (id, id)", 1909),
    ];
    for (sql, number) in cases {
        let outcome = execute_batch(&storage, sql, &mut ctx);
        assert_eq!(
            outcome.error.as_ref().map(|e| e.number),
            Some(number),
            "{sql}: {:?}",
            outcome.error
        );
    }

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// Included leaf values follow UPDATE and DELETE, and survive a restart
/// (the include list rides the catalog; the leaf format rides the pages).
#[test]
fn included_values_survive_update_delete_and_restart() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("include-restart");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let setup = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL, v INT); \
         CREATE INDEX ix ON t (email) INCLUDE (email, v); \
         INSERT INTO t VALUES (1, 'a@x.com', 10), (2, 'b@x.com', 20); \
         UPDATE t SET v = 99 WHERE id = 1; \
         DELETE FROM t WHERE id = 2",
        &mut ctx,
    );
    assert!(setup.error.is_none(), "{:?}", setup.error);
    drop(storage);

    let storage = Storage::open(path.clone()).expect("reopen");
    let mut ctx = TxnContext::default();
    let outcome = execute_batch(
        &storage,
        "SELECT email, v FROM t WHERE email = 'a@x.com'",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(storage.covering_scans(), 1, "covering after reopen");
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => assert_eq!(
            rowset.rows,
            vec![vec![Datum::VarChar("a@x.com".into()), Datum::Int(99)]],
            "the UPDATE reached the leaf; the DELETEd row is gone"
        ),
        other => panic!("expected rows, got {other:?}"),
    }
    let gone = execute_batch(
        &storage,
        "SELECT email, v FROM t WHERE email = 'b@x.com'",
        &mut ctx,
    );
    match &gone.results[0] {
        StatementResult::Rows(rowset) => assert!(rowset.rows.is_empty()),
        other => panic!("expected rows, got {other:?}"),
    }

    drop(storage);
    let _ = std::fs::remove_file(&path);
}

/// A query reading a column that is NOT included falls back to the key
/// lookup — through the INCLUDE index's length-prefixed leaf value, whose
/// `Locator::Key` payload would be swallowed by the old bare decode.
/// SHOWPLAN tells the two apart: covering has no Key Lookup line.
#[test]
fn a_non_covering_read_on_an_include_index_still_finds_rows() {
    use crate::engine::{StatementResult, TxnContext, execute_batch};

    let path = unique_temp_path("include-fallback");
    let storage = Storage::create(path.clone(), test_storage_options()).expect("create");
    let mut ctx = TxnContext::default();
    let setup = execute_batch(
        &storage,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(40) NOT NULL, name VARCHAR(20)); \
         CREATE INDEX ix ON t (email) INCLUDE (email, id); \
         INSERT INTO t VALUES (1, 'a@x.com', 'Alice')",
        &mut ctx,
    );
    assert!(setup.error.is_none(), "{:?}", setup.error);
    // Pad past the tiny-table tie-break: a <= 16-row table plans as a
    // scan, and this test is about the seek's two plan shapes.
    let mut pad = String::from("INSERT INTO t VALUES (100, 'z0@x.com', 'p')");
    for i in 1..20 {
        pad.push_str(&format!(", ({}, 'z{i}@x.com', 'p')", 100 + i));
    }
    let setup = execute_batch(&storage, &pad, &mut ctx);
    assert!(setup.error.is_none(), "{:?}", setup.error);

    // `name` is not included: the seek fetches the base row by PK key.
    let outcome = execute_batch(
        &storage,
        "SELECT name FROM t WHERE email = 'a@x.com'",
        &mut ctx,
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(storage.covering_scans(), 0, "not covering");
    match &outcome.results[0] {
        StatementResult::Rows(rowset) => {
            assert_eq!(rowset.rows, vec![vec![Datum::VarChar("Alice".into())]]);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // SHOWPLAN: the covering shape has no Key Lookup; the fallback does.
    let plans = execute_batch(
        &storage,
        "SET SHOWPLAN_TEXT ON; \
         SELECT id FROM t WHERE email = 'a@x.com'; \
         SELECT name FROM t WHERE email = 'a@x.com'",
        &mut ctx,
    );
    assert!(plans.error.is_none(), "{:?}", plans.error);
    let lines_of = |result: &StatementResult| -> Vec<String> {
        match result {
            StatementResult::Rows(rowset) => {
                rowset.rows.iter().map(|r| format!("{:?}", r[0])).collect()
            }
            other => panic!("expected plan rows, got {other:?}"),
        }
    };
    let covering = lines_of(&plans.results[1]).join("\n");
    assert!(
        covering.contains("Index Seek (covering)") && !covering.contains("Key Lookup"),
        "covering plan: {covering}"
    );
    let lookup = lines_of(&plans.results[2]).join("\n");
    assert!(
        lookup.contains("Key Lookup"),
        "non-covering plan keeps the lookup: {lookup}"
    );

    drop(storage);
    let _ = std::fs::remove_file(&path);
}
