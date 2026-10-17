use super::*;

// ---- Stage 6: the row-at-a-time single-table scan ------------------------

/// The first rowset in an outcome.
fn first_rowset(outcome: &BatchOutcome) -> &crate::engine::RowSet {
    for result in &outcome.results {
        if let StatementResult::Rows(rowset) = result {
            return rowset;
        }
    }
    panic!("no rowset in outcome: {:?}", outcome.results);
}

#[test]
fn the_scan_path_returns_exactly_what_the_collecting_path_returns() {
    // The whole compatibility claim of the row-at-a-time path is that a
    // caller cannot tell it apart, so the oracle is the collecting path
    // itself: every shape the gate accepts must produce the identical
    // RowSet — same columns, same types, same rows, same order — through
    // both. `without_scan_path` makes the gate decline, which is the only
    // difference between the two runs.
    let path = unique_temp_path("scan-ab");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();

    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE ab (id INT PRIMARY KEY, v INT, s VARCHAR(20), n INT NULL)",
    );
    for i in 0..300 {
        let s = format!("row{i}");
        let n = if i % 3 == 0 {
            "NULL".to_string()
        } else {
            i.to_string()
        };
        batch(
            &engine,
            &mut ctx,
            &format!("INSERT INTO ab VALUES ({i}, {}, '{s}', {n})", i * 2),
        );
    }
    // A heap (no PK) as well, since the two take different cursors.
    batch(&engine, &mut ctx, "CREATE TABLE hp (id INT, v INT)");
    for i in 0..300 {
        batch(
            &engine,
            &mut ctx,
            &format!("INSERT INTO hp VALUES ({i}, {i})"),
        );
    }
    // A secondary index, so the seek access path is compared as well as the
    // scan — `plan::choose` only considers secondary indexes, so a PK
    // equality is not a seek here.
    batch(&engine, &mut ctx, "CREATE INDEX ix_ab_v ON ab (v)");
    // A wide table, where projection pruning has something to prune.
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE wd (id INT PRIMARY KEY, a VARCHAR(20), b VARCHAR(20), c INT, d VARCHAR(20), e INT NULL)",
    );
    for i in 0..200 {
        batch(
            &engine,
            &mut ctx,
            &format!(
                "INSERT INTO wd VALUES ({i}, 'a{i}', 'b{i}', {i}, 'd{i}', {})",
                if i % 4 == 0 {
                    "NULL".into()
                } else {
                    i.to_string()
                }
            ),
        );
    }

    let queries = [
        // Bare columns, wildcards, aliases, qualified names.
        "SELECT * FROM ab",
        "SELECT id FROM ab",
        "SELECT v, id FROM ab",
        "SELECT ab.* FROM ab",
        "SELECT a.* FROM ab a",
        "SELECT id AS ident, v AS value FROM ab",
        "SELECT a.v FROM ab a",
        "SELECT a.v AS vv FROM ab a",
        "SELECT * FROM ab a",
        // The projection may repeat and reorder columns.
        "SELECT v, v, id, s FROM ab",
        // WHERE, including NULL/3VL and a non-sargable predicate on a PK.
        "SELECT id FROM ab WHERE v > 100",
        "SELECT id FROM ab WHERE n IS NULL",
        "SELECT id FROM ab WHERE n > 50",
        "SELECT id FROM ab WHERE s = 'ROW7'",
        "SELECT id FROM ab WHERE id + 0 > 297",
        "SELECT id FROM ab WHERE v > 100 AND n IS NOT NULL",
        "SELECT id FROM ab WHERE 1 = 0",
        // TOP, with and without a filter, at and past the row count.
        "SELECT TOP 5 id FROM ab",
        "SELECT TOP 1 id FROM ab",
        "SELECT TOP 5 id FROM ab WHERE v > 100",
        "SELECT TOP 1000 id FROM ab",
        "SELECT TOP 5 * FROM ab",
        // The seek access path: `v` is indexed, so these choose IndexSeek
        // and its candidates are re-filtered and projected the same way.
        "SELECT id FROM ab WHERE v = 100",
        "SELECT id, v FROM ab WHERE v = 100",
        "SELECT * FROM ab WHERE v > 500",
        "SELECT * FROM ab WHERE v >= 100 AND v <= 200",
        "SELECT TOP 3 id FROM ab WHERE v > 100",
        "SELECT id FROM ab WHERE v = 99999",
        // Projection pruning: the scan decodes only what the query reads,
        // so a WHERE on a column that is *not* projected must still keep
        // that column — these are the shapes that catch a pruned-away
        // predicate column.
        "SELECT id FROM wd",
        "SELECT id FROM wd WHERE a = 'a7'",
        "SELECT a FROM wd WHERE c > 100",
        "SELECT id, d FROM wd WHERE b = 'b3' AND e IS NULL",
        "SELECT e FROM wd WHERE e IS NOT NULL",
        "SELECT id FROM wd WHERE CASE WHEN c > 100 THEN a ELSE d END = 'a150'",
        "SELECT id FROM wd WHERE a LIKE 'a1%'",
        "SELECT id FROM wd WHERE c IN (1, 2, 3)",
        "SELECT id FROM wd WHERE c BETWEEN 10 AND 12",
        "SELECT id FROM wd WHERE LEN(a) > 3",
        "SELECT d, c, a FROM wd WHERE id = 5",
        "SELECT * FROM wd WHERE c = 5",
        // The heap: 300 rows is inside one 1024-row slice either way.
        "SELECT id FROM hp",
        "SELECT TOP 3 id FROM hp",
        "SELECT id FROM hp WHERE v > 290",
    ];

    for query in queries {
        // Both guards are needed, and for the same reason: an A/B whose two
        // sides run the same code agrees with itself. The first proves the
        // scan path ran; the second proves `without_scan_path` really took
        // it away, which nothing else here would notice if it stopped
        // working.
        let before = engine.storage.scan_selects();
        let streamed = batch(&engine, &mut ctx, query);
        assert_eq!(
            engine.storage.scan_selects(),
            before + 1,
            "{query} did not take the scan path, so comparing it proves nothing"
        );
        let before = engine.storage.scan_selects();
        let collected = crate::engine::without_scan_path(|| batch(&engine, &mut ctx, query));
        assert_eq!(
            engine.storage.scan_selects(),
            before,
            "{query} took the scan path for both runs, so it was compared with itself"
        );
        assert!(streamed.error.is_none(), "{query}: {:?}", streamed.error);
        assert!(collected.error.is_none(), "{query}: {:?}", collected.error);
        assert_eq!(
            first_rowset(&streamed),
            first_rowset(&collected),
            "{query} differs between the scan path and the collecting path"
        );
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn top_stops_the_scan_rather_than_reading_the_table_and_truncating() {
    // The collecting path reads every row, then truncates. Counting slices
    // is what tells the two apart: the rows returned are identical either
    // way, so a result-only assertion would pass without the early exit.
    let path = unique_temp_path("scan-top");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE big (id INT PRIMARY KEY, v INT)",
    );
    // Several slices' worth (SCAN_SLICE_ROWS is 1024), so "stopped early"
    // and "read it all" differ by more than rounding.
    for i in 0..3000 {
        batch(
            &engine,
            &mut ctx,
            &format!("INSERT INTO big VALUES ({i}, {i})"),
        );
    }

    let before = engine.storage.scan_slices();
    let out = batch(&engine, &mut ctx, "SELECT TOP 1 id FROM big");
    let slices = engine.storage.scan_slices() - before;
    assert_eq!(first_rowset(&out).rows.len(), 1, "TOP 1 returns one row");
    assert_eq!(
        slices, 1,
        "TOP 1 must read one slice, not walk the whole table"
    );

    // The counter means what the assertions above assume: an unlimited scan
    // of the same table reads every slice. Without this the two could pass
    // against a scan that never ran.
    let before = engine.storage.scan_slices();
    let out = batch(&engine, &mut ctx, "SELECT id FROM big");
    assert_eq!(first_rowset(&out).rows.len(), 3000);
    assert!(
        engine.storage.scan_slices() - before >= 3,
        "3000 rows at 1024 per slice is at least three slices"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_scan_decodes_only_the_columns_the_query_reads() {
    // The rows returned are identical whether or not the projection is
    // pruned, so nothing about the result can see this — the width the scan
    // asked for is the only observable.
    let path = unique_temp_path("scan-prune");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE w (id INT PRIMARY KEY, a VARCHAR(20), b VARCHAR(20), c INT)",
    );
    for i in 0..50 {
        batch(
            &engine,
            &mut ctx,
            &format!("INSERT INTO w VALUES ({i}, 'a{i}', 'b{i}', {i})"),
        );
    }

    for (query, expected, why) in [
        ("SELECT id FROM w", 1, "one projected column"),
        ("SELECT id, c FROM w", 2, "two projected columns"),
        ("SELECT * FROM w", 4, "a wildcard needs every column"),
        // The WHERE's columns are read even when nothing projects them.
        (
            "SELECT id FROM w WHERE a = 'a1'",
            2,
            "id + the predicate's a",
        ),
        (
            "SELECT id FROM w WHERE a = 'a1' AND c > 2",
            3,
            "id + both predicate columns",
        ),
        // A column named twice costs one decode.
        ("SELECT id, id FROM w WHERE id > 1", 1, "id, deduped"),
        // A predicate column that is also projected is not counted twice.
        ("SELECT a FROM w WHERE a = 'a1'", 1, "a, deduped"),
    ] {
        let out = batch(&engine, &mut ctx, query);
        assert!(out.error.is_none(), "{query}: {:?}", out.error);
        assert_eq!(
            engine.storage.last_scan_width(),
            expected,
            "{query} should decode {expected} columns ({why})"
        );
    }

    // The counter reports the whole row when nothing prunes, so the numbers
    // above are a pruned width and not a stuck reading.
    crate::engine::without_scan_path(|| batch(&engine, &mut ctx, "SELECT id FROM w"));
    assert_eq!(
        engine.storage.last_scan_width(),
        usize::MAX,
        "the collecting path decodes the whole row"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn pruning_carries_each_kept_column_its_own_type_and_collation() {
    // `needed` renumbers the columns, so `types` and `collations` are
    // rebuilt in the scanned row's coordinates. Both are silent when wrong:
    // a misindexed type mis-restores a DECIMAL's scale, and a misindexed
    // collation makes a _CS column compare case-insensitively. The columns
    // that matter sit at HIGH schema indices and are projected/filtered from
    // LOW ones, so a rebuild that kept the schema's numbering reads the
    // wrong entry rather than coincidentally the right one.
    let path = unique_temp_path("scan-prune-types");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE tc (id INT PRIMARY KEY, pad1 VARCHAR(10), pad2 VARCHAR(10), \
             amount DECIMAL(10,4), cs VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CS_AS, \
             ci VARCHAR(20))",
    );
    batch(
        &engine,
        &mut ctx,
        "INSERT INTO tc VALUES (1, 'p', 'q', 12.3456, 'Match', 'Match')",
    );
    batch(
        &engine,
        &mut ctx,
        "INSERT INTO tc VALUES (2, 'p', 'q', 0.5000, 'other', 'other')",
    );

    // A _CS column is exact; a default (_CI) one is not. Both are read via
    // the WHERE only, so both live at a remapped position.
    let out = batch(&engine, &mut ctx, "SELECT id FROM tc WHERE cs = 'MATCH'");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(
        first_rowset(&out).rows.is_empty(),
        "a _CS column must not match a different casing"
    );
    let out = batch(&engine, &mut ctx, "SELECT id FROM tc WHERE cs = 'Match'");
    assert_eq!(
        first_rowset(&out).rows.len(),
        1,
        "_CS matches its own casing"
    );
    let out = batch(&engine, &mut ctx, "SELECT id FROM tc WHERE ci = 'MATCH'");
    assert_eq!(
        first_rowset(&out).rows.len(),
        1,
        "the default collation is case-insensitive"
    );

    // The DECIMAL's scale survives the `types` remap. It has to be read
    // through the WHERE to test that: `datum_to_sql` consults the column
    // type for a DECIMAL's precision/scale and for nothing else, so this is
    // the only shape a misindexed `types` is visible in. (Asserting the
    // *output* column's type would prove nothing — that comes from
    // `plan.columns`, which is not remapped.) Read at position 1 of
    // [id, amount] while the schema puts it at 3, so a rebuild that kept the
    // schema's numbering finds `pad1` and falls back to scale 0 — turning
    // 12.3456 into 123456.
    let out = batch(
        &engine,
        &mut ctx,
        "SELECT id FROM tc WHERE amount = 12.3456",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        first_rowset(&out).rows.len(),
        1,
        "a DECIMAL in the WHERE keeps its scale through the remap"
    );
    let out = batch(&engine, &mut ctx, "SELECT amount FROM tc WHERE id = 1");
    assert_eq!(
        first_rowset(&out).columns[0].column_type,
        crate::relstore::types::ColumnType::Decimal {
            precision: 10,
            scale: 4
        },
        "the projected column keeps its schema type"
    );

    // And every one of these agrees with the collecting path, which reads
    // the whole row and so cannot be remapped wrong.
    for query in [
        "SELECT id FROM tc WHERE cs = 'MATCH'",
        "SELECT id FROM tc WHERE cs = 'Match'",
        "SELECT id FROM tc WHERE ci = 'MATCH'",
        "SELECT amount FROM tc WHERE id = 1",
        "SELECT id FROM tc WHERE amount = 12.3456",
        "SELECT amount, cs FROM tc WHERE ci = 'match'",
    ] {
        let streamed = batch(&engine, &mut ctx, query);
        let collected = crate::engine::without_scan_path(|| batch(&engine, &mut ctx, query));
        assert_eq!(
            first_rowset(&streamed),
            first_rowset(&collected),
            "{query} differs between the pruned scan and the collecting path"
        );
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn top_0_is_left_to_the_collecting_path_so_an_invalid_where_still_errors() {
    // The engine has no separate binding pass: an unresolvable column (207)
    // and a non-boolean predicate (4145) are both raised by *evaluating* the
    // predicate on a row. `TOP 0` wants no rows, so a scan path that honours
    // it evaluates nothing and answers an invalid query with an empty result
    // set instead of rejecting it. The gate declines TOP 0 for that reason.
    let path = unique_temp_path("scan-top0");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE p (id INT PRIMARY KEY, v INT)",
    );
    for i in 1..4 {
        batch(
            &engine,
            &mut ctx,
            &format!("INSERT INTO p VALUES ({i}, {i})"),
        );
    }

    for (query, expected) in [
        ("SELECT TOP 0 id FROM p WHERE bogus = 1", 207),
        ("SELECT TOP 0 id FROM p WHERE id", 4145),
    ] {
        assert_eq!(
            batch(&engine, &mut ctx, query).error.map(|e| e.number),
            Some(expected),
            "{query} must still be rejected, not answered with no rows"
        );
    }
    // The same errors without TOP, so the cases above are about TOP 0 and
    // not about a query that is broken some other way.
    for (query, expected) in [
        ("SELECT id FROM p WHERE bogus = 1", 207),
        ("SELECT id FROM p WHERE id", 4145),
    ] {
        assert_eq!(
            batch(&engine, &mut ctx, query).error.map(|e| e.number),
            Some(expected),
            "{query}"
        );
    }
    // And a valid TOP 0 still answers with an empty result set.
    let out = batch(&engine, &mut ctx, "SELECT TOP 0 id FROM p");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert!(first_rowset(&out).rows.is_empty(), "TOP 0 returns no rows");
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_sargable_where_still_seeks_its_index_instead_of_scanning() {
    // The scan path takes the planner's access path rather than declining a
    // seek — declining would throw away the table definition, the schema and
    // the choice, all of which build_table_source would then recompute. A
    // results-only test cannot see which path ran; the slice counter can.
    let path = unique_temp_path("scan-seek");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE sk (id INT PRIMARY KEY, v INT)",
    );
    for i in 0..2000 {
        batch(
            &engine,
            &mut ctx,
            &format!("INSERT INTO sk VALUES ({i}, {i})"),
        );
    }
    // A secondary index: `plan::choose` only considers those, so the
    // clustered PK is not a seekable path here.
    batch(&engine, &mut ctx, "CREATE INDEX ix_sk_v ON sk (v)");

    let slices = engine.storage.scan_slices();
    let selects = engine.storage.scan_selects();
    let out = batch(&engine, &mut ctx, "SELECT id FROM sk WHERE v = 1500");
    assert_eq!(
        engine.storage.scan_slices() - slices,
        0,
        "an equality on an indexed column must seek, not scan"
    );
    assert_eq!(
        engine.storage.scan_selects() - selects,
        1,
        "and the seek is still answered on the scan path, not handed back"
    );
    assert_eq!(first_rowset(&out).rows.len(), 1, "the seek finds the row");

    // The same column, with the predicate made non-sargable, does scan — so
    // the assertion above is about the plan, not about a dead counter.
    let before = engine.storage.scan_slices();
    let out = batch(&engine, &mut ctx, "SELECT id FROM sk WHERE v + 0 = 1500");
    assert!(
        engine.storage.scan_slices() - before > 0,
        "a non-sargable predicate scans"
    );
    assert_eq!(first_rowset(&out).rows.len(), 1, "and finds the same row");
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_sys_catalog_view_is_not_answered_with_a_user_table_of_the_same_name() {
    // `build_table_source` answers `sys.tables` by its full name *before*
    // any catalog lookup. The gate has to apply that precedence itself: a
    // quoted `[sys.tables]` is a creatable, insertable user table, so a gate
    // that resolved the catalog first would scan it and answer the query
    // with its columns — silently, since both are perfectly good rowsets.
    let path = unique_temp_path("scan-sys");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    let out = batch(
        &engine,
        &mut ctx,
        "CREATE TABLE [sys.tables] (id INT PRIMARY KEY, decoy INT)",
    );
    assert!(out.error.is_none(), "{:?}", out.error);
    batch(&engine, &mut ctx, "INSERT INTO [sys.tables] VALUES (1, 9)");

    let out = batch(&engine, &mut ctx, "SELECT * FROM sys.tables");
    assert!(out.error.is_none(), "{:?}", out.error);
    let columns: Vec<&str> = first_rowset(&out)
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(
        columns,
        vec!["object_id", "name"],
        "sys.tables is the catalog view, not the user table shadowing its name"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_view_is_not_scanned_as_if_it_were_its_own_base_table() {
    // A view's rows come from running its SELECT; its `root_page` is not a
    // table's. Scanning one as a base table would read whatever object that
    // page belongs to.
    let path = unique_temp_path("scan-view");
    let engine = new_engine(&path);
    let mut ctx = TxnContext::default();
    batch(
        &engine,
        &mut ctx,
        "CREATE TABLE vt (id INT PRIMARY KEY, v INT)",
    );
    for i in 0..10 {
        batch(
            &engine,
            &mut ctx,
            &format!("INSERT INTO vt VALUES ({i}, {})", i * 10),
        );
    }
    batch(
        &engine,
        &mut ctx,
        "CREATE VIEW vv AS SELECT id, v FROM vt WHERE v >= 50",
    );

    // `SELECT *` is the query that finds this: a view's TableDef carries no
    // columns, so the wildcard expands to none and the gate would accept a
    // plan that projects nothing — then scan the view's `root_page` of 0,
    // which is the catalog root, not the view.
    let out = batch(&engine, &mut ctx, "SELECT * FROM vv");
    assert!(out.error.is_none(), "{:?}", out.error);
    assert_eq!(
        first_rowset(&out).columns.len(),
        2,
        "the view's columns: {:?}",
        first_rowset(&out).columns
    );
    assert_eq!(first_rowset(&out).rows.len(), 5, "the view's own rows");
    let _ = std::fs::remove_file(path);
}

#[test]
fn fk_enforcement_and_sys_views_stay_inside_the_session_database() {
    // The children-of-parent decision (FK NO ACTION on parent DML, the
    // DROP TABLE 3726 guard) and the sys.* enumerations are all derived
    // per database. A same-named parent/child pair in ANOTHER database
    // must not leak into a default-database session.
    let path = unique_temp_path("multidb-exec-scope");
    let engine = new_engine(&path);
    sql(&engine, "CREATE TABLE p (id INT PRIMARY KEY, v INT)");
    sql(&engine, "INSERT INTO p (id, v) VALUES (1, 10)");

    // Another database with its own p, plus a child c referencing it —
    // including a row whose FK key collides with the default db's p row.
    let storage = engine.storage_arc();
    let hr = storage.rel_create_database("hr").expect("create hr");
    for (name, key, fks) in [
        ("p", vec!["id".to_string()], Vec::new()),
        (
            "c",
            vec!["id".to_string()],
            vec![crate::relstore::catalog::ForeignKeyDef {
                name: "fk_c_p".to_string(),
                columns: vec![1],
                parent: "p".to_string(),
            }],
        ),
    ] {
        storage
            .rel_create_table(
                hr,
                name,
                vec![
                    crate::relstore::row::Column {
                        name: "id".to_string(),
                        column_type: crate::relstore::types::ColumnType::Int,
                        nullable: false,
                        collation: None,
                    },
                    crate::relstore::row::Column {
                        name: "pid".to_string(),
                        column_type: crate::relstore::types::ColumnType::Int,
                        nullable: false,
                        collation: None,
                    },
                ],
                &key,
                Vec::new(),
                None,
                Vec::new(),
                fks,
            )
            .expect("hr table");
    }
    storage
        .rel_insert(
            hr,
            "p",
            vec![
                crate::relstore::types::Datum::Int(1),
                crate::relstore::types::Datum::Int(0),
            ],
        )
        .expect("hr p row");
    storage
        .rel_insert(
            hr,
            "c",
            vec![
                crate::relstore::types::Datum::Int(10),
                crate::relstore::types::Datum::Int(1),
            ],
        )
        .expect("hr c row referencing hr p id 1");

    // DELETE of the default db's p row must not see hr's child (no false
    // 547), and DROP TABLE p must not be blocked by hr's FK (no 3726).
    let env = sql(&engine, "DELETE FROM p WHERE id = 1");
    assert_eq!(
        env["results"][0]["rows_affected"].as_i64(),
        Some(1),
        "cross-database child must not block the delete: {env}"
    );
    let env = sql(&engine, "DROP TABLE p");
    assert!(
        env["error"].is_null(),
        "cross-database FK must not raise 3726: {env}"
    );

    // sys.* views enumerate only the session's database.
    let (_, rows) = sql_rows(&engine, "SELECT name FROM sys.tables ORDER BY name");
    assert!(
        rows.iter().all(|r| r[0].as_deref() != Some("c")),
        "hr's objects must not appear in a default-db session's sys.tables: {rows:?}"
    );
    let _ = std::fs::remove_file(path);
}
