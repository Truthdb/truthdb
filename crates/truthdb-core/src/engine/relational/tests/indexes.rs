use super::*;

// ---- secondary indexes + planner (Stage 7) -------------------------

#[test]
fn sql_index_ab_harness_identical_results() {
    let path = unique_temp_path("sql-index-ab");
    let engine = new_engine(&path);
    // Two identical tables; an index only on one.
    for t in ["noidx", "idx"] {
        engine
            .execute(&format!(
                "CREATE TABLE {t} (id INT NOT NULL PRIMARY KEY, a INT, name NVARCHAR(20))"
            ))
            .expect("create");
        engine
            .execute(&format!(
                "INSERT INTO {t} VALUES (1,10,'a'),(2,20,'b'),(3,20,'c'),(4,30,NULL),(5,10,'e')"
            ))
            .expect("insert");
        // Pad past the tiny-table tie-break (identically on both sides).
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO {t} VALUES ({}, 900, 'p')", 100 + i))
                .expect("pad");
        }
    }
    engine
        .execute("CREATE INDEX ix_a ON idx (a)")
        .expect("create index");

    // Every query returns identical rows whether it scans or seeks.
    for pred in [
        "a = 20",
        "a > 15",
        "a >= 20",
        "a < 25",
        "a = 10 AND id > 1",
        "a <> 20",
    ] {
        let q = |t: &str| format!("SELECT id, a FROM {t} WHERE {pred} ORDER BY id");
        let (_, base) = sql_rows(&engine, &q("noidx"));
        let (_, with_index) = sql_rows(&engine, &q("idx"));
        assert_eq!(base, with_index, "mismatch for predicate `{pred}`");
    }

    // The equality predicate actually uses the index.
    let plan = plan_lines(&engine, "SELECT id FROM idx WHERE a = 20");
    assert!(
        plan.iter()
            .any(|l| l.contains("Index Seek") && l.contains("ix_a")),
        "expected an index seek, got {plan:?}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_unique_index_rejects_duplicate_2601() {
    let path = unique_temp_path("sql-unique-index");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email NVARCHAR(50))")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 'a@x'), (2, 'b@x')")
        .expect("insert");
    engine
        .execute("CREATE UNIQUE INDEX ux_email ON t (email)")
        .expect("create unique index");
    // A duplicate email now violates the unique index (2601, not 2627).
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO t VALUES (3, 'a@x')"),
        2601
    );
    // Updating to a duplicate also violates it.
    assert_eq!(
        sql_error_number(&engine, "UPDATE t SET email = 'a@x' WHERE id = 2"),
        2601
    );
    // A distinct value is fine.
    engine
        .execute("INSERT INTO t VALUES (3, 'c@x')")
        .expect("distinct insert");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_unique_index_build_rejects_existing_duplicates() {
    let path = unique_temp_path("sql-unique-build");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 5)")
        .expect("insert");
    // Building a unique index over duplicate data fails.
    assert_eq!(
        sql_error_number(&engine, "CREATE UNIQUE INDEX ux_a ON t (a)"),
        2601
    );
    // ...and the failed build left no index behind (still scannable).
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_index_maintained_across_update_and_delete() {
    let path = unique_temp_path("sql-index-maint");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .expect("insert");
    engine.execute("CREATE INDEX ix_a ON t (a)").expect("index");

    // Update moves a row from a=20 to a=25; delete removes a=30.
    engine
        .execute("UPDATE t SET a = 25 WHERE id = 2")
        .expect("update");
    engine
        .execute("DELETE FROM t WHERE a = 30")
        .expect("delete");

    // Index seeks reflect the mutations.
    let (_, at20) = sql_rows(&engine, "SELECT id FROM t WHERE a = 20");
    assert!(at20.is_empty(), "a=20 gone after update");
    let (_, at25) = sql_rows(&engine, "SELECT id FROM t WHERE a = 25");
    assert_eq!(at25, vec![vec![Some("2".into())]]);
    let (_, at30) = sql_rows(&engine, "SELECT id FROM t WHERE a = 30");
    assert!(at30.is_empty(), "a=30 gone after delete");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_showplan_text_reports_seek_versus_scan() {
    let path = unique_temp_path("sql-showplan");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
        .expect("create");
    engine.execute("CREATE INDEX ix_a ON t (a)").expect("index");
    // Pad past the tiny-table tie-break: a table of <= 16 rows plans as
    // a scan (the seek ties with it), and this test is about the seek.
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, 900)", 100 + i))
            .expect("pad");
    }

    let seek = plan_lines(&engine, "SELECT id FROM t WHERE a = 7");
    assert_eq!(seek[0], "Index Seek(t.ix_a), SEEK: a = 7");
    assert_eq!(seek[1], "Key Lookup(t)");

    // No sargable predicate → a scan.
    let scan = plan_lines(&engine, "SELECT id FROM t WHERE a + 1 = 8");
    assert_eq!(scan, vec!["Table Scan(t)".to_string()]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_index_survives_restart() {
    let path = unique_temp_path("sql-index-restart");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1,10),(2,20)")
        .expect("insert");
    // Pad past the tiny-table tie-break (a <= 16-row table plans as a scan).
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, 900)", 100 + i))
            .expect("pad");
    }
    engine.execute("CREATE INDEX ix_a ON t (a)").expect("index");

    drop(engine);
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("replay");
    // The index is still usable after recovery.
    let plan = plan_lines(&engine, "SELECT id FROM t WHERE a = 20");
    assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE a = 20");
    assert_eq!(rows, vec![vec![Some("2".into())]]);
    // Maintenance still works post-restart.
    engine
        .execute("INSERT INTO t VALUES (3, 20)")
        .expect("insert after restart");
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE a = 20 ORDER BY id");
    assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_composite_and_descending_index_seek() {
    let path = unique_temp_path("sql-composite-index");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT, b INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1,1,100),(2,1,200),(3,2,100),(4,2,200)")
        .expect("insert");
    // Pad past the tiny-table tie-break (a <= 16-row table plans as a scan).
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, 900, 900)", 100 + i))
            .expect("pad");
    }
    engine
        .execute("CREATE INDEX ix_ab ON t (a, b DESC)")
        .expect("create composite index");

    // Equality on the leading column + range on the second seeks the index.
    let plan = plan_lines(&engine, "SELECT id FROM t WHERE a = 2 AND b = 200");
    assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE a = 2 AND b = 200");
    assert_eq!(rows, vec![vec![Some("4".into())]]);
    // Leading-column-only seek returns both a=1 rows.
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE a = 1 ORDER BY id");
    assert_eq!(rows, vec![vec![Some("1".into())], vec![Some("2".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_index_on_heap_table_uses_rid_locator() {
    let path = unique_temp_path("sql-heap-index");
    let engine = new_engine(&path);
    // No PRIMARY KEY → heap table.
    engine
        .execute("CREATE TABLE h (a INT, name NVARCHAR(20))")
        .expect("create heap");
    engine
        .execute("INSERT INTO h VALUES (10,'x'),(20,'y'),(10,'z')")
        .expect("insert");
    // Pad past the tiny-table tie-break (a <= 16-row table plans as a scan).
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO h VALUES ({}, 'p')", 900 + i))
            .expect("pad");
    }
    engine.execute("CREATE INDEX ix_a ON h (a)").expect("index");

    let plan = plan_lines(&engine, "SELECT name FROM h WHERE a = 10");
    assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
    let (_, mut rows) = sql_rows(&engine, "SELECT name FROM h WHERE a = 10");
    rows.sort();
    assert_eq!(rows, vec![vec![Some("x".into())], vec![Some("z".into())]]);
    // Update through a heap row keeps the index consistent.
    engine
        .execute("UPDATE h SET a = 99 WHERE name = 'x'")
        .expect("update");
    let (_, rows) = sql_rows(&engine, "SELECT name FROM h WHERE a = 10");
    assert_eq!(rows, vec![vec![Some("z".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_drop_index_and_sys_indexes() {
    let path = unique_temp_path("sql-drop-index");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
        .expect("create");
    engine.execute("CREATE INDEX ix_a ON t (a)").expect("index");
    // Pad past the tiny-table tie-break, or the post-drop "Table Scan"
    // assertion below would hold with the index still present (vacuous).
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, 900)", 100 + i))
            .expect("pad");
    }

    // sys.indexes lists it.
    let (_, rows) = sql_rows(&engine, "SELECT name FROM sys.indexes");
    assert_eq!(rows, vec![vec![Some("ix_a".into())]]);

    engine.execute("DROP INDEX ix_a ON t").expect("drop index");
    let (_, rows) = sql_rows(&engine, "SELECT name FROM sys.indexes");
    assert!(rows.is_empty(), "index gone from catalog");
    // Queries now scan.
    let plan = plan_lines(&engine, "SELECT id FROM t WHERE a = 1");
    assert_eq!(plan, vec!["Table Scan(t)".to_string()]);
    // Dropping a missing index errors 3701.
    assert_eq!(sql_error_number(&engine, "DROP INDEX nope ON t"), 3701);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_nvarchar_equality_seeks_case_insensitively() {
    let path = unique_temp_path("sql-index-nvarchar");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC'), (3, 'xyz')")
        .expect("insert");
    // Pad past the tiny-table tie-break; '0...' sorts below 'a', so the
    // range assertions below keep their exact row sets.
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, '0p{i}')", 100 + i))
            .expect("pad");
    }
    engine
        .execute("CREATE INDEX ix_name ON t (name)")
        .expect("index");

    // Under the default (case-insensitive) collation, equality folds case.
    // The index key is folded the same way, so it still SEEKS (not scans) and
    // the seek finds every case-variant: 'abc' and 'ABC' share one folded key.
    let plan = plan_lines(&engine, "SELECT id FROM t WHERE name = 'abc'");
    assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
    let (_, mut rows) = sql_rows(&engine, "SELECT id FROM t WHERE name = 'abc'");
    rows.sort();
    assert_eq!(
        rows,
        vec![vec![Some("1".into())], vec![Some("2".into())]],
        "case-insensitive equality matches both 'abc' and 'ABC'"
    );

    // An NVARCHAR range SEEKS since the keys became collation sort keys
    // (#94): sort-key byte order IS the filter's compare order, so the old
    // UTF-16BE divergence that forced a scan no longer exists.
    // Case-insensitive: 'ABC' folds with 'abc' > 'a', so all three match.
    let plan = plan_lines(&engine, "SELECT id FROM t WHERE name > 'a'");
    assert!(
        plan.iter().any(|l| l.contains("Index Seek")),
        "NVARCHAR ranges seek over sort keys: {plan:?}"
    );
    let (_, mut rows) = sql_rows(&engine, "SELECT id FROM t WHERE name > 'a'");
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![Some("1".into())],
            vec![Some("2".into())],
            vec![Some("3".into())]
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_varchar_range_can_index_seek() {
    let path = unique_temp_path("sql-index-varchar");
    let engine = new_engine(&path);
    // VARCHAR keys are UTF-8 bytes, whose order equals code-point order, so
    // a range seek is correct.
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, code VARCHAR(20))")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1,'aaa'),(2,'mmm'),(3,'zzz')")
        .expect("insert");
    // Pad past the tiny-table tie-break; '0...' sorts below 'b', so the
    // range assertion below keeps its exact row set.
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, '0c{i}')", 100 + i))
            .expect("pad");
    }
    engine
        .execute("CREATE INDEX ix_code ON t (code)")
        .expect("index");

    let plan = plan_lines(&engine, "SELECT id FROM t WHERE code > 'b'");
    assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
    let (_, mut rows) = sql_rows(&engine, "SELECT id FROM t WHERE code > 'b'");
    rows.sort();
    assert_eq!(rows, vec![vec![Some("2".into())], vec![Some("3".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_range_seek_follows_linguistic_order_not_code_points() {
    // Under the default collation, accented letters sort next to their
    // base letter ('å' < 'b') while their UTF-8 bytes sort past 'z'. The
    // index keys are collation SORT KEYS, so a range seek's bounds agree
    // with the filter — a code-point-keyed index would exclude 'å'/'ä'
    // from `w < 'b'` and silently drop matching rows.
    let path = unique_temp_path("sql-index-locale-range");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, w VARCHAR(20))")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1,'a'),(2,'å'),(3,'ä'),(4,'b'),(5,'z')")
        .expect("insert");
    // Pad past the tiny-table tie-break; 'z...' sorts above 'b' in every
    // collation involved, so the range's row set is untouched.
    for i in 0..20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, 'zz{i}')", 100 + i))
            .expect("pad");
    }
    engine.execute("CREATE INDEX ix_w ON t (w)").expect("index");

    let q = "SELECT id FROM t WHERE w < 'b' ORDER BY id";
    let plan = plan_lines(&engine, "SELECT id FROM t WHERE w < 'b'");
    assert!(
        plan.iter().any(|l| l.contains("Index Seek")),
        "the range seeks: {plan:?}"
    );
    let (_, seeked) = sql_rows(&engine, q);
    assert_eq!(
        seeked,
        vec![
            vec![Some("1".into())],
            vec![Some("2".into())],
            vec![Some("3".into())]
        ],
        "'å' and 'ä' sort below 'b' linguistically and the seek keeps them"
    );

    // A/B: the scan agrees.
    engine.execute("DROP INDEX ix_w ON t").expect("drop");
    let (_, scanned) = sql_rows(&engine, q);
    assert_eq!(scanned, seeked, "seek == scan");
    let _ = std::fs::remove_file(path);
}

/// A/B (seek vs scan) equality for character range seeks
/// across collations, with supplementary-plane characters, empty strings
/// and NULLs in both the stored data and the bounds.
#[test]
fn character_range_seeks_match_scans_across_collations() {
    let values = [
        "a",
        "A",
        "b",
        "z",
        "Z",
        "å",
        "ä",
        "ö",
        "é",
        "e",
        "aa",
        "ab",
        "",
        "z\u{1F600}",
        "a\u{1F600}",
        "\u{1F600}",
        "\u{1F600}a",
        "\u{20000}",
        "\u{10000}",
        "\u{E000}",
        "\u{FFFD}",
        "\u{10FFFF}",
    ];
    let bounds = ["a", "å", "b", "z", "\u{1F600}", "\u{E000}", "\u{20000}", ""];
    let ops = [">", ">=", "<", "<="];
    for (label, decl) in [
        ("nv-default", "NVARCHAR(40)"),
        ("nv-cs", "NVARCHAR(40) COLLATE Latin1_General_CS_AS"),
        ("nv-ai", "NVARCHAR(40) COLLATE Latin1_General_CI_AI"),
        ("nv-bin2", "NVARCHAR(40) COLLATE Latin1_General_BIN2"),
        ("nv-sv", "NVARCHAR(40) COLLATE Finnish_Swedish_CI_AS"),
        ("vc-default", "VARCHAR(40)"),
        ("vc-bin2", "VARCHAR(40) COLLATE Latin1_General_BIN2"),
    ] {
        let path = unique_temp_path(&format!("probe-ab-{label}"));
        let engine = new_engine(&path);
        engine
            .execute(&format!(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, w {decl})"
            ))
            .expect("create");
        for (i, v) in values.iter().enumerate() {
            // BIN2/CS keep 'a'/'A' distinct; CI collations make some
            // values duplicate keys — allowed (non-unique index).
            engine
                .execute(&format!("INSERT INTO t VALUES ({i}, '{v}')"))
                .expect("insert");
        }
        // NULLs and padding past the tiny-table tie-break.
        for i in 0..12 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, NULL)", 100 + i))
                .expect("insert null");
        }
        engine.execute("CREATE INDEX ix_w ON t (w)").expect("index");
        let mut queries = Vec::new();
        for b in bounds {
            for op in ops {
                queries.push(format!("SELECT id FROM t WHERE w {op} '{b}' ORDER BY id"));
            }
        }
        let mut with_index = Vec::new();
        for q in &queries {
            let plan = plan_lines(&engine, q.strip_suffix(" ORDER BY id").unwrap());
            assert!(
                plan.iter().any(|l| l.contains("Index Seek")),
                "{label}: expected seek for {q}: {plan:?}"
            );
            with_index.push(sql_rows(&engine, q).1);
        }
        engine.execute("DROP INDEX ix_w ON t").expect("drop");
        for (q, seeked) in queries.iter().zip(with_index) {
            let scanned = sql_rows(&engine, q).1;
            assert_eq!(scanned, seeked, "{label}: seek != scan for {q}");
        }
        let _ = std::fs::remove_file(path);
    }
}

/// Composite (eq prefix + NVARCHAR range) and DESC-column
/// bounds, exercising prefix_upper_bound's carry over inverted bytes.
#[test]
fn composite_and_desc_index_bounds_match_scans() {
    let path = unique_temp_path("probe-composite-desc");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, k INT, w NVARCHAR(40))")
        .expect("create");
    let values = [
        "a",
        "b",
        "z",
        "å",
        "ä",
        "\u{1F600}",
        "z\u{1F600}",
        "\u{20000}",
        "\u{E000}",
        "aa",
    ];
    let mut id = 0;
    for k in [1, 2, 3] {
        for v in values {
            engine
                .execute(&format!("INSERT INTO t VALUES ({id}, {k}, '{v}')"))
                .expect("insert");
            id += 1;
        }
    }
    engine
        .execute("CREATE INDEX ix_kw ON t (k, w)")
        .expect("index");
    let mut queries = Vec::new();
    for b in ["å", "b", "\u{1F600}", "z"] {
        for op in [">", ">=", "<", "<="] {
            queries.push(format!(
                "SELECT id FROM t WHERE k = 2 AND w {op} '{b}' ORDER BY id"
            ));
        }
    }
    // Equality-only on k too (prefix_upper_bound over the eq prefix).
    queries.push("SELECT id FROM t WHERE k = 2 ORDER BY id".to_string());
    let mut with_index = Vec::new();
    for q in &queries {
        let plan = plan_lines(&engine, q.strip_suffix(" ORDER BY id").unwrap());
        assert!(
            plan.iter().any(|l| l.contains("Index Seek")),
            "expected seek for {q}: {plan:?}"
        );
        with_index.push(sql_rows(&engine, q).1);
    }
    engine.execute("DROP INDEX ix_kw ON t").expect("drop");
    for (q, seeked) in queries.iter().zip(with_index) {
        let scanned = sql_rows(&engine, q).1;
        assert_eq!(scanned, seeked, "composite: seek != scan for {q}");
    }

    // DESC index: a range must NOT seek (bounds are not inverted), an
    // equality must seek correctly through inverted-byte bounds
    // (prefix_upper_bound's 0xFF carry path).
    engine
        .execute("CREATE INDEX ix_wd ON t (w DESC)")
        .expect("desc index");
    let plan = plan_lines(&engine, "SELECT id FROM t WHERE w < 'b'");
    assert!(
        plan.iter().all(|l| !l.contains("Index Seek")),
        "a DESC column must not back a range seek: {plan:?}"
    );
    let q = "SELECT id FROM t WHERE w = 'å' ORDER BY id";
    let plan = plan_lines(&engine, "SELECT id FROM t WHERE w = 'å'");
    assert!(
        plan.iter().any(|l| l.contains("Index Seek")),
        "DESC equality seeks: {plan:?}"
    );
    let seeked = sql_rows(&engine, q).1;
    engine.execute("DROP INDEX ix_wd ON t").expect("drop");
    assert_eq!(sql_rows(&engine, q).1, seeked, "desc eq: seek != scan");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_drop_index_is_table_scoped() {
    let path = unique_temp_path("sql-drop-scoped");
    let engine = new_engine(&path);
    // Two tables with same-named indexes; DROP INDEX must only touch the
    // named table's index.
    for t in ["t1", "t2"] {
        engine
            .execute(&format!(
                "CREATE TABLE {t} (id INT NOT NULL PRIMARY KEY, a INT)"
            ))
            .expect("create");
        // Pad past the tiny-table tie-break (an empty table plans as a scan).
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO {t} VALUES ({}, 900)", 100 + i))
                .expect("pad");
        }
        engine
            .execute(&format!("CREATE INDEX ix ON {t} (a)"))
            .expect("index");
    }
    engine.execute("DROP INDEX ix ON t1").expect("drop t1.ix");

    // t2's index survives; t1's is gone.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT object_id FROM sys.indexes ORDER BY object_id",
    );
    assert_eq!(rows.len(), 1, "only t2's index remains");
    let plan = plan_lines(&engine, "SELECT id FROM t2 WHERE a = 1");
    assert!(
        plan.iter().any(|l| l.contains("Index Seek")),
        "t2 still seeks"
    );
    let plan = plan_lines(&engine, "SELECT id FROM t1 WHERE a = 1");
    assert_eq!(plan, vec!["Table Scan(t1)".to_string()], "t1 index dropped");
    let _ = std::fs::remove_file(path);
}

#[test]
fn sql_create_index_inside_transaction_is_rejected() {
    let path = unique_temp_path("sql-index-in-txn");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT)")
        .expect("create");
    // DDL (incl. CREATE INDEX) is disallowed inside an explicit transaction.
    assert_eq!(
        sql_error_number(&engine, "BEGIN TRAN; CREATE INDEX ix_a ON t (a);"),
        226
    );
    let _ = std::fs::remove_file(path);
}
