use super::*;

// ---- Stage 5: collation-aware equality (case-insensitive default) --------
//
// The database default collation is `..._CI_AS` (case-insensitive), so string
// equality, grouping, DISTINCT, joins, key seeks, and uniqueness all fold
// case unless a column is declared `_CS`/`_BIN`. These tests exercise every
// folded-key path — a missed fold site would surface as a wrong result here.

#[test]
fn collation_ci_where_and_predicates_fold_case() {
    let path = unique_temp_path("coll-ci-where");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name VARCHAR(20))")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC'), (3, 'Xyz')")
        .expect("insert");
    // Equality (scan path, no index): matches every case-variant.
    let (_, mut rows) = sql_rows(&engine, "SELECT id FROM t WHERE name = 'aBc' ORDER BY id");
    rows.sort();
    assert_eq!(
        rows,
        vec![vec![Some("1".into())], vec![Some("2".into())]],
        "CI equality matches both cases"
    );
    // IN, LIKE, BETWEEN all fold case too.
    let (_, r) = sql_rows(&engine, "SELECT COUNT(*) FROM t WHERE name IN ('ABC')");
    assert_eq!(r, vec![vec![Some("2".into())]], "CI IN");
    let (_, r) = sql_rows(&engine, "SELECT COUNT(*) FROM t WHERE name LIKE 'X%'");
    assert_eq!(r, vec![vec![Some("1".into())]], "CI LIKE matches 'Xyz'");
    let (_, r) = sql_rows(
        &engine,
        "SELECT COUNT(*) FROM t WHERE name BETWEEN 'A' AND 'D'",
    );
    assert_eq!(r, vec![vec![Some("2".into())]], "CI BETWEEN folds bounds");
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_ci_char_pk_uniqueness_and_lookup() {
    let path = unique_temp_path("coll-ci-pk");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (code VARCHAR(10) NOT NULL PRIMARY KEY, v INT)")
        .expect("create");
    engine
        .execute("INSERT INTO t VALUES ('ABC', 1)")
        .expect("ins1");
    // A case-variant is a duplicate PK under the case-insensitive collation.
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO t VALUES ('abc', 2)"),
        2627,
        "CI clustered PK rejects a case-variant duplicate"
    );
    // A clustered PK lookup by a case-variant finds the stored row.
    let (_, rows) = sql_rows(&engine, "SELECT v FROM t WHERE code = 'aBc'");
    assert_eq!(rows, vec![vec![Some("1".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_ci_unique_index_folds_case() {
    let path = unique_temp_path("coll-ci-uix");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, email VARCHAR(50))")
        .expect("create");
    engine
        .execute("CREATE UNIQUE INDEX ux ON t (email)")
        .expect("uix");
    engine
        .execute("INSERT INTO t VALUES (1, 'A@B.com')")
        .expect("ins1");
    assert_eq!(
        sql_error_number(&engine, "INSERT INTO t VALUES (2, 'a@b.COM')"),
        2601,
        "CI unique index rejects a case-variant duplicate"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_ci_join_and_grouping_fold_case() {
    let path = unique_temp_path("coll-ci-join");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE l (id INT NOT NULL PRIMARY KEY, k VARCHAR(10))")
        .expect("l");
    engine
        .execute("CREATE TABLE r (id INT NOT NULL PRIMARY KEY, k VARCHAR(10))")
        .expect("r");
    engine
        .execute("INSERT INTO l VALUES (1, 'abc'), (2, 'ABC'), (3, 'zzz')")
        .expect("l ins");
    engine
        .execute("INSERT INTO r VALUES (10, 'ABC'), (20, 'aBc')")
        .expect("r ins");
    // Hash join equi-key folds: every l/r case-variant pair matches (2*2 = 4).
    let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM l JOIN r ON l.k = r.k");
    assert_eq!(
        rows,
        vec![vec![Some("4".into())]],
        "CI join matches all case pairs"
    );
    // GROUP BY folds case: 'abc'/'ABC' form one group of 2, 'zzz' one of 1.
    let (_, mut rows) = sql_rows(&engine, "SELECT COUNT(*) FROM l GROUP BY k");
    rows.sort();
    assert_eq!(
        rows,
        vec![vec![Some("1".into())], vec![Some("2".into())]],
        "CI GROUP BY collapses case-variants into 2 groups (sizes 1 and 2)"
    );
    // DISTINCT collapses case-variants to {abc, zzz}; COUNT(DISTINCT) counts once.
    let (_, rows) = sql_rows(&engine, "SELECT DISTINCT k FROM l");
    assert_eq!(rows.len(), 2, "CI DISTINCT collapses 'abc'/'ABC': {rows:?}");
    let (_, rows) = sql_rows(&engine, "SELECT COUNT(DISTINCT k) FROM l");
    assert_eq!(rows, vec![vec![Some("2".into())]], "CI COUNT(DISTINCT)");
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_ci_foreign_key_folds_case() {
    let path = unique_temp_path("coll-ci-fk");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE parent (code VARCHAR(10) NOT NULL PRIMARY KEY)")
        .expect("parent");
    engine
        .execute(
            "CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pcode VARCHAR(10) \
                 REFERENCES parent(code))",
        )
        .expect("child");
    engine
        .execute("INSERT INTO parent VALUES ('ABC')")
        .expect("parent ins");
    // A child FK value matches the parent case-insensitively (folded probe).
    engine
        .execute("INSERT INTO child VALUES (1, 'aBc')")
        .expect("child ins with case-variant FK");
    // Deleting the parent is blocked by the case-variant child reference.
    assert_eq!(
        sql_error_number(&engine, "DELETE FROM parent WHERE code = 'ABC'"),
        547,
        "CI FK: child 'aBc' still references parent 'ABC'"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_cs_override_is_case_sensitive() {
    let path = unique_temp_path("coll-cs");
    let engine = new_engine(&path);
    engine
        .execute(
            "CREATE TABLE t (code VARCHAR(10) COLLATE Latin1_General_CS_AS NOT NULL PRIMARY KEY)",
        )
        .expect("create cs pk");
    // Under a case-sensitive collation, case-variants are distinct keys.
    engine
        .execute("INSERT INTO t VALUES ('ABC')")
        .expect("ins ABC");
    engine
        .execute("INSERT INTO t VALUES ('abc')")
        .expect("case-sensitive PK admits 'abc' alongside 'ABC'");
    // Equality is exact: only the matching case.
    let (_, rows) = sql_rows(&engine, "SELECT code FROM t WHERE code = 'abc'");
    assert_eq!(rows, vec![vec![Some("abc".into())]], "CS equality is exact");
    let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM t");
    assert_eq!(rows, vec![vec![Some("2".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_ci_folded_keys_survive_restart() {
    let path = unique_temp_path("coll-ci-restart");
    {
        let engine = new_engine(&path);
        engine
            .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name VARCHAR(20))")
            .expect("create");
        engine
            .execute("INSERT INTO t VALUES (1, 'Hello'), (2, 'WORLD')")
            .expect("insert");
        // Pad past the tiny-table tie-break (a <= 16-row table scans).
        for i in 0..20 {
            engine
                .execute(&format!("INSERT INTO t VALUES ({}, '0p{i}')", 100 + i))
                .expect("pad");
        }
        engine
            .execute("CREATE INDEX ix ON t (name)")
            .expect("index");
    }
    // Reopen: the folded index keys on disk must still seek case-insensitively.
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("engine");
    let plan = plan_lines(&engine, "SELECT id FROM t WHERE name = 'hello'");
    assert!(plan.iter().any(|l| l.contains("Index Seek")), "{plan:?}");
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t WHERE name = 'hello'");
    assert_eq!(
        rows,
        vec![vec![Some("1".into())]],
        "folded seek survives restart"
    );
    let _ = std::fs::remove_file(path);
}

// ---- Regression tests for adversarial-review findings on #82 -------------

#[test]
fn all_null_groups_aggregate_identically_through_the_spill_path() {
    // A group whose every value is NULL: SUM/MIN/MAX/AVG NULL, COUNT(col)
    // 0, COUNT(*) counts rows — and the grace-hash spill path must answer
    // exactly as the in-memory path does (an all-NULL group must not read
    // as "no group" after partitioning).
    let path = unique_temp_path("all-null-spill");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, k VARCHAR(10), v INT)")
        .expect("create");
    // 100 all-NULL rows in group 'a', 100 mixed rows in group 'b'.
    for i in 0..100 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, 'a', NULL)"))
            .expect("insert a");
        let v = if i % 2 == 0 {
            "NULL".to_string()
        } else {
            i.to_string()
        };
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, 'b', {v})", 100 + i))
            .expect("insert b");
    }
    let query = "SELECT k, COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v)                      FROM t GROUP BY k ORDER BY k";
    let (_, in_memory) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(Some(400));
    let (_, spilled) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(None);
    assert_eq!(spilled, in_memory, "spill path changes all-NULL groups");
    assert_eq!(
        in_memory[0],
        vec![
            Some("a".into()),
            Some("100".into()),
            Some("0".into()),
            None,
            None,
            None,
            None
        ],
        "all-NULL group: COUNT(*) counts, everything else is NULL/0"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_ci_group_by_spill_folds_case() {
    // The grace-hash GROUP BY spill path must partition by the FOLDED key, or
    // case-variant groups split across partitions on large input. 'World'/
    // 'world' hash to *different* partitions unfolded (unlike 'ABC'/'abc',
    // which collide by luck), so this pair reliably catches the bug.
    let path = unique_temp_path("coll-ci-agg-spill");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, k VARCHAR(10))")
        .expect("create");
    for i in 0..200 {
        let k = if i % 2 == 0 { "World" } else { "world" };
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, '{k}')"))
            .expect("ins");
    }
    let query = "SELECT COUNT(*) FROM t GROUP BY k";
    let (_, reference) = sql_rows(&engine, query);
    assert_eq!(
        reference,
        vec![vec![Some("200".into())]],
        "in-memory: one case-insensitive group of 200"
    );
    crate::engine::set_test_sort_budget(Some(400));
    let (_, spilled) = sql_rows(&engine, query);
    crate::engine::set_test_sort_budget(None);
    assert_eq!(
        spilled, reference,
        "spilled GROUP BY must fold case like the in-memory path (one group)"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_cs_column_delete_is_exact() {
    // UPDATE/DELETE WHERE on a _CS column must compare exactly — a plain
    // Vec<String> resolver would report the CI default for every column and
    // delete case-variant rows it must keep (data loss).
    let path = unique_temp_path("coll-cs-dml");
    let engine = new_engine(&path);
    engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) COLLATE Latin1_General_CS_AS)",
            )
            .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC')")
        .expect("ins");
    engine
        .execute("DELETE FROM t WHERE code = 'abc'")
        .expect("delete");
    let (_, rows) = sql_rows(&engine, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![vec![Some("2".into())]],
        "CS DELETE removes only the exact 'abc' (id=1), keeps 'ABC' (id=2)"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_ci_self_ref_fk_same_statement() {
    // A self-referencing FK to a case-variant sibling in the SAME statement
    // must resolve case-insensitively (the sibling isn't committed yet, so the
    // batch match — not the folded rel_get — is the only satisfying path).
    let path = unique_temp_path("coll-ci-selfref");
    let engine = new_engine(&path);
    engine
            .execute(
                "CREATE TABLE t (id VARCHAR(10) NOT NULL PRIMARY KEY, pid VARCHAR(10) REFERENCES t(id))",
            )
            .expect("create");
    engine
        .execute("INSERT INTO t VALUES ('ABC', NULL), ('X', 'abc')")
        .expect("case-variant self-reference to a same-statement sibling");
    let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM t");
    assert_eq!(rows, vec![vec![Some("2".into())]]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_cs_distinct_is_exact() {
    // DISTINCT on a _CS column must keep case-variants distinct, consistent
    // with GROUP BY and COUNT(DISTINCT) on the same column.
    let path = unique_temp_path("coll-cs-distinct");
    let engine = new_engine(&path);
    engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) COLLATE Latin1_General_CS_AS)",
            )
            .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC')")
        .expect("ins");
    let (_, rows) = sql_rows(&engine, "SELECT DISTINCT code FROM t");
    assert_eq!(rows.len(), 2, "CS DISTINCT keeps both cases: {rows:?}");
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_mixed_fk_still_enforced_on_delete() {
    // A mixed-collation FK (child _CS, parent CI) must not take the index fast
    // path — that folds by the child collation and would miss a case-variant
    // reference (child 'abc' → parent 'ABC'), wrongly allowing the parent
    // delete. The collation-match gate routes it to the parent-collation scan.
    let path = unique_temp_path("coll-mixed-fk");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE parent (code VARCHAR(10) NOT NULL PRIMARY KEY)")
        .expect("parent");
    engine
        .execute(
            "CREATE TABLE child (id INT NOT NULL PRIMARY KEY, pcode VARCHAR(10) \
                 COLLATE Latin1_General_CS_AS REFERENCES parent(code))",
        )
        .expect("child");
    engine
        .execute("CREATE INDEX ix ON child (pcode)")
        .expect("index"); // would otherwise enable the fast path
    engine
        .execute("INSERT INTO parent VALUES ('ABC')")
        .expect("parent ins");
    engine
        .execute("INSERT INTO child VALUES (1, 'abc')")
        .expect("child references parent case-insensitively");
    assert_eq!(
        sql_error_number(&engine, "DELETE FROM parent WHERE code = 'ABC'"),
        547,
        "mixed-collation FK is still enforced (child 'abc' references parent 'ABC')"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_cs_having_is_exact() {
    // HAVING (and the SELECT projection) evaluate against a synthetic group
    // row; on a _CS grouping column they must compare exactly, or a HAVING
    // re-merges groups that grouping kept apart.
    let path = unique_temp_path("coll-cs-having");
    let engine = new_engine(&path);
    engine
            .execute(
                "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, code VARCHAR(10) COLLATE Latin1_General_CS_AS)",
            )
            .expect("create");
    engine
        .execute("INSERT INTO t VALUES (1, 'abc'), (2, 'ABC')")
        .expect("ins");
    let (_, rows) = sql_rows(
        &engine,
        "SELECT code FROM t GROUP BY code HAVING code = 'ABC'",
    );
    assert_eq!(
        rows,
        vec![vec![Some("ABC".into())]],
        "CS HAVING matches only the exact 'ABC' group"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn collation_ci_self_ref_fk_update() {
    // UPDATE setting a self-referencing FK to a case-variant of an existing
    // PK must succeed under the case-insensitive collation.
    let path = unique_temp_path("coll-ci-selfref-upd");
    let engine = new_engine(&path);
    engine
            .execute(
                "CREATE TABLE t (id VARCHAR(10) NOT NULL PRIMARY KEY, pid VARCHAR(10) REFERENCES t(id))",
            )
            .expect("create");
    engine
        .execute("INSERT INTO t VALUES ('ABC', NULL), ('X', NULL)")
        .expect("ins");
    engine
        .execute("UPDATE t SET pid = 'abc' WHERE id = 'X'")
        .expect("case-variant self-reference resolves on UPDATE");
    let (_, rows) = sql_rows(&engine, "SELECT pid FROM t WHERE id = 'X'");
    assert_eq!(rows, vec![vec![Some("abc".into())]], "UPDATE applied");
    let _ = std::fs::remove_file(path);
}
