//! `sys.s3_search`: full-text search over a bucket's contents from SQL,
//! ranked, with the CONTAINS operators, and never returning a version that
//! is no longer its key's latest.

use crate::engine::test_support::*;
use crate::engine::*;
use truthdb_object::{BodyWriter, ObjectAttrs};

fn put(engine: &Engine, bucket: &str, key: &str, text: &str) -> u64 {
    let storage = engine.storage();
    let mut writer = BodyWriter::begin(storage, 1).expect("begin");
    writer.write(storage, text.as_bytes(), 1).expect("write");
    let sealed = writer.finish(storage, 1).expect("finish");
    let attrs = ObjectAttrs {
        content_type: Some("text/plain".into()),
        ..ObjectAttrs::default()
    };
    engine
        .object(|file| file.transaction(|tx| tx.put_object(bucket, key, &sealed, &attrs, 2_000, 0)))
        .expect("put")
        .expect("put accepted")
        .version
}

fn index(engine: &Engine) {
    while engine.fulltext_pass(&|| false).expect("pass") {}
}

/// The keys `condition` finds in `bucket`, in result order.
fn keys(engine: &Engine, bucket: &str, condition: &str) -> Vec<String> {
    let condition = condition.replace('\'', "''");
    sql_rows(
        engine,
        &format!("SELECT [key] FROM sys.s3_search('{bucket}', '{condition}')"),
    )
    .1
    .into_iter()
    .map(|row| row[0].clone().expect("a key"))
    .collect()
}

fn sorted(mut keys: Vec<String>) -> Vec<String> {
    keys.sort();
    keys
}

#[test]
fn a_search_finds_ranks_and_checks_each_hit_against_the_latest_version() {
    let path = unique_temp_path("s3-search");
    let engine = new_engine(&path);
    engine
        .object(|file| file.transaction(|tx| tx.create_bucket("docs", 1_000)))
        .unwrap()
        .unwrap();
    engine.execute("s3 enable fulltext docs").unwrap();
    put(
        &engine,
        "docs",
        "guide.txt",
        "Database tuning guide: indexes, plans and statistics.",
    );
    let notes = "Meeting notes. The database was slow; we tuned the database twice.";
    put(&engine, "docs", "notes.txt", notes);
    put(
        &engine,
        "docs",
        "recipe.txt",
        "A recipe for bread with flour, water and salt.",
    );
    put(&engine, "docs", "logs/a.log", "error: disk full on db-1");
    put(&engine, "docs", "logs/b.log", "warning: disk almost full");
    index(&engine);

    // Ranked: more occurrences in a shorter text first.
    assert_eq!(
        keys(&engine, "docs", "database"),
        ["notes.txt", "guide.txt"]
    );
    assert_eq!(keys(&engine, "docs", "database AND guide"), ["guide.txt"]);
    assert_eq!(
        sorted(keys(&engine, "docs", "bread OR error")),
        ["logs/a.log", "recipe.txt"]
    );
    assert_eq!(keys(&engine, "docs", "disk AND NOT error"), ["logs/b.log"]);
    assert_eq!(
        keys(&engine, "docs", "\"disk full\" & !warning"),
        ["logs/a.log"]
    );
    assert_eq!(
        sorted(keys(&engine, "docs", "\"tun*\"")),
        ["guide.txt", "notes.txt"]
    );
    assert!(keys(&engine, "docs", "zebra").is_empty());

    // The columns, and a score that falls down the list.
    let (columns, rows) = sql_rows(&engine, "SELECT * FROM sys.s3_search('docs', 'database')");
    assert_eq!(
        columns,
        [
            "bucket",
            "key",
            "version_id",
            "score",
            "size",
            "last_modified_date"
        ]
    );
    assert_eq!(rows[0][0].as_deref(), Some("docs"));
    assert_eq!(rows[0][2].as_deref(), Some("null"));
    let score = |row: &Vec<Option<String>>| row[3].as_deref().unwrap().parse::<f64>().unwrap();
    assert!(score(&rows[0]) > score(&rows[1]) && score(&rows[1]) > 0.0);
    assert_eq!(rows[0][4], Some(notes.len().to_string()));

    // @top; joined with the inventory like any rowset.
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT [key] FROM sys.s3_search('docs', 'database', 1)"
        )
        .1,
        [[Some("notes.txt".to_string())]]
    );
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT o.content_type FROM sys.s3_search('docs', 'bread') s \
             JOIN sys.s3_objects('docs') o ON o.[key] = s.[key]"
        )
        .1,
        [[Some("text/plain".to_string())]]
    );

    // A key overwritten or deleted since it was indexed is not returned, even
    // before the indexer catches up; once it has, the new text is found.
    put(&engine, "docs", "recipe.txt", "A recipe for soup.");
    engine
        .object(|file| {
            file.transaction(|tx| tx.delete_object("docs", "logs/a.log", None, 3_000, 0))
        })
        .unwrap()
        .unwrap();
    assert!(keys(&engine, "docs", "bread").is_empty());
    assert!(keys(&engine, "docs", "error").is_empty());
    index(&engine);
    assert_eq!(keys(&engine, "docs", "soup"), ["recipe.txt"]);
    assert!(keys(&engine, "docs", "bread").is_empty());

    // Errors: no such bucket, not enabled, a malformed condition, a bad @top.
    engine
        .object(|file| file.transaction(|tx| tx.create_bucket("plain", 1_000)))
        .unwrap()
        .unwrap();
    for (query, number) in [
        ("SELECT * FROM sys.s3_search('nope', 'x')", 208),
        ("SELECT * FROM sys.s3_search('plain', 'x')", 7601),
        ("SELECT * FROM sys.s3_search('docs', 'a b')", 7630),
        ("SELECT * FROM sys.s3_search('docs', '(a')", 7630),
        ("SELECT * FROM sys.s3_search('docs', 'x', 0)", 206),
        ("SELECT * FROM sys.s3_search('docs', 'x', 10001)", 206),
        ("SELECT * FROM sys.s3_search('docs')", 313),
    ] {
        assert_eq!(sql_error_number(&engine, query), number, "{query}");
    }
    // Disabled: no longer searchable.
    engine.execute("s3 disable fulltext docs").unwrap();
    assert_eq!(
        sql_error_number(&engine, "SELECT * FROM sys.s3_search('docs', 'soup')"),
        7601
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_search_over_many_documents_returns_its_top_hits() {
    let path = unique_temp_path("s3-search-many");
    let engine = new_engine(&path);
    engine
        .object(|file| file.transaction(|tx| tx.create_bucket("many", 1_000)))
        .unwrap()
        .unwrap();
    engine.execute("s3 enable fulltext many").unwrap();
    // Document n holds "common" once among n filler words: shorter ranks
    // higher, so the order is by n.
    for n in 1..=300 {
        let text = format!("common {}", "filler ".repeat(n));
        put(&engine, "many", &format!("d{n:03}"), &text);
    }
    index(&engine);
    let found = keys(&engine, "many", "common");
    assert_eq!(found.len(), 100, "the default top");
    assert_eq!(found[0], "d001");
    assert_eq!(found[99], "d100");
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT COUNT(*) FROM sys.s3_search('many', 'common', 10000)"
        )
        .1,
        [[Some("300".to_string())]]
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}
