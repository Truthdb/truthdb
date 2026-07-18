//! A small sqllogictest-style runner (Stage 8). Each `tests/slt/*.slt` file is
//! run in-process against a fresh engine. Supported directives:
//!
//! ```text
//! statement ok            # the SQL that follows must succeed
//! statement error         # the SQL that follows must fail
//! query <types> [rowsort] # run the SQL, compare rows to the block after ----
//! ```
//!
//! A record is terminated by a blank line. `#` lines are comments. Result
//! cells are whitespace-joined; NULL prints as `NULL`. `rowsort` sorts the
//! result and expected rows before comparing (for order-independent queries).

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
    path.push(format!("truthdb-slt-{label}-{nanos}.db"));
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

fn format_datum(datum: &Datum) -> String {
    match datum {
        Datum::Null => "NULL".to_string(),
        Datum::TinyInt(v) => v.to_string(),
        Datum::SmallInt(v) => v.to_string(),
        Datum::Int(v) => v.to_string(),
        Datum::BigInt(v) => v.to_string(),
        Datum::Bit(b) => (if *b { "1" } else { "0" }).to_string(),
        Datum::Real(v) => v.to_string(),
        Datum::Float(v) => v.to_string(),
        Datum::VarChar(s) | Datum::NVarChar(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// Runs one SQL statement and returns its outcome: Ok(rows) on success (rows
/// empty for non-SELECT), Err(error number) on a SQL error.
fn run(engine: &mut Engine, ctx: &mut TxnContext, sql: &str) -> Result<Vec<String>, i32> {
    let outcome = engine.sql_batch(sql, ctx).expect("sql_batch");
    if let Some(error) = outcome.error {
        return Err(error.number);
    }
    let mut lines = Vec::new();
    for result in &outcome.results {
        if let StatementResult::Rows(rowset) = result {
            for row in &rowset.rows {
                lines.push(row.iter().map(format_datum).collect::<Vec<_>>().join(" "));
            }
        }
    }
    Ok(lines)
}

fn run_file(path: &Path) {
    let text = std::fs::read_to_string(path).expect("read slt file");
    let db = temp_path(path.file_stem().unwrap().to_str().unwrap());
    let mut engine = new_engine(&db);
    let mut ctx = TxnContext::default();
    // A real session identity, as every connected session has one: DB_NAME(),
    // sys.databases and USE read it.
    ctx.set_session_identity("truthdb".into(), "sa".into(), 1, "dbo".into(), 0, 0);

    let file = path.display();
    let mut lines = text.lines().peekable();
    while let Some(line) = lines.next() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts.as_slice() {
            ["statement", "ok"] => {
                let sql = take_sql(&mut lines);
                if let Err(number) = run(&mut engine, &mut ctx, &sql) {
                    panic!("{file}: `statement ok` failed with error {number}: {sql}");
                }
            }
            ["statement", "error"] => {
                let sql = take_sql(&mut lines);
                if run(&mut engine, &mut ctx, &sql).is_ok() {
                    panic!("{file}: `statement error` unexpectedly succeeded: {sql}");
                }
            }
            ["query", ..] if parts[0] == "query" => {
                let rowsort = parts.last() == Some(&"rowsort");
                let mut sql_lines = Vec::new();
                for l in lines.by_ref() {
                    if l.trim_end() == "----" {
                        break;
                    }
                    sql_lines.push(l);
                }
                let sql = sql_lines.join("\n");
                let mut expected: Vec<String> = Vec::new();
                for l in lines.by_ref() {
                    if l.trim_end().is_empty() {
                        break;
                    }
                    expected.push(l.trim_end().to_string());
                }
                let mut got = match run(&mut engine, &mut ctx, &sql) {
                    Ok(rows) => rows,
                    Err(number) => panic!("{file}: query failed with error {number}: {sql}"),
                };
                if rowsort {
                    got.sort();
                    expected.sort();
                }
                assert_eq!(got, expected, "{file}: query result mismatch:\n{sql}");
            }
            _ => panic!("{file}: unrecognized directive: {line}"),
        }
    }
    let _ = std::fs::remove_file(&db);
}

/// Reads the SQL body of a `statement` record (until a blank line or EOF).
fn take_sql<'a>(lines: &mut std::iter::Peekable<impl Iterator<Item = &'a str>>) -> String {
    let mut sql = Vec::new();
    for line in lines.by_ref() {
        if line.trim_end().is_empty() {
            break;
        }
        sql.push(line);
    }
    sql.join("\n")
}

#[test]
fn slt_corpus() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/slt");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("read slt dir")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "slt"))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "no .slt files found in {}",
        dir.display()
    );
    for file in files {
        run_file(&file);
    }
}
