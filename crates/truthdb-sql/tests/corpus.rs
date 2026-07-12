//! Parser corpus: every `.sql` file under `tests/corpus/ok/` must parse, and
//! its debug-formatted AST must match the sibling `.ast` golden; every file
//! under `tests/corpus/err/` must fail to parse with the SQL error number
//! named on the first line (`-- error: <n>`). New parser bugs become new
//! corpus files.
//!
//! Refresh goldens with `TRUTHDB_BLESS=1 cargo test -p truthdb-sql --test corpus`.

use std::path::Path;

fn bless() -> bool {
    std::env::var("TRUTHDB_BLESS").is_ok()
}

#[test]
fn corpus_ok() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/ok");
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).expect("read ok corpus dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let sql = std::fs::read_to_string(&path).expect("read sql");
        let statements = truthdb_sql::parse(&sql)
            .unwrap_or_else(|err| panic!("{}: expected parse OK, got {err}", path.display()));
        let actual = format!("{statements:#?}\n");
        let golden = path.with_extension("ast");
        if bless() {
            std::fs::write(&golden, &actual).expect("write golden");
        } else {
            let expected = std::fs::read_to_string(&golden)
                .unwrap_or_else(|_| panic!("missing golden {}", golden.display()));
            assert_eq!(actual, expected, "AST mismatch for {}", path.display());
        }
        checked += 1;
    }
    assert!(checked > 0, "no ok-corpus files found");
}

#[test]
fn corpus_err() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/err");
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).expect("read err corpus dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let sql = std::fs::read_to_string(&path).expect("read sql");
        let expected_number: i32 = sql
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("-- error:"))
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or_else(|| panic!("{}: first line must be `-- error: <n>`", path.display()));
        let err = truthdb_sql::parse(&sql)
            .err()
            .unwrap_or_else(|| panic!("{}: expected a parse error", path.display()));
        assert_eq!(
            err.number,
            expected_number,
            "{}: expected error {expected_number}, got {} ({})",
            path.display(),
            err.number,
            err.message
        );
        checked += 1;
    }
    assert!(checked > 0, "no err-corpus files found");
}
