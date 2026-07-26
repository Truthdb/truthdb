use std::path::{Path, PathBuf};

use serde_json::Value;

use super::{BatchOutcome, Engine, TxnContext};
use crate::storage::{Storage, StorageOptions};

pub(in crate::engine) fn test_storage_options() -> StorageOptions {
    StorageOptions {
        size_gib: 1,
        wal_ratio: 0.05,
        metadata_ratio: 0.08,
        snapshot_ratio: 0.02,
        allocator_ratio: 0.02,
        reserved_ratio: 0.17,
        default_collation: None,
    }
}

pub(in crate::engine) fn unique_temp_path(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    path.push(format!("truthdb-{label}-{nanos}.db"));
    path
}

/// Extracts one integer column from a SELECT via the SQL envelope.
pub(in crate::engine) fn sql_column_i64(engine: &Engine, sql: &str, column: usize) -> Vec<i64> {
    let response = engine.execute(sql).expect("sql");
    let response: Value = serde_json::from_str(&response).expect("json");
    assert_eq!(
        response["kind"], "sql",
        "expected a rows envelope: {response}"
    );
    response["results"][0]["rows"]
        .as_array()
        .expect("rows array")
        .iter()
        .map(|row| row[column].as_str().expect("cell").parse().expect("i64"))
        .collect()
}

/// Runs SQL and returns the parsed envelope.
pub(in crate::engine) fn sql(engine: &Engine, text: &str) -> Value {
    let response = engine.execute(text).expect("execute");
    serde_json::from_str(&response).expect("json envelope")
}

/// Runs SQL expected to error and returns the SQL error number from the
/// envelope's trailing `error`.
pub(in crate::engine) fn sql_error_number(engine: &Engine, text: &str) -> i64 {
    let env = sql(engine, text);
    env["error"]["number"]
        .as_i64()
        .unwrap_or_else(|| panic!("expected an error envelope, got {env}"))
}

/// Runs a single-statement SELECT and returns its (columns, rows) where
/// each cell is `Option<String>` (None = NULL).
pub(in crate::engine) fn sql_rows(
    engine: &Engine,
    text: &str,
) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let env = sql(engine, text);
    assert_eq!(env["kind"], "sql", "expected rows, got {env}");
    let result = &env["results"][0];
    assert_eq!(result["type"], "rows", "expected a rowset, got {result}");
    let columns = result["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap().to_string())
        .collect();
    let rows = result["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            row.as_array()
                .unwrap()
                .iter()
                .map(|cell| cell.as_str().map(str::to_string))
                .collect()
        })
        .collect();
    (columns, rows)
}

pub(in crate::engine) fn new_engine(path: &Path) -> Engine {
    let storage = Storage::create(path.to_path_buf(), test_storage_options()).expect("create");
    Engine::new(storage).expect("engine")
}

/// A table's catalog object id (via `sys.tables`).
pub(in crate::engine) fn table_object_id(engine: &Engine, name: &str) -> u32 {
    let (_, rows) = sql_rows(
        engine,
        &format!("SELECT object_id FROM sys.tables WHERE name = '{name}'"),
    );
    rows[0][0]
        .as_ref()
        .expect("object_id")
        .parse()
        .expect("u32")
}

/// Runs a SQL batch through the session path with a persistent transaction
/// context (as a TDS connection would), returning the typed outcome.
pub(in crate::engine) fn batch(engine: &Engine, ctx: &mut TxnContext, sql: &str) -> BatchOutcome {
    engine.sql_batch(sql, ctx).expect("sql_batch")
}
