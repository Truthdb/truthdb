use std::path::PathBuf;

use crate::engine::test_support::*;
use crate::engine::*;
use crate::relstore::types::Datum;

/// The integer `id` column (column 0) of the first rowset in an outcome.
fn ids(outcome: &BatchOutcome) -> Vec<i32> {
    for result in &outcome.results {
        if let StatementResult::Rows(rowset) = result {
            return rowset
                .rows
                .iter()
                .map(|row| match row[0] {
                    Datum::TinyInt(v) => v as i32,
                    Datum::SmallInt(v) => v as i32,
                    Datum::Int(v) => v,
                    Datum::BigInt(v) => v as i32,
                    ref other => panic!("expected integer id, got {other:?}"),
                })
                .collect();
        }
    }
    panic!("no rowset in outcome: {:?}", outcome.results);
}

/// Every rowset's integer column 0, in statement order (a batch with
/// TRY/CATCH can emit several rowsets).
fn all_int_rows(outcome: &BatchOutcome) -> Vec<Vec<i32>> {
    outcome
        .results
        .iter()
        .filter_map(|r| match r {
            StatementResult::Rows(rowset) => Some(
                rowset
                    .rows
                    .iter()
                    .map(|row| match row[0] {
                        Datum::TinyInt(v) => v as i32,
                        Datum::SmallInt(v) => v as i32,
                        Datum::Int(v) => v,
                        Datum::BigInt(v) => v as i32,
                        ref other => panic!("expected integer, got {other:?}"),
                    })
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// Plan text lines for a SELECT under SHOWPLAN_TEXT (one batch so the SET
/// persists to the SELECT).
fn plan_lines(engine: &Engine, select: &str) -> Vec<String> {
    let env = sql(engine, &format!("SET SHOWPLAN_TEXT ON; {select}"));
    let results = env["results"].as_array().expect("results array");
    let rows = results
        .iter()
        .find(|r| r["type"] == "rows")
        .unwrap_or_else(|| panic!("no plan rows in {env}"));
    rows["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r[0].as_str().unwrap().to_string())
        .collect()
}

mod aggregation_joins;
mod collation;
mod core_sql;
mod databases;
mod indexes;
mod locking;
mod procedural_control_flow;
mod procedural_errors;
mod query;
mod routines;
mod scans;
mod security;
mod transactions;
mod triggers;
