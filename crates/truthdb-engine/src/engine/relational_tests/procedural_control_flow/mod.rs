use super::*;

/// The first rowset's first row, as `i32`s across its columns.
fn row_ints(outcome: &BatchOutcome) -> Vec<i32> {
    for result in &outcome.results {
        if let StatementResult::Rows(rowset) = result {
            return rowset.rows[0]
                .iter()
                .map(|d| match d {
                    Datum::TinyInt(v) => *v as i32,
                    Datum::SmallInt(v) => *v as i32,
                    Datum::Int(v) => *v,
                    Datum::BigInt(v) => *v as i32,
                    other => panic!("expected integer, got {other:?}"),
                })
                .collect();
        }
    }
    panic!("no rowset in outcome: {:?}", outcome.results);
}

mod core;
mod review;
