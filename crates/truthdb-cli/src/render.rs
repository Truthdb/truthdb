//! Renders server responses for the REPL. SQL responses arrive as a
//! `{"kind":"sql"|"error",...}` envelope (aligned tables, `(N rows
//! affected)`, `Msg <n>` errors); anything else is a legacy ES response,
//! printed as-is.

use serde_json::Value;

/// Renders one response `message` (with the transport `ok` flag) to a
/// printable string.
pub fn render(ok: bool, message: &str) -> String {
    match serde_json::from_str::<Value>(message) {
        Ok(value) => match value.get("kind").and_then(Value::as_str) {
            Some("sql") => render_sql(&value),
            Some("error") => render_error(&value),
            _ => render_legacy(ok, message),
        },
        Err(_) => render_legacy(ok, message),
    }
}

fn render_sql(envelope: &Value) -> String {
    let mut out = String::new();
    let results = envelope
        .get("results")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for result in &results {
        match result.get("type").and_then(Value::as_str) {
            Some("rows") => {
                let columns = string_vec(result.get("columns"));
                let rows: Vec<Vec<Option<String>>> = result
                    .get("rows")
                    .and_then(Value::as_array)
                    .map(|rows| rows.iter().map(cell_row).collect())
                    .unwrap_or_default();
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(&render_table(&columns, &rows));
                out.push_str(&format!(
                    "\n({} row{} affected)",
                    rows.len(),
                    plural(rows.len())
                ));
            }
            Some("count") => {
                let n = result
                    .get("rows_affected")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(&format!("({n} row{} affected)", plural(n as usize)));
            }
            // "done" (DDL): print nothing, like sqlcmd.
            _ => {}
        }
    }
    // A trailing error (a batch that stopped mid-way) prints after the
    // results of the statements that did run.
    if let Some(error) = envelope.get("error").filter(|e| !e.is_null()) {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&render_error(error));
    }
    out
}

fn render_error(envelope: &Value) -> String {
    let number = envelope.get("number").and_then(Value::as_i64).unwrap_or(0);
    let level = envelope.get("level").and_then(Value::as_u64).unwrap_or(0);
    let state = envelope.get("state").and_then(Value::as_u64).unwrap_or(0);
    let message = envelope
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("");
    format!("Msg {number}, Level {level}, State {state}\n{message}")
}

fn render_legacy(ok: bool, message: &str) -> String {
    let status = if ok { "ok" } else { "err" };
    if message.contains('\n') {
        format!("[{status}]\n{message}")
    } else {
        format!("[{status}] {message}")
    }
}

/// Renders an aligned ASCII table with a header underline, sqlcmd-style.
fn render_table(columns: &[String], rows: &[Vec<Option<String>>]) -> String {
    let ncols = columns.len();
    let mut widths: Vec<usize> = columns.iter().map(|c| display_width(c)).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate().take(ncols) {
            widths[i] = widths[i].max(display_width(&cell_text(cell)));
        }
    }

    let mut out = String::new();
    // Header.
    for (i, column) in columns.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&pad(column, widths[i]));
    }
    out.push('\n');
    // Underline.
    for (i, width) in widths.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&"-".repeat(*width));
    }
    // Rows.
    for row in rows {
        out.push('\n');
        for (i, width) in widths.iter().enumerate() {
            if i > 0 {
                out.push(' ');
            }
            let text = row.get(i).map(cell_text).unwrap_or_default();
            out.push_str(&pad(&text, *width));
        }
    }
    out
}

fn cell_row(value: &Value) -> Vec<Option<String>> {
    value
        .as_array()
        .map(|cells| cells.iter().map(cell_value).collect())
        .unwrap_or_default()
}

fn cell_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn cell_text(cell: &Option<String>) -> String {
    match cell {
        None => "NULL".to_string(),
        // Newlines/tabs in a cell would break the aligned grid; render them
        // as spaces so column widths stay meaningful.
        Some(s) => s.replace(['\n', '\r', '\t'], " "),
    }
}

fn string_vec(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|v| v.as_str().unwrap_or("").to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn display_width(s: &str) -> usize {
    s.chars().count()
}

fn pad(s: &str, width: usize) -> String {
    let len = display_width(s);
    if len >= width {
        s.to_string()
    } else {
        format!("{s}{}", " ".repeat(width - len))
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_a_rows_envelope_as_aligned_table() {
        let message = r#"{"kind":"sql","results":[{"type":"rows","columns":["id","name"],"rows":[["1","Skor"],["2",null]]}]}"#;
        let out = render(true, message);
        let expected = "id name\n-- ----\n1  Skor\n2  NULL\n(2 rows affected)";
        assert_eq!(out, expected);
    }

    #[test]
    fn renders_count_and_done() {
        let count = r#"{"kind":"sql","results":[{"type":"count","rows_affected":3}]}"#;
        assert_eq!(render(true, count), "(3 rows affected)");
        let done = r#"{"kind":"sql","results":[{"type":"done"}]}"#;
        assert_eq!(render(true, done), "");
        let one = r#"{"kind":"sql","results":[{"type":"count","rows_affected":1}]}"#;
        assert_eq!(render(true, one), "(1 row affected)");
    }

    #[test]
    fn renders_error_envelope() {
        let message = r#"{"kind":"error","number":2627,"level":14,"state":1,"message":"Violation of PRIMARY KEY constraint."}"#;
        assert_eq!(
            render(false, message),
            "Msg 2627, Level 14, State 1\nViolation of PRIMARY KEY constraint."
        );
    }

    #[test]
    fn falls_back_to_legacy_for_es_responses() {
        let message = r#"{"acknowledged":true,"index":"products"}"#;
        let out = render(true, message);
        assert!(out.starts_with("[ok]"), "got: {out}");
    }
}
