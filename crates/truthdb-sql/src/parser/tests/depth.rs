use super::*;

#[test]
fn deeply_nested_parens_error_not_overflow() {
    let sql = format!("SELECT {}1{}", "(".repeat(5000), ")".repeat(5000));
    let err = Parser::parse_str(&sql).expect_err("must reject, not overflow");
    assert_eq!(err.number, 191);
}
#[test]
fn deeply_nested_from_error_not_overflow() {
    // Nested parenthesized-group FROM: must reject cleanly, not overflow.
    let group = format!("SELECT 1 FROM {}t{}", "(".repeat(5000), ")".repeat(5000));
    assert_eq!(Parser::parse_str(&group).unwrap_err().number, 191);
    // Nested derived tables likewise.
    let derived = format!(
        "SELECT * FROM {}SELECT * FROM t{} x",
        "(SELECT * FROM ".repeat(2000),
        ") y".repeat(2000),
    );
    assert_eq!(Parser::parse_str(&derived).unwrap_err().number, 191);
}
#[test]
fn deep_not_and_unary_chains_error_not_overflow() {
    let nots = format!("SELECT {}1", "NOT ".repeat(5000));
    assert_eq!(Parser::parse_str(&nots).unwrap_err().number, 191);
    // Spaced so `--` is not read as a comment.
    let neg = format!("SELECT {}1", "- ".repeat(5000));
    assert_eq!(Parser::parse_str(&neg).unwrap_err().number, 191);
}
#[test]
fn long_operator_chain_errors_not_overflow() {
    // Parses iteratively but would overflow eval; the node budget caps it.
    let sql = format!("SELECT 1{}", " OR 1".repeat(20_000));
    assert_eq!(Parser::parse_str(&sql).unwrap_err().number, 191);
}
#[test]
fn the_node_budget_is_per_expression_not_per_batch() {
    // Thousands of tiny FLAT expressions are fine — none deepens an
    // evaluation spine. A per-batch count once made a 1001-tuple INSERT
    // unparseable, which put row-lock escalation above the reachable
    // ceiling.
    let tuples: Vec<String> = (0..3000).map(|i| format!("({i}, {i})")).collect();
    let sql = format!("INSERT INTO t VALUES {}", tuples.join(", "));
    assert!(Parser::parse_str(&sql).is_ok());
    // Two statements each near the budget: legal — the budget resets.
    // Each `OR` costs ~2 nodes (the operator and its literal): 900 ORs
    // ≈ 1801 nodes, inside the 2000 budget — twice over in one batch.
    let chain = format!("SELECT 1{}", " OR 1".repeat(900));
    let batch = format!("{chain}; {chain}");
    assert!(Parser::parse_str(&batch).is_ok());
    // One expression over the budget still errors, even at the end of an
    // otherwise-light batch (1001 ORs ≈ 2003 nodes).
    let over = format!("SELECT 1; SELECT 1{}", " OR 1".repeat(1001));
    assert_eq!(Parser::parse_str(&over).unwrap_err().number, 191);
    // CTE and derived-table bodies parse under depth >= 1 (no depth-0
    // reset for their expressions): the per-statement reset must cover
    // them, or a big-but-legal statement poisons the next one's budget.
    let big = format!("SELECT 1{}", " OR 1".repeat(900));
    let derived = format!("{big}; SELECT * FROM (SELECT 1{}) d", " OR 1".repeat(200));
    assert!(Parser::parse_str(&derived).is_ok(), "derived after big");
    let cte = format!(
        "{big}; WITH c AS (SELECT 1{} AS x) SELECT x FROM c",
        " OR 1".repeat(200)
    );
    assert!(Parser::parse_str(&cte).is_ok(), "cte after big");
}
#[test]
fn reasonable_depth_is_accepted() {
    let sql = format!("SELECT {}1{}", "(".repeat(50), ")".repeat(50));
    assert!(Parser::parse_str(&sql).is_ok());
    let chain = format!("SELECT 1{}", " + 1".repeat(100));
    assert!(Parser::parse_str(&chain).is_ok());
}
