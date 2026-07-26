use super::*;

#[test]
fn assignment_select_parses_as_assign_item() {
    // `SELECT @v = expr` is an assignment item, not a boolean comparison.
    let stmts = Parser::parse_str("SELECT @v = 1 + 2").expect("parse");
    let Statement::Select(select) = &stmts[0] else {
        panic!("expected select")
    };
    assert!(
        matches!(&select.items[0], SelectItem::Assign { target, .. } if target == "v"),
        "expected an assignment item: {:?}",
        select.items[0]
    );

    // `@v = x` inside a WHERE stays a comparison (only the item list assigns).
    let stmts = Parser::parse_str("SELECT 1 WHERE @v = 5").expect("parse");
    let Statement::Select(select) = &stmts[0] else {
        panic!("expected select")
    };
    assert!(matches!(&select.items[0], SelectItem::Expr { .. }));
    assert!(select.where_clause.is_some());

    // Mixing an assignment with a result column is a syntax-level error 141.
    assert_eq!(
        Parser::parse_str("SELECT @v = 1, 2").unwrap_err().number,
        141,
    );
}
