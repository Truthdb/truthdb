use super::*;
use crate::parser::Parser;
use crate::value::SqlValue;

fn eval_predicate(sql: &str, columns: &[&str], row: &[SqlValue]) -> SqlValue {
    // Parse `SELECT <expr>` and evaluate the single item.
    let statements = Parser::parse_str(&format!("SELECT {sql}")).expect("parse");
    let select = match &statements[0] {
        crate::ast::Statement::Select(s) => s,
        _ => panic!("expected select"),
    };
    let expr = match &select.items[0] {
        crate::ast::SelectItem::Expr { expr, .. } => expr,
        _ => panic!("expected expr"),
    };
    let names: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
    eval(expr, row, &names, &EvalContext::default()).expect("eval")
}

#[test]
fn arithmetic_and_precedence() {
    assert_eq!(eval_predicate("1 + 2 * 3", &[], &[]), SqlValue::Int(7));
    assert_eq!(eval_predicate("(1 + 2) * 3", &[], &[]), SqlValue::Int(9));
    assert_eq!(eval_predicate("7 / 2", &[], &[]), SqlValue::Int(3));
    assert_eq!(eval_predicate("7 % 3", &[], &[]), SqlValue::Int(1));
    assert_eq!(eval_predicate("-5 + 2", &[], &[]), SqlValue::Int(-3));
}

#[test]
fn null_arithmetic_is_null() {
    assert_eq!(eval_predicate("1 + NULL", &[], &[]), SqlValue::Null);
}

#[test]
fn session_identity_intrinsics() {
    let ctx = EvalContext {
        database: "truthdb".to_string(),
        databases: vec![(1, "truthdb".to_string())],
        login: "sa".to_string(),
        spid: 53,
        ..EvalContext::default()
    };
    let eval_ctx = |sql: &str| {
        let statements = Parser::parse_str(&format!("SELECT {sql}")).expect("parse");
        let crate::ast::Statement::Select(select) = &statements[0] else {
            panic!("expected select")
        };
        let crate::ast::SelectItem::Expr { expr, .. } = &select.items[0] else {
            panic!("expected expr")
        };
        let no_columns: Vec<String> = Vec::new();
        eval(expr, &[], &no_columns, &ctx).expect("eval")
    };
    assert_eq!(eval_ctx("DB_NAME()"), SqlValue::Str("truthdb".to_string()));
    assert_eq!(eval_ctx("DB_NAME(1)"), SqlValue::Str("truthdb".to_string()));
    assert_eq!(eval_ctx("SUSER_SNAME()"), SqlValue::Str("sa".to_string()));
    assert_eq!(eval_ctx("SUSER_NAME()"), SqlValue::Str("sa".to_string()));
    assert_eq!(eval_ctx("@@SPID"), SqlValue::Int(53));
}

#[test]
fn divide_by_zero_errors() {
    let statements = Parser::parse_str("SELECT 1 / 0").unwrap();
    let crate::ast::Statement::Select(select) = &statements[0] else {
        panic!()
    };
    let crate::ast::SelectItem::Expr { expr, .. } = &select.items[0] else {
        panic!()
    };
    let empty: Vec<String> = Vec::new();
    assert_eq!(
        eval(expr, &[], &empty, &EvalContext::default())
            .unwrap_err()
            .number,
        8134
    );
}

#[test]
fn three_valued_comparisons() {
    assert_eq!(eval_predicate("1 = 1", &[], &[]), SqlValue::Bool(true));
    assert_eq!(eval_predicate("1 = 2", &[], &[]), SqlValue::Bool(false));
    assert_eq!(eval_predicate("1 = NULL", &[], &[]), SqlValue::Null);
    assert_eq!(eval_predicate("NULL <> 1", &[], &[]), SqlValue::Null);
}

#[test]
fn is_null_is_two_valued() {
    assert_eq!(
        eval_predicate("x IS NULL", &["x"], &[SqlValue::Null]),
        SqlValue::Bool(true)
    );
    assert_eq!(
        eval_predicate("x IS NOT NULL", &["x"], &[SqlValue::Null]),
        SqlValue::Bool(false)
    );
    assert_eq!(
        eval_predicate("x IS NULL", &["x"], &[SqlValue::Int(5)]),
        SqlValue::Bool(false)
    );
}

#[test]
fn boolean_connectives_over_null() {
    // NULL AND FALSE = FALSE; NULL AND TRUE = NULL; NULL OR TRUE = TRUE.
    assert_eq!(
        eval_predicate("x = 1 AND 1 = 2", &["x"], &[SqlValue::Null]),
        SqlValue::Bool(false)
    );
    assert_eq!(
        eval_predicate("x = 1 AND 1 = 1", &["x"], &[SqlValue::Null]),
        SqlValue::Null
    );
    assert_eq!(
        eval_predicate("x = 1 OR 1 = 1", &["x"], &[SqlValue::Null]),
        SqlValue::Bool(true)
    );
    assert_eq!(
        eval_predicate("NOT (x = 1)", &["x"], &[SqlValue::Null]),
        SqlValue::Null
    );
}

#[test]
fn large_valid_chain_evaluates_without_overflow() {
    // A left-leaning OR chain within the eval depth budget evaluates,
    // recursing down its spine without overflowing the stack.
    let sql = format!("1{}", " OR 1".repeat(400));
    assert_eq!(eval_predicate(&sql, &[], &[]), SqlValue::Bool(true));
}

#[test]
fn over_deep_chain_errors_not_overflow() {
    // Past the depth budget eval fails cleanly (191), never overflowing.
    let sql = format!("1{}", " OR 1".repeat(700));
    let statements = Parser::parse_str(&format!("SELECT {sql}")).unwrap();
    let crate::ast::Statement::Select(select) = &statements[0] else {
        panic!()
    };
    let crate::ast::SelectItem::Expr { expr, .. } = &select.items[0] else {
        panic!()
    };
    let empty: Vec<String> = Vec::new();
    assert_eq!(
        eval(expr, &[], &empty, &EvalContext::default())
            .unwrap_err()
            .number,
        191
    );
}

#[test]
fn column_reference_resolution() {
    assert_eq!(
        eval_predicate("price * 2", &["price"], &[SqlValue::Int(50)]),
        SqlValue::Int(100)
    );
    // Case-insensitive.
    assert_eq!(
        eval_predicate("PRICE + 1", &["price"], &[SqlValue::Int(9)]),
        SqlValue::Int(10)
    );
}
