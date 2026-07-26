use super::*;

#[test]
fn ignorable_set_options_parse_as_noops() {
    // Cosmetic/advisory options clients send at connection time: ON/OFF
    // flags, value forms, a signed value, and a required-ON option at ON.
    // (NOCOUNT graduated to a real option in Stage 14.)
    let sql = "SET QUOTED_IDENTIFIER ON; SET ANSI_WARNINGS OFF; \
               SET TEXTSIZE 2147483647; SET DATEFORMAT mdy; SET LOCK_TIMEOUT -1";
    let stmts = Parser::parse_str(sql).expect("all recognized as no-ops");
    assert_eq!(stmts.len(), 5);
    assert!(
        stmts
            .iter()
            .all(|s| matches!(s, Statement::Set(SetStatement::Ignored))),
        "every option should parse to SetStatement::Ignored: {stmts:?}",
    );
    // NOCOUNT is a real session option now.
    let stmts = Parser::parse_str("SET NOCOUNT ON; SET NOCOUNT OFF").expect("parses");
    assert!(matches!(
        stmts.as_slice(),
        [
            Statement::Set(SetStatement::NoCount(true)),
            Statement::Set(SetStatement::NoCount(false))
        ]
    ));
    // An unknown option is still a syntax error, not silently ignored.
    assert_eq!(Parser::parse_str("SET WHATSIT ON").unwrap_err().number, 102);
}
#[test]
fn result_changing_set_options_are_not_silently_ignored() {
    // OFF for an option TruthDB hardwires to ON must be rejected, never
    // silently accepted (it would change query results).
    assert_eq!(
        Parser::parse_str("SET ANSI_NULLS OFF").unwrap_err().number,
        102,
    );
    assert_eq!(
        Parser::parse_str("SET CONCAT_NULL_YIELDS_NULL OFF")
            .unwrap_err()
            .number,
        102,
    );
    // ...but the matching ON is a faithful no-op.
    assert!(matches!(
        Parser::parse_str("SET ANSI_NULLS ON").as_deref(),
        Ok([Statement::Set(SetStatement::Ignored)]),
    ));
    // Options that change what/how much runs stay hard errors, not no-ops,
    // so we never silently drop a client's row cap or skip flag.
    for sql in [
        "SET ROWCOUNT 100",
        "SET NOEXEC ON",
        "SET IMPLICIT_TRANSACTIONS ON",
    ] {
        assert_eq!(
            Parser::parse_str(sql).unwrap_err().number,
            102,
            "{sql} must not be a silent no-op",
        );
    }
}
