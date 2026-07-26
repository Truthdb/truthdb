use super::*;

#[test]
fn try_catch_parses_into_blocks() {
    let stmts = Parser::parse_str(
        "BEGIN TRY \
           INSERT INTO t VALUES (1); \
           SELECT 2; \
         END TRY \
         BEGIN CATCH \
           SELECT ERROR_NUMBER(); \
         END CATCH",
    )
    .expect("parse");
    let Statement::TryCatch {
        try_block,
        catch_block,
        ..
    } = &stmts[0]
    else {
        panic!("expected a TRY/CATCH, got {:?}", stmts[0]);
    };
    assert_eq!(try_block.len(), 2, "two statements in the TRY block");
    assert_eq!(catch_block.len(), 1, "one statement in the CATCH block");
    assert!(matches!(try_block[0], Statement::Insert(_)));

    // A nested TRY inside the TRY block is consumed whole (its END TRY / END
    // CATCH do not close the outer block).
    let stmts = Parser::parse_str(
        "BEGIN TRY \
           BEGIN TRY SELECT 1; END TRY BEGIN CATCH SELECT 2; END CATCH; \
           SELECT 3; \
         END TRY \
         BEGIN CATCH SELECT 4; END CATCH",
    )
    .expect("parse");
    let Statement::TryCatch { try_block, .. } = &stmts[0] else {
        panic!("expected a TRY/CATCH");
    };
    assert_eq!(try_block.len(), 2, "nested TRY + the following SELECT");
    assert!(matches!(try_block[0], Statement::TryCatch { .. }));

    // An unterminated TRY block is a syntax error, not a hang.
    assert_eq!(
        Parser::parse_str("BEGIN TRY SELECT 1;").unwrap_err().number,
        102,
    );
}
#[test]
fn try_catch_parses_without_statement_terminators() {
    // The canonical T-SQL form omits the `;` before END TRY / END CATCH.
    // `END` must not be read as an implicit alias for the preceding select
    // item (or table), which would leave the cursor on `TRY`.
    for sql in [
        "BEGIN TRY SELECT 1 END TRY BEGIN CATCH SELECT 2 END CATCH",
        "BEGIN TRY SELECT * FROM t END TRY BEGIN CATCH SELECT 2 END CATCH",
        "BEGIN TRY SELECT a FROM t WHERE a = 1 END TRY \
         BEGIN CATCH SELECT ERROR_MESSAGE() END CATCH",
    ] {
        let stmts = Parser::parse_str(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let Statement::TryCatch {
            try_block,
            catch_block,
            ..
        } = &stmts[0]
        else {
            panic!("expected a TRY/CATCH for {sql}");
        };
        assert_eq!(try_block.len(), 1, "{sql}");
        assert_eq!(catch_block.len(), 1, "{sql}");
    }

    // An explicit `AS end` still aliases (only the *bare* END is declined),
    // and a delimited [end] is an identifier, not the block terminator.
    let stmts = Parser::parse_str("SELECT 1 AS end").expect("AS end still aliases");
    assert!(matches!(stmts[0], Statement::Select(_)));
    let stmts = Parser::parse_str("SELECT 1 [end]").expect("[end] still aliases");
    assert!(matches!(stmts[0], Statement::Select(_)));
}
