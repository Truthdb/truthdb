use super::*;

/// RAISERROR severity <= 10 arrives as an INFO token (0xAB), not an ERROR:
/// the batch succeeds and its DONE is clean.
#[tokio::test]
async fn raiserror_low_severity_is_an_info_token() {
    let path = temp_path("raiserror-info");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client
        .batch("RAISERROR('for your information', 5, 1)")
        .await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Info { number: 50000 })),
        "an INFO 50000 token: {tokens:?}"
    );
    assert!(
        !tokens.iter().any(|t| matches!(t, Token::Error { .. })),
        "no ERROR token: {tokens:?}"
    );
    // The connection stays healthy.
    let tokens = client.batch("SELECT 1 AS one").await;
    assert!(tokens.iter().any(|t| matches!(t, Token::Row(_))));

    let _ = std::fs::remove_file(&path);
}

/// Severity >= 20 is fatal: the error and its DONE are delivered, then the
/// server closes the connection — as SQL Server drops the session.
#[tokio::test]
async fn fatal_severity_closes_the_connection() {
    use tokio::io::AsyncReadExt;

    let path = temp_path("raiserror-fatal");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client
        .batch("RAISERROR('going down', 20, 1) WITH LOG")
        .await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 50000, .. })),
        "the fatal error is delivered first: {tokens:?}"
    );
    // The server hangs up: the next read is EOF (no reply will ever come).
    // Bounded, so a regression that keeps the connection open FAILS here in
    // seconds instead of hanging the suite on a read that never returns.
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.stream.read(&mut byte),
    )
    .await
    .expect("the server must close the connection after a severity-20 error")
    .expect("read after fatal");
    assert_eq!(
        read, 0,
        "the connection is closed after a severity-20 error"
    );

    let _ = std::fs::remove_file(&path);
}

/// REVIEW probe (passes): severity >= 20 inside an RPC (sp_executesql)
/// delivers the error inside the DONEPROC-framed reply and then closes the
/// connection — the RPC reply path honors the same fatal flag as a batch.
#[tokio::test]
async fn review_poc_fatal_inside_an_rpc_closes_the_connection() {
    use tokio::io::AsyncReadExt;

    let path = temp_path("review-rpc-fatal");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client
        .rpc(&sp_executesql_rpc("RAISERROR('die', 20, 1) WITH LOG"))
        .await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 50000, .. })),
        "the fatal error is delivered: {tokens:?}"
    );
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.stream.read(&mut byte),
    )
    .await
    .expect("the server must close the connection after a fatal RPC error")
    .expect("read after fatal");
    assert_eq!(read, 0, "closed after a severity-20 error in an RPC");

    let _ = std::fs::remove_file(&path);
}

/// REVIEW probe (passes): a fatal error in the FIRST RPC of a multi-RPC
/// request finishes that reply, never runs the remaining RPCs, and closes
/// the connection.
#[tokio::test]
async fn review_poc_fatal_in_a_multi_rpc_request_skips_the_rest() {
    use tokio::io::AsyncReadExt;

    let path = temp_path("review-multirpc-fatal");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let mut body = sp_executesql_rpc("RAISERROR('die', 20, 1) WITH LOG");
    body.push(0xff); // batch separator
    body.extend(sp_executesql_rpc("SELECT 1 AS a"));
    let tokens = client.rpc(&body).await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 50000, .. })),
        "the fatal error is delivered: {tokens:?}"
    );
    assert!(
        !tokens.iter().any(|t| matches!(t, Token::Row(_))),
        "the RPC after the fatal one never ran: {tokens:?}"
    );
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.stream.read(&mut byte),
    )
    .await
    .expect("the server must close the connection after a fatal error")
    .expect("read after fatal");
    assert_eq!(read, 0, "closed after the fatal RPC's reply");

    let _ = std::fs::remove_file(&path);
}

/// REVIEW probe (passes): a mid-batch RAISERROR <= 10 puts its INFO token
/// between the preceding statement's DONE and the next result set's
/// COLMETADATA — the stream-order rule the flush-before-RAISERROR exists
/// for.
#[tokio::test]
async fn review_poc_info_token_orders_between_result_sets() {
    let path = temp_path("review-info-order");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client
        .batch("SELECT 1 AS a; RAISERROR('fyi', 5, 1); SELECT 2 AS b")
        .await;
    let info = tokens
        .iter()
        .position(|t| matches!(t, Token::Info { number: 50000 }))
        .unwrap_or_else(|| panic!("an INFO token: {tokens:?}"));
    let first_done = tokens
        .iter()
        .position(|t| matches!(t, Token::Done { .. }))
        .unwrap_or_else(|| panic!("a DONE token: {tokens:?}"));
    let second_colmeta = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| matches!(t, Token::ColMetadata(_)))
        .map(|(i, _)| i)
        .nth(1)
        .unwrap_or_else(|| panic!("a second COLMETADATA: {tokens:?}"));
    assert!(
        first_done < info,
        "the first SELECT's DONE precedes the INFO: {tokens:?}"
    );
    assert!(
        info < second_colmeta,
        "the INFO precedes the second result set: {tokens:?}"
    );
    assert_eq!(row_ints(&tokens), [1, 2], "both SELECTs ran: {tokens:?}");

    let _ = std::fs::remove_file(&path);
}

/// `SET NOCOUNT ON` drops DONE_COUNT from statement DONEs on the wire —
/// results are untouched — and OFF restores it.
#[tokio::test]
async fn set_nocount_suppresses_done_counts_on_the_wire() {
    let path = temp_path("nocount");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE nc (id INT NOT NULL PRIMARY KEY)")
        .await;

    let counts = |tokens: &[Token]| -> Vec<Option<u64>> {
        tokens
            .iter()
            .filter_map(|t| match t {
                Token::Done { count, .. } => Some(*count),
                _ => None,
            })
            .collect()
    };

    let tokens = client
        .batch("SET NOCOUNT ON; INSERT INTO nc VALUES (1), (2); SELECT id FROM nc")
        .await;
    // SET, INSERT, SELECT: three DONEs, none carrying a count — but the rows
    // themselves still arrive.
    assert_eq!(
        counts(&tokens),
        [None, None, None],
        "NOCOUNT must drop every DONE count: {tokens:?}"
    );
    assert_eq!(row_ints(&tokens), [1, 2], "results are untouched");

    // The option is session-durable and OFF restores the counts.
    let tokens = client.batch("INSERT INTO nc VALUES (3)").await;
    assert_eq!(counts(&tokens), [None], "still on across batches");
    let tokens = client
        .batch("SET NOCOUNT OFF; INSERT INTO nc VALUES (4)")
        .await;
    assert_eq!(
        counts(&tokens),
        [None, Some(1)],
        "OFF restores the count: {tokens:?}"
    );

    let _ = std::fs::remove_file(&path);
}
