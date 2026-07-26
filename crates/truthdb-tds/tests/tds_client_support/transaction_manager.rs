use super::*;

#[tokio::test]
async fn mismatched_transaction_descriptor_is_rejected() {
    // A descriptor the server never handed out means the client's transaction
    // view has desynchronised: the request must not run.
    let path = temp_path("bad-descriptor");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    // No transaction is open, so the connection's descriptor is 0; claim 42.
    client
        .raw_batch(&headers_block(&all_headers(42)), "SELECT 1")
        .await;
    assert!(
        client.try_read_message().await.is_none(),
        "a mismatched transaction descriptor must close the connection"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn transaction_descriptor_round_trips_through_envchange() {
    // The server mints a descriptor on TM begin, the client echoes it on the
    // next request, and it returns to 0 after commit. This is what the
    // validation above enforces, so pin the values rather than only the flow.
    let path = temp_path("descriptor-roundtrip");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    assert_eq!(client.tran_descriptor, 0, "no transaction at login");

    client.tm_request(TM_BEGIN_XACT, 0).await;
    let in_txn = client.tran_descriptor;
    assert_ne!(in_txn, 0, "begin must mint a non-zero descriptor");

    // The next request echoes it and is accepted (the server validates it).
    let rows = client.batch("SELECT 1").await;
    assert!(
        rows.iter().any(|t| matches!(t, Token::Row(_))),
        "echoing the descriptor must be accepted: {rows:?}"
    );
    assert_eq!(
        client.tran_descriptor, in_txn,
        "descriptor is stable in-txn"
    );

    client.tm_request(TM_COMMIT_XACT, 0).await;
    assert_eq!(client.tran_descriptor, 0, "commit clears the descriptor");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn begin_after_a_batch_commit_is_accepted_with_a_placeholder_descriptor() {
    // Regression: go-mssqldb hardcodes descriptor 0 on TM begin while using the
    // live descriptor everywhere else. Committing via a SQL batch leaves the
    // server's descriptor non-zero (the batch path emits no ENVCHANGE), so a
    // following begin arrives claiming 0 against a non-zero descriptor.
    // Validating a begin would kill this correct client's connection.
    let path = temp_path("begin-placeholder");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;

    // Begin via TM, then commit via a SQL batch: the server's descriptor stays
    // non-zero because only TM requests move it.
    client.tm_request(TM_BEGIN_XACT, 0).await;
    assert_ne!(client.tran_descriptor, 0, "begin minted a descriptor");
    client.batch("INSERT INTO t VALUES (1)").await;
    client.batch("COMMIT TRANSACTION").await;

    // A second begin, carrying go-mssqldb's placeholder 0, must be accepted.
    let begin = client.tm_request(TM_BEGIN_XACT, 0).await;
    assert!(
        has_envchange(&begin, 8),
        "a begin with a placeholder descriptor must be accepted: {begin:?}"
    );
    let rows = client.batch("SELECT id FROM t").await;
    assert!(
        rows.iter().any(|t| matches!(t, Token::Row(_))),
        "the connection is still usable: {rows:?}"
    );
    client.tm_request(TM_ROLLBACK_XACT, 0).await;
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn nested_tm_commit_keeps_the_transaction_and_its_descriptor() {
    // A nested COMMIT (@@TRANCOUNT 2 -> 1) does not end the transaction, so it
    // must not announce one ending: emitting ENVCHANGE 9 here would contradict
    // the same reply's DONE(INXACT) and zero a descriptor the client is still
    // meant to send.
    let path = temp_path("nested-tm-commit");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;

    client.tm_request(TM_BEGIN_XACT, 0).await;
    let descriptor = client.tran_descriptor;
    assert_ne!(descriptor, 0);
    // Nest a second transaction via SQL: @@TRANCOUNT is now 2.
    client.batch("BEGIN TRANSACTION").await;

    // The inner commit only decrements @@TRANCOUNT: still in a transaction.
    let commit = client.tm_request(TM_COMMIT_XACT, 0).await;
    assert!(
        !has_envchange(&commit, 9),
        "a nested commit must not announce the transaction ending: {commit:?}"
    );
    assert!(
        commit
            .iter()
            .any(|t| matches!(t, Token::Done { in_xact: true, .. })),
        "still in a transaction: {commit:?}"
    );
    assert_eq!(
        client.tran_descriptor, descriptor,
        "the descriptor survives a nested commit"
    );

    // The outer commit ends it: now the ENVCHANGE fires and clears it.
    let commit = client.tm_request(TM_COMMIT_XACT, 0).await;
    assert!(
        has_envchange(&commit, 9),
        "outer commit ends it: {commit:?}"
    );
    assert_eq!(client.tran_descriptor, 0);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn tm_begin_commit_persists_and_emits_envchanges() {
    let path = temp_path("tm-commit");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;

    // db.BeginTx(): a TM begin request → ENVCHANGE(8) + DONE(INXACT).
    let begin = client.tm_request(TM_BEGIN_XACT, 0).await;
    assert!(has_envchange(&begin, 8), "begin tokens: {begin:?}");
    assert!(
        begin
            .iter()
            .any(|t| matches!(t, Token::Done { in_xact: true, .. })),
        "begin DONE must set INXACT: {begin:?}"
    );

    // A statement inside the transaction reports it is still in a transaction.
    let insert = client.batch("INSERT INTO t VALUES (1)").await;
    assert!(
        insert
            .iter()
            .any(|t| matches!(t, Token::Done { in_xact: true, .. })),
        "in-txn statement DONE must set INXACT: {insert:?}"
    );

    // Commit → ENVCHANGE(9) + DONE without INXACT.
    let commit = client.tm_request(TM_COMMIT_XACT, 0).await;
    assert!(has_envchange(&commit, 9), "commit tokens: {commit:?}");
    assert!(
        commit
            .iter()
            .any(|t| matches!(t, Token::Done { in_xact: false, .. })),
        "commit DONE must clear INXACT: {commit:?}"
    );

    // The committed row is durable and visible after the transaction.
    let select = client.batch("SELECT id FROM t").await;
    assert_eq!(row_ints(&select), vec![1]);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn tm_begin_rollback_discards_writes() {
    let path = temp_path("tm-rollback");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .await;
    client.batch("INSERT INTO t VALUES (1)").await;

    client.tm_request(TM_BEGIN_XACT, 0).await;
    client.batch("INSERT INTO t VALUES (2)").await;

    // Rollback → ENVCHANGE(10); the second insert is discarded.
    let rollback = client.tm_request(TM_ROLLBACK_XACT, 0).await;
    assert!(
        has_envchange(&rollback, 10),
        "rollback tokens: {rollback:?}"
    );

    let select = client.batch("SELECT id FROM t ORDER BY id").await;
    assert_eq!(row_ints(&select), vec![1], "only the pre-txn row survives");
    let _ = std::fs::remove_file(path);
}
