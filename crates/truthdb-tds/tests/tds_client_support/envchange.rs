use super::*;

/// The login response advertises the connection's default SQL collation
/// (ENVCHANGE 7) — mssql-jdbc dereferences it to encode every NVARCHAR RPC
/// parameter and NPEs client-side without it — and every DONE stamps its
/// statement's command class in CurCmd, which the same driver requires
/// before it accepts a DONE's row count (executeUpdate returns -1 without
/// it). Both regressions previously survived every in-repo test.
#[tokio::test]
async fn login_advertises_collation_and_dones_carry_their_command_class() {
    let path = temp_path("curcmd");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    let login = client.login("sa", "secret").await;
    assert!(
        has_envchange(&login, 7),
        "login must carry ENVCHANGE 7 (SQL collation): {login:?}"
    );

    client
        .batch("CREATE TABLE cc (id INT NOT NULL PRIMARY KEY)")
        .await;
    // The REAL engine's statement→command mapping, batch path: INSERT 0xC3,
    // UPDATE 0xC5, SELECT 0xC1, DELETE 0xC4 on the statement's own DONE.
    for (sql, want) in [
        ("INSERT INTO cc VALUES (1), (2)", 0xc3u16),
        ("UPDATE cc SET id = 3 WHERE id = 1", 0xc5),
        ("SELECT id FROM cc ORDER BY id", 0xc1),
        ("DELETE FROM cc WHERE id = 3", 0xc4),
    ] {
        let tokens = client.batch(sql).await;
        let cmds: Vec<u16> = tokens
            .iter()
            .filter_map(|t| match t {
                Token::Done { cmd, .. } => Some(*cmd),
                _ => None,
            })
            .collect();
        assert_eq!(cmds, [want], "{sql}: {tokens:?}");
    }

    // The RPC path: the DONEINPROC carries the statement's class, the final
    // DONEPROC carries EXECUTE (0xE0).
    let tokens = client
        .rpc(&sp_executesql_rpc("INSERT INTO cc VALUES (9)"))
        .await;
    let inproc: Vec<u16> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::DoneInProc { cmd, .. } => Some(*cmd),
            _ => None,
        })
        .collect();
    assert_eq!(inproc, [0xc3], "tokens: {tokens:?}");
    let procs: Vec<u16> = tokens
        .iter()
        .filter_map(|t| match t {
            Token::DoneProc { cmd, .. } => Some(*cmd),
            _ => None,
        })
        .collect();
    assert_eq!(procs, [0xe0], "tokens: {tokens:?}");

    let _ = std::fs::remove_file(&path);
}

/// `USE` answers with the database-context ENVCHANGE (type 1) and the 5701
/// INFO — the exact tokens SSMS listens for — before the statement's DONE;
/// a wrong database is 911 and emits neither.
#[tokio::test]
async fn use_statement_emits_the_database_envchange() {
    let path = temp_path("use-envchange");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client.batch("USE truthdb").await;
    assert!(
        has_envchange(&tokens, 1),
        "USE must emit ENVCHANGE 1 (database): {tokens:?}"
    );
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Info { number: 5701 })),
        "USE must emit INFO 5701: {tokens:?}"
    );

    let tokens = client.batch("USE somewhere_else").await;
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 911, .. })),
        "a wrong database is 911: {tokens:?}"
    );
    assert!(
        !has_envchange(&tokens, 1),
        "a failed USE must not announce a context change: {tokens:?}"
    );

    let _ = std::fs::remove_file(&path);
}

/// REVIEW PoC: SQL Server emits a prior statement's DONE before a later
/// USE's ENVCHANGE. TruthDB's core-side DONE deferral lets the ENVCHANGE
/// jump the queue: for "INSERT ...; USE truthdb" the ENVCHANGE (and INFO
/// 5701) reach the wire before the INSERT's DONE.
#[tokio::test]
async fn use_envchange_follows_prior_statement_done() {
    let path = temp_path("use-order");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;
    client
        .batch("CREATE TABLE uo (id INT NOT NULL PRIMARY KEY)")
        .await;

    let tokens = client.batch("INSERT INTO uo VALUES (1); USE truthdb").await;
    let done_at = tokens
        .iter()
        .position(|t| matches!(t, Token::Done { count: Some(1), .. }))
        .expect("the INSERT's DONE");
    let env_at = tokens
        .iter()
        .position(|t| matches!(t, Token::EnvChange { kind: 1, .. }))
        .expect("the USE's ENVCHANGE");
    assert!(
        done_at < env_at,
        "SQL Server order: DONE(insert) before ENVCHANGE(database); got {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// REVIEW PoC: `sp_executesql N'USE truthdb'` over RPC — ENVCHANGE 1 + INFO
/// 5701 arrive, the statement's DONE is a DONEINPROC, and the RPC tail
/// (RETURNSTATUS, DONEPROC) stays framed.
#[tokio::test]
async fn use_inside_exec_rpc_keeps_doneinproc_framing() {
    let path = temp_path("use-rpc");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client.rpc(&sp_executesql_rpc("USE truthdb")).await;
    assert!(has_envchange(&tokens, 1), "ENVCHANGE 1: {tokens:?}");
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Info { number: 5701 })),
        "INFO 5701: {tokens:?}"
    );
    assert!(
        tokens.iter().any(|t| matches!(t, Token::DoneInProc { .. })),
        "the USE's DONE renders as DONEINPROC: {tokens:?}"
    );
    assert!(
        tokens.iter().any(|t| matches!(t, Token::ReturnStatus(0))),
        "RETURNSTATUS: {tokens:?}"
    );
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::DoneProc { error: false, .. })),
        "DONEPROC: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// REVIEW PoC: two USEs in one batch — each emits its own ENVCHANGE + INFO.
#[tokio::test]
async fn two_uses_in_one_batch_emit_two_envchanges() {
    let path = temp_path("use-two");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client.login("sa", "secret").await;

    let tokens = client.batch("USE truthdb; USE truthdb").await;
    let envs = tokens
        .iter()
        .filter(|t| matches!(t, Token::EnvChange { kind: 1, .. }))
        .count();
    let infos = tokens
        .iter()
        .filter(|t| matches!(t, Token::Info { number: 5701 }))
        .count();
    assert_eq!((envs, infos), (2, 2), "tokens: {tokens:?}");
    let _ = std::fs::remove_file(&path);
}
