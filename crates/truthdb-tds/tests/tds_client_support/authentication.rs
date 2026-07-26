use super::*;

#[tokio::test]
async fn encryption_off_never_offers_tls_even_to_a_client_that_asks() {
    let path = temp_path("enc-off");
    let engine = engine(&path);
    let mut cfg = config();
    cfg.encryption = truthdb_tds::Encryption::Off;
    let mut client = connect_with(engine, cfg).await;
    let advertised = client
        .prelogin_with_encryption(0x01) // ENCRYPT_ON: the client wants TLS
        .await
        .expect("server answered");
    assert_eq!(advertised, 0x02, "must advertise NOT_SUP");
    // ...and the session continues in plaintext.
    client.login("sa", "secret").await;
    let rows = client.batch("SELECT 1 AS n").await;
    assert!(rows.iter().any(|t| matches!(t, Token::Row(_))), "{rows:?}");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn encryption_optional_serves_a_plaintext_client() {
    // The default: a client that does not ask to encrypt is served as before.
    let path = temp_path("enc-optional");
    let engine = engine(&path);
    let mut client = connect_with(engine, config()).await;
    let advertised = client
        .prelogin_with_encryption(0x02) // NOT_SUP: the client will not encrypt
        .await
        .expect("server answered");
    assert_eq!(advertised, 0x02);
    client.login("sa", "secret").await;
    let rows = client.batch("SELECT 1 AS n").await;
    assert!(rows.iter().any(|t| matches!(t, Token::Row(_))), "{rows:?}");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn encryption_required_refuses_a_client_that_cannot_encrypt() {
    // The server must say encryption is mandatory and then refuse — falling
    // back to plaintext would silently defeat the setting.
    let path = temp_path("enc-required");
    let engine = engine(&path);
    let mut cfg = config();
    cfg.encryption = truthdb_tds::Encryption::Required;
    let mut client = connect_with(engine, cfg).await;
    let advertised = client
        .prelogin_with_encryption(0x02) // NOT_SUP
        .await
        .expect("server answers the PRELOGIN first");
    assert_eq!(
        advertised, 0x03,
        "must advertise REQ so the client learns why"
    );
    // The connection is then dropped rather than served in plaintext.
    assert!(
        client.try_read_message().await.is_none(),
        "a client that cannot encrypt must not be served"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn login_failure_reports_18456() {
    let path = temp_path("login-fail");
    let engine = engine(&path);
    let mut client = connect(engine).await;

    client.prelogin().await;
    let tokens = client.login("sa", "wrong-password").await;
    // SQL Server sanitizes the wire state to 1 for every login failure.
    assert_eq!(assert_login_denied(&tokens), 1, "tokens: {tokens:?}");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn unknown_user_is_indistinguishable_from_a_wrong_password() {
    // Username enumeration defense: a non-existent login and a real login with
    // the wrong password must produce the SAME wire response (18456, state 1).
    let path = temp_path("login-enum");
    let engine = engine(&path);

    let mut c1 = connect(engine.clone()).await;
    c1.prelogin().await;
    let wrong_pw = c1.login("sa", "definitely-not-secret").await;

    let mut c2 = connect(engine).await;
    c2.prelogin().await;
    let no_such = c2.login("nobody-here", "whatever").await;

    assert_eq!(assert_login_denied(&wrong_pw), 1);
    assert_eq!(assert_login_denied(&no_such), 1);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn a_migrated_login_authenticates_with_its_configured_password() {
    // The engine() helper migrated sa=secret; the correct password logs in.
    let path = temp_path("login-ok");
    let engine = engine(&path);
    let mut client = connect(engine).await;
    client.prelogin().await;
    let tokens = client.login("sa", "secret").await;
    assert!(tokens.contains(&Token::LoginAck), "tokens: {tokens:?}");
    assert!(!tokens.iter().any(|t| matches!(t, Token::Error { .. })));
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn a_disabled_login_cannot_authenticate_even_with_the_right_password() {
    // CREATE a login, then DISABLE it: the correct password is still rejected,
    // with the same generic 18456/state 1 as any other failure.
    let path = temp_path("login-disabled");
    let engine = engine(&path);
    let mut admin = connect(engine.clone()).await;
    admin.prelogin().await;
    admin.login("sa", "secret").await;
    admin
        .batch("CREATE LOGIN app WITH PASSWORD = 'app-secret'")
        .await;

    // Enabled: the password works.
    let mut ok = connect(engine.clone()).await;
    ok.prelogin().await;
    assert!(
        ok.login("app", "app-secret")
            .await
            .contains(&Token::LoginAck),
        "enabled login should authenticate"
    );

    admin.batch("ALTER LOGIN app DISABLE").await;

    // Disabled: the same correct password is now rejected, generically.
    let mut denied = connect(engine).await;
    denied.prelogin().await;
    let tokens = denied.login("app", "app-secret").await;
    assert_eq!(assert_login_denied(&tokens), 1, "tokens: {tokens:?}");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn repeated_failures_from_one_pair_are_throttled() {
    // A shared throttle across connections: after the free attempts, the next
    // failure is delayed measurably. Keeps the delay small (backoff base) so
    // the test stays fast while still proving the throttle is wired in.
    let path = temp_path("login-throttle");
    let engine = engine(&path);
    let throttle = LoginThrottle::new();

    // Burn the free attempts (each a fresh connection sharing the throttle).
    for _ in 0..4 {
        let mut c = connect_with_throttle(engine.clone(), config(), throttle.clone()).await;
        c.prelogin().await;
        let _ = c.login("sa", "nope").await;
    }
    // The next attempt is now delayed before the response arrives.
    let mut c = connect_with_throttle(engine.clone(), config(), throttle.clone()).await;
    c.prelogin().await;
    let start = std::time::Instant::now();
    let tokens = c.login("sa", "nope").await;
    let elapsed = start.elapsed();
    assert_eq!(assert_login_denied(&tokens), 1);
    assert!(
        elapsed >= std::time::Duration::from_millis(80),
        "throttled attempt returned in {elapsed:?}, expected a backoff delay"
    );
    let _ = std::fs::remove_file(path);
}

/// A login naming a database that does not exist is refused with SQL
/// Server's 4060 (severity 11) rather than a session in a phantom database.
#[tokio::test]
async fn login_to_an_unknown_database_is_refused_with_4060() {
    let path = temp_path("login-4060");
    let engine = engine(&path);
    let mut client = connect(engine.clone()).await;
    client.prelogin().await;
    client
        .write_packet(PKT_LOGIN7, &build_login7("sa", "secret", "nosuchdb"))
        .await;
    let (_, payload) = client.read_message().await;
    let tokens = parse_tokens(&payload);
    assert!(
        tokens
            .iter()
            .any(|t| matches!(t, Token::Error { number: 4060, .. })),
        "unknown login database must be 4060: {tokens:?}"
    );
    let _ = std::fs::remove_file(&path);
}
