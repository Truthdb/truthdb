//! Encryption-at-rest administration through the engine: `encryption
//! status`, `encryption rotate master key` and
//! `sys.dm_database_encryption_keys`.

use crate::engine::test_support::*;
use crate::engine::*;

#[test]
fn the_encryption_status_and_view_describe_the_database() {
    let path = unique_temp_path("encryption-status");
    let engine = new_engine(&path);
    let encrypted = engine.storage().is_encrypted();
    let status: serde_json::Value =
        serde_json::from_str(&engine.execute("Encryption Status;").expect("status")).unwrap();
    assert_eq!(status["encrypted"], encrypted);

    let mut ctx = TxnContext::default();
    let outcome = batch(
        &engine,
        &mut ctx,
        "SELECT encryption_state, key_algorithm, key_length, encryptor_type \
         FROM sys.dm_database_encryption_keys",
    );
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let [StatementResult::Rows(rows)] = &outcome.results[..] else {
        panic!("{:?}", outcome.results);
    };
    use truthdb_relational::relstore::types::Datum;
    if encrypted {
        assert_eq!(
            rows.rows,
            vec![vec![
                Datum::Int(3),
                Datum::NVarChar("AES".into()),
                Datum::Int(256),
                Datum::NVarChar("KEY FILE".into()),
            ]]
        );
        // The test key file has one key: the rotation rewraps under it.
        let rotated: serde_json::Value = serde_json::from_str(
            &engine
                .execute("encryption rotate master key")
                .expect("rotate"),
        )
        .unwrap();
        assert_eq!(rotated["key_fingerprint"], status["key_fingerprint"]);
    } else {
        assert_eq!(
            rows.rows,
            vec![vec![
                Datum::Int(1),
                Datum::Null,
                Datum::Null,
                Datum::NVarChar("NONE".into()),
            ]]
        );
        let err = engine
            .execute("encryption rotate master key")
            .expect_err("a plain database has no master key");
        assert!(err.to_string().contains("not encrypted"), "{err}");
    }
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn over_tds_only_a_sysadmin_runs_the_encryption_administration() {
    let path = unique_temp_path("encryption-admin-tds");
    let engine = new_engine(&path);
    engine
        .execute("CREATE LOGIN owner WITH PASSWORD = 'S3cret!'")
        .unwrap();
    engine.execute("CREATE USER owner FOR LOGIN owner").unwrap();
    engine
        .execute("ALTER ROLE db_owner ADD MEMBER owner")
        .unwrap();
    let login_sid = engine.lookup_login("owner").unwrap().principal_id;
    let (user, user_sid) = engine.resolve_session_user("owner", login_sid);
    let mut ctx = TxnContext::default();
    ctx.set_session_identity(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "owner".into(),
        9,
        user,
        login_sid,
        user_sid,
    );
    for command in ["encryption status", "encryption rotate master key"] {
        let outcome = batch(&engine, &mut ctx, command);
        assert_eq!(
            outcome.error.as_ref().map(|e| e.number),
            Some(15247),
            "{command}"
        );
    }
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn encryption_enable_converts_the_database_in_the_maintenance_sweeps() {
    use std::os::unix::fs::PermissionsExt;
    // The encrypted test run creates every database encrypted.
    if truthdb_core::encryption::test_key_source().is_some() {
        return;
    }
    let path = unique_temp_path("encryption-enable");
    let keys = unique_temp_path("encryption-enable-keys").with_extension("keys");
    std::fs::write(&keys, format!("main {}\n", "1d".repeat(32))).unwrap();
    std::fs::set_permissions(&keys, std::fs::Permissions::from_mode(0o600)).unwrap();
    let source = truthdb_core::encryption::KeySource::File(keys.clone());
    let engine = new_engine(&path);
    for sql in [
        "CREATE TABLE secrets (id INT NOT NULL PRIMARY KEY, v NVARCHAR(100))",
        "INSERT INTO secrets VALUES (1, N'PLAINTEXT-CANARY-ONE'), (2, N'PLAINTEXT-CANARY-TWO')",
    ] {
        engine.execute(sql).expect(sql);
    }
    engine.checkpoint().expect("checkpoint");
    drop(engine);
    let utf16: Vec<u8> = "PLAINTEXT-CANARY"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    let before = std::fs::read(&path).unwrap();
    assert!(
        before
            .windows(utf16.len())
            .any(|window| window == utf16.as_slice()),
        "the plain database holds the text"
    );

    // Restarted with a master key configured, then enabled while it runs.
    let storage = RelStorage::open_with_keys(path.clone(), crate::database_engines, Some(&source))
        .expect("open");
    let engine = Engine::new(storage).expect("engine");
    let status: serde_json::Value =
        serde_json::from_str(&engine.execute("encryption enable").expect("enable")).unwrap();
    assert_eq!(status["converting"], true);
    engine
        .execute("INSERT INTO secrets VALUES (3, N'PLAINTEXT-CANARY-THREE')")
        .expect("insert during the conversion");
    for _ in 0..1000 {
        if !engine.storage().encryption_status().converting {
            break;
        }
        engine.encryption_conversion().expect("sweep");
    }
    let status: serde_json::Value =
        serde_json::from_str(&engine.execute("encryption status").expect("status")).unwrap();
    assert_eq!(
        (status["encrypted"].clone(), status["converting"].clone()),
        (true.into(), false.into())
    );
    let mut ctx = TxnContext::default();
    let outcome = batch(
        &engine,
        &mut ctx,
        "SELECT encryption_state FROM sys.dm_database_encryption_keys",
    );
    let [StatementResult::Rows(rows)] = &outcome.results[..] else {
        panic!("{:?}", outcome.results);
    };
    assert_eq!(
        rows.rows[0][0],
        truthdb_relational::relstore::types::Datum::Int(3)
    );
    drop(engine);
    // NVARCHAR text is stored as UTF-16.
    let file = std::fs::read(&path).unwrap();
    let utf8 = b"PLAINTEXT-CANARY".to_vec();
    let utf16: Vec<u8> = "PLAINTEXT-CANARY"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    for needle in [&utf8, &utf16] {
        assert!(
            !file
                .windows(needle.len())
                .any(|window| window == needle.as_slice()),
            "plaintext left in the file"
        );
    }

    let storage = RelStorage::open_with_keys(path.clone(), crate::database_engines, Some(&source))
        .expect("reopen");
    let engine = Engine::new(storage).expect("engine");
    let (_, rows) = sql_rows(&engine, "SELECT v FROM secrets ORDER BY id");
    assert_eq!(rows.len(), 3);
    drop(engine);
    for file in [&path, &keys] {
        let _ = std::fs::remove_file(file);
    }
}
