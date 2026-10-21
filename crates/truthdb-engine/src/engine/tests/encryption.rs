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
