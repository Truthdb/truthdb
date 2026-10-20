//! The object engine through the combined engine: operations run under the
//! storage lock (directly or on a worker), object writes trigger checkpoints,
//! the maintenance collection frees unreferenced bodies, and every database
//! file runs the object engine.

use crate::engine::test_support::*;
use crate::engine::*;
use truthdb_object::{ObjFile, SEGMENT_BYTES};

fn bytes(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| seed.wrapping_add((i % 253) as u8))
        .collect()
}

/// Writes `data` as one sealed body, each segment its own engine call.
fn put(engine: &Engine, data: &[u8]) -> u64 {
    let body = engine.object(|file| file.begin_body(1)).expect("begin");
    for chunk in data.chunks(SEGMENT_BYTES) {
        engine
            .object(|file| file.append_segment(body, chunk, 1))
            .expect("segment");
    }
    engine
        .object(|file| file.transaction(|tx| tx.seal_body(body, [0; 16])))
        .expect("seal");
    body
}

fn read(engine: &Engine, body: u64, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    engine
        .object(|file| file.read_body(body, 0, &mut out))
        .expect("read");
    out
}

#[test]
fn a_body_written_through_the_engine_survives_a_reopen() {
    let path = unique_temp_path("objects-reopen");
    let engine = new_engine(&path);
    let data = bytes(1, SEGMENT_BYTES + 99);
    let body = put(&engine, &data);
    assert_eq!(read(&engine, body, data.len()), data);
    drop(engine);

    let err = RelStorage::open(path.clone())
        .err()
        .expect("a relational-only open refuses a file with object data");
    assert!(err.to_string().contains("holds engine 1"), "{err}");
    let engine =
        Engine::new(RelStorage::open_with(path.clone(), crate::database_engines).expect("open"))
            .expect("engine");
    assert_eq!(read(&engine, body, data.len()), data);
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn the_maintenance_collection_frees_an_unreferenced_body() {
    let path = unique_temp_path("objects-gc");
    let engine = new_engine(&path);
    let body = put(&engine, &bytes(2, 1000));
    engine
        .object(|file| file.transaction(|tx| tx.drop_ref(body, 0, 0)))
        .expect("drop the last reference");
    engine.object_gc().expect("collect");
    let err = engine
        .object(|file| file.body_info(body))
        .expect_err("collected");
    assert!(err.to_string().contains("no body"), "{err}");
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn object_writes_take_checkpoints_as_the_log_fills() {
    let path = unique_temp_path("objects-checkpoint");
    let storage = RelStorage::create_with_wal_bounds_with(
        path.clone(),
        test_storage_options(),
        8 * 1024 * 1024,
        8 * 1024 * 1024,
        crate::database_engines,
    )
    .expect("create");
    let engine = Engine::new(storage).expect("engine");
    // Twelve 1 MiB segments through an 8 MiB ring: without checkpoints the
    // ring would fill and the writes fail.
    let data = bytes(3, 12 * SEGMENT_BYTES);
    let body = put(&engine, &data);
    assert!(
        engine.wal_usage_ratio() < 0.75,
        "a checkpoint reclaimed the ring"
    );
    assert_eq!(read(&engine, body, data.len()), data);
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn an_object_operation_runs_on_a_worker() {
    let path = unique_temp_path("objects-worker");
    let engine = new_engine(&path);
    let (handle, join) = crate::session::spawn_engine(engine);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let info = runtime.block_on(async {
        let body = handle
            .run_object(|file: &mut ObjFile<'_>| file.begin_body(1))
            .await
            .expect("begin");
        handle
            .run_object(move |file: &mut ObjFile<'_>| file.append_segment(body, b"hello", 1))
            .await
            .expect("segment");
        handle
            .run_object(move |file: &mut ObjFile<'_>| {
                file.transaction(|tx| tx.seal_body(body, [7; 16]))
            })
            .await
            .expect("seal")
    });
    assert_eq!(info.len, 5);
    handle.shutdown();
    join.join().expect("pool exits");
    let _ = std::fs::remove_file(path);
}

#[test]
fn access_keys_are_administered_over_the_native_protocol() {
    let path = unique_temp_path("objects-keys");
    let engine = new_engine(&path);
    let json = |command: &str| -> serde_json::Value {
        serde_json::from_str(&engine.execute(command).expect(command)).unwrap()
    };
    let created = json("S3 Create Access Key");
    let id = created["access_key_id"].as_str().expect("id").to_string();
    let secret = created["secret_access_key"].as_str().expect("secret");
    assert_eq!(id.len(), 20);
    assert!(id.starts_with("TDB"));
    assert_eq!(secret.len(), 40);
    assert_eq!(created["account_id"], truthdb_object::DEFAULT_ACCOUNT);
    let (key, stored) = engine
        .object(|file| file.access_key(&id))
        .expect("lookup")
        .expect("the key");
    assert_eq!(stored, secret);
    assert_eq!(
        (key.account.as_str(), key.user),
        (truthdb_object::DEFAULT_ACCOUNT, None)
    );

    // An account, its user, and a key of each.
    let account = json("s3 create account acme");
    let account_id = account["account_id"].as_str().unwrap().to_string();
    assert!(
        truthdb_object::valid_account_id(&account_id),
        "{account_id}"
    );
    assert_eq!(
        json("s3 list accounts")["accounts"],
        serde_json::json!([
            {"account_id": truthdb_object::DEFAULT_ACCOUNT, "name": "default"},
            {"account_id": account_id, "name": "acme"},
        ])
    );
    let user = json(&format!("s3 create user {account_id} alice"));
    assert!(user["user_id"].as_str().unwrap().starts_with("AIDA"));
    let root_key = json(&format!("s3 create access key {account_id}"));
    let alice_key = json(&format!("s3 create access key {account_id} alice"));
    assert_eq!(alice_key["user_name"], "alice");
    assert!(
        engine.execute("s3 create access key 999999999999").is_err(),
        "no such account"
    );
    assert!(
        engine
            .execute(&format!("s3 create access key {account_id} bob"))
            .is_err(),
        "no such user"
    );

    let listed = json("s3 list access keys;");
    let listed: Vec<(String, String, serde_json::Value)> = listed["access_keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|key| {
            (
                key["access_key_id"].as_str().unwrap().to_string(),
                key["account_id"].as_str().unwrap().to_string(),
                key["user_name"].clone(),
            )
        })
        .collect();
    assert_eq!(listed.len(), 3, "a trailing ';' is fine");
    assert!(listed.contains(&(
        alice_key["access_key_id"].as_str().unwrap().to_string(),
        account_id.clone(),
        serde_json::json!("alice")
    )));

    // An account goes only once its keys, users and buckets have.
    assert!(
        engine
            .execute(&format!("s3 drop account {account_id}"))
            .is_err()
    );
    assert!(
        engine
            .execute(&format!("s3 drop user {account_id} alice"))
            .is_err()
    );
    for key in [&root_key, &alice_key, &created] {
        engine
            .execute(&format!(
                "s3 drop access key {}",
                key["access_key_id"].as_str().unwrap()
            ))
            .expect("drop key");
    }
    json(&format!("s3 drop user {account_id} alice"));
    engine
        .vector(|file| file.transaction(|tx| tx.create_bucket("vb1", &account_id, 1)))
        .expect("vector bucket")
        .expect("created");
    let refused = engine
        .execute(&format!("s3 drop account {account_id}"))
        .unwrap_err();
    assert!(refused.to_string().contains("vector buckets"), "{refused}");
    engine
        .vector(|file| file.transaction(|tx| tx.delete_bucket("vb1")))
        .expect("delete")
        .expect("deleted");
    json(&format!("s3 drop account {account_id}"));
    assert!(engine.execute("s3 drop account 000000000000").is_err());
    assert_eq!(
        json("s3 list access keys")["access_keys"],
        serde_json::json!([])
    );
    assert!(engine.execute(&format!("s3 drop access key {id}")).is_err());
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn over_tds_only_a_sysadmin_runs_the_s3_administration() {
    use crate::engine::TxnContext;
    let path = unique_temp_path("objects-admin-tds");
    let engine = new_engine(&path);
    engine
        .execute("CREATE LOGIN owner WITH PASSWORD = 'S3cret!'")
        .unwrap();
    engine.execute("CREATE USER owner FOR LOGIN owner").unwrap();
    engine
        .execute("ALTER ROLE db_owner ADD MEMBER owner")
        .unwrap();
    // `sa` is a sysadmin.
    engine
        .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
        .unwrap();
    let session = |login: &str| {
        let login_sid = engine.lookup_login(login).unwrap().principal_id;
        let (user, user_sid) = engine.resolve_session_user(login, login_sid);
        let mut ctx = TxnContext::default();
        ctx.set_session_identity(
            "truthdb".into(),
            crate::relstore::catalog::DEFAULT_DATABASE_ID,
            login.into(),
            9,
            user,
            login_sid,
            user_sid,
        );
        ctx
    };
    // A database owner is not a server administrator.
    let outcome = batch(&engine, &mut session("owner"), "s3 create account acme");
    assert_eq!(outcome.error.as_ref().map(|e| e.number), Some(15247));
    assert_eq!(engine.object(|file| file.accounts()).unwrap().len(), 1);

    let outcome = batch(&engine, &mut session("sa"), "s3 create account acme;");
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let reply = match &outcome.results[..] {
        [StatementResult::Rows(rows)] => match &rows.rows[0][0] {
            truthdb_relational::relstore::types::Datum::NVarChar(text) => text.clone(),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    };
    let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(reply["name"], "acme");
    assert_eq!(engine.object(|file| file.accounts()).unwrap().len(), 2);
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn the_maintenance_collection_drains_a_backlog_in_batches() {
    let path = unique_temp_path("objects-gc-backlog");
    let engine = new_engine(&path);
    let bodies: Vec<u64> = (0..20).map(|i| put(&engine, &bytes(i, 10))).collect();
    for body in &bodies {
        engine
            .object(|file| file.transaction(|tx| tx.drop_ref(*body, 0, 0)))
            .expect("drop");
    }
    engine.object_gc().expect("collect");
    for body in bodies {
        assert!(
            engine.object(|file| file.body_info(body)).is_err(),
            "{body} freed"
        );
    }
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_failed_object_operation_still_checkpoints() {
    let path = unique_temp_path("objects-failed-checkpoint");
    let storage = RelStorage::create_with_wal_bounds_with(
        path.clone(),
        test_storage_options(),
        8 * 1024 * 1024,
        8 * 1024 * 1024,
        crate::database_engines,
    )
    .expect("create");
    let engine = Engine::new(storage).expect("engine");
    let body = engine.object(|file| file.begin_body(1)).expect("begin");
    let segment = bytes(4, SEGMENT_BYTES);
    // One call writing segment after segment runs the ring full and fails.
    let err = engine
        .object(|file| {
            for _ in 0..12 {
                file.append_segment(body, &segment, 1)?;
            }
            Ok(())
        })
        .expect_err("the ring ran full");
    assert!(err.to_string().contains("full"), "{err}");
    assert!(
        engine.wal_usage_ratio() < 0.5,
        "the failure still took a checkpoint"
    );
    engine
        .object(|file| file.append_segment(body, &segment, 1))
        .expect("the next write fits");
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn user_policies_are_validated_kept_listed_and_deleted() {
    let path = unique_temp_path("objects-user-policies");
    let engine = new_engine(&path);
    let json = |command: &str| -> serde_json::Value {
        serde_json::from_str(&engine.execute(command).expect(command)).unwrap()
    };
    let account = json("s3 create account acme")["account_id"]
        .as_str()
        .unwrap()
        .to_string();
    json(&format!("s3 create user {account} alice"));
    let policy = r#"{"Version": "2012-10-17",  "Statement": {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}}"#;
    json(&format!(
        "s3 put user policy {account} alice read {policy};"
    ));
    let listed = json(&format!("s3 list user policies {account} alice"));
    assert_eq!(listed["policies"][0]["policy_name"], "read");
    assert_eq!(
        listed["policies"][0]["policy_document"], policy,
        "kept as written"
    );
    for bad in [
        format!("s3 put user policy {account} alice bad {{\"Statement\": []}}"),
        format!(
            r#"s3 put user policy {account} alice bad {{"Statement": {{"Effect": "Allow", "Principal": "*", "Action": "s3:*", "Resource": "*"}}}}"#
        ),
        format!("s3 put user policy {account} nobody read {policy}"),
    ] {
        assert!(engine.execute(&bad).is_err(), "{bad}");
    }
    assert!(
        engine
            .execute(&format!("s3 drop user {account} alice"))
            .is_err()
    );
    json(&format!("s3 delete user policy {account} alice read"));
    assert!(
        engine
            .execute(&format!("s3 delete user policy {account} alice read"))
            .is_err()
    );
    json(&format!("s3 drop user {account} alice"));
    drop(engine);
    let _ = std::fs::remove_file(path);
}
