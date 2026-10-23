//! The SQL queue: `sys.sp_s3_queue_receive`, `_ack` and
//! `_change_visibility` over a stream topic, run by `EXEC` outside the
//! caller's transaction, with SQL Server's argument rules, for server
//! administrators only, and locking nothing.

use crate::engine::test_support::*;
use crate::engine::*;
use serde_json::Value;
use truthdb_streams::NewRecord;

fn append(engine: &Engine, values: &[&str]) {
    let records: Vec<NewRecord> = values
        .iter()
        .map(|value| NewRecord {
            key: None,
            value: value.as_bytes().to_vec(),
        })
        .collect();
    engine
        .streams(|file| file.transaction(|tx| tx.append("q", 0, &records, None, 1_000)))
        .unwrap()
        .unwrap();
}

/// (body, receipt, receive count) of each message a receive returns.
fn received(engine: &Engine, sql: &str) -> Vec<(String, String, String)> {
    let (columns, rows) = sql_rows(engine, sql);
    assert_eq!(
        columns,
        [
            "message_id",
            "receipt_handle",
            "body",
            "body_bytes",
            "md5_of_body",
            "receive_count",
            "sent_time",
            "first_receive_time"
        ]
    );
    rows.into_iter()
        .map(|row| {
            (
                row[2].clone().unwrap(),
                row[1].clone().unwrap(),
                row[5].clone().unwrap(),
            )
        })
        .collect()
}

#[test]
fn a_queue_is_received_acknowledged_and_hidden_through_exec() {
    let path = unique_temp_path("sql-queue");
    let engine = new_engine(&path);
    engine.execute("stream create topic q retention 0").unwrap();
    append(&engine, &["a", "b", "c"]);
    let first = received(
        &engine,
        "EXEC sys.sp_s3_queue_receive @queue = N'q', @max_messages = 2",
    );
    assert_eq!(
        first.iter().map(|m| m.0.as_str()).collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(first[0].2, "1");
    let (_, rows) = sql_rows(&engine, "EXEC sys.sp_s3_queue_receive N'q', 10, 0");
    assert_eq!(rows.len(), 1, "positional; the rest in flight");
    assert_eq!(rows[0][0].as_deref(), Some("0-2"));
    assert_eq!(rows[0][3].as_deref(), Some("0x63"));
    assert_eq!(
        rows[0][4].as_deref(),
        Some("4a8a08f09d37b73795649038408b5f33"),
        "MD5 of 'c'"
    );
    // Acknowledged with its status captured; hidden at once.
    let env = sql(
        &engine,
        &format!(
            "DECLARE @rc INT = 9; EXEC @rc = sys.sp_s3_queue_ack N'q', N'{}'; SELECT @rc",
            first[0].1
        ),
    );
    assert_eq!(
        env["results"].as_array().unwrap().last().unwrap()["rows"][0][0],
        "0"
    );
    sql(
        &engine,
        &format!(
            "EXEC sys.sp_s3_queue_change_visibility @queue = N'q', @receipt_handle = N'{}', \
             @visibility_timeout = 0",
            first[1].1
        ),
    );
    // A receive in a transaction rolled back still took its messages.
    let env = sql(
        &engine,
        "BEGIN TRANSACTION; EXEC sys.sp_s3_queue_receive N'q', 10, 30; ROLLBACK",
    );
    assert!(env["error"].is_null(), "{env}");
    let rows = env["results"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|result| result["rows"].as_array())
        .unwrap_or_else(|| panic!("no rowset: {env}"));
    let bodies: Vec<&str> = rows.iter().map(|row| row[2].as_str().unwrap()).collect();
    assert_eq!(
        bodies,
        ["c", "b"],
        "in the order they became visible: c's timeout of 0, then b made so"
    );
    assert!(
        received(&engine, "EXEC sys.sp_s3_queue_receive N'q'").is_empty(),
        "the rollback returned nothing"
    );

    for (query, number) in [
        ("EXEC sys.sp_s3_queue_receive", 201),
        ("EXEC sys.sp_s3_queue_receive N'q', 1, 1, 1", 8144),
        (
            "EXEC sys.sp_s3_queue_receive @queue = N'q', @nope = 1",
            8145,
        ),
        ("EXEC sys.sp_s3_queue_receive N'q', @queue = N'q'", 8143),
        ("EXEC sys.sp_s3_queue_receive @queue = N'q', 1", 119),
        ("EXEC sys.sp_s3_queue_receive N'q', 11", 50000),
        ("EXEC sys.sp_s3_queue_receive N'q', N'x'", 245),
        (
            "EXEC sys.sp_s3_queue_receive N'q', 99999999999999999999.0",
            8115,
        ),
        ("EXEC sys.sp_s3_queue_receive N'q', 10.5, 43201", 50000),
        ("EXEC sys.sp_s3_queue_receive N'q', 1, 43201", 50000),
        ("EXEC sys.sp_s3_queue_receive 5", 206),
        ("EXEC sys.sp_s3_queue_receive N'missing'", 50000),
        ("EXEC sys.sp_s3_queue_ack N'q', N'not-a-receipt'", 50000),
        ("EXEC sys.sp_s3_queue_ack N'q'", 201),
        (
            "DECLARE @x INT = 1; EXEC sys.sp_s3_queue_receive N'q', @x OUTPUT",
            8162,
        ),
        ("EXEC dbo.sp_s3_queue_receive N'q'", 2812),
        ("EXEC @undeclared = sys.sp_s3_queue_receive N'q'", 137),
    ] {
        assert_eq!(sql_error_number(&engine, query), number, "{query}");
    }
    let env = sql(&engine, "EXEC sys.sp_s3_queue_receive N'missing'");
    assert_eq!(
        env["error"]["message"],
        Value::from("The queue 'missing' does not exist.")
    );

    // Numbers given as a decimal or a string convert as to `int`; a body
    // that is not UTF-8 is NULL text beside its bytes.
    engine
        .streams(|file| {
            file.transaction(|tx| {
                tx.append(
                    "q",
                    0,
                    &[NewRecord {
                        key: None,
                        value: vec![0xFF, 0x00],
                    }],
                    None,
                    1_000,
                )
            })
        })
        .unwrap()
        .unwrap();
    let (_, rows) = sql_rows(&engine, "EXEC sys.sp_s3_queue_receive N'q', 2.9, N' 30 '");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0][2].as_deref(), rows[0][3].as_deref()),
        (None, Some("0xFF00"))
    );
    // Locks nothing: it runs in transactions of its own.
    let needs = crate::engine::analyze_locks(
        engine.storage(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "EXEC sys.sp_s3_queue_receive N'q'",
        Isolation::ReadCommitted,
    );
    assert!(needs.is_empty(), "{needs:?}");

    // Only a server administrator.
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
    let outcome = batch(&engine, &mut ctx, "EXEC sys.sp_s3_queue_receive N'q'");
    assert_eq!(outcome.error.as_ref().map(|e| e.number), Some(229));
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_standby_refuses_the_queue() {
    let primary_path = unique_temp_path("queue-primary");
    let bak = unique_temp_path("queue-bak");
    let standby_path = unique_temp_path("queue-standby");
    let primary = new_engine(&primary_path);
    primary.execute("stream create topic q").unwrap();
    primary.storage().backup_full(&bak).expect("backup");
    crate::storage::RelStorage::restore_full_standby_with(
        &standby_path,
        &bak,
        &[],
        crate::database_engines,
        None,
    )
    .expect("seed");
    let standby = Engine::new(
        crate::storage::RelStorage::open_with(standby_path.clone(), crate::database_engines)
            .expect("open"),
    )
    .expect("engine");
    let env = sql(&standby, "EXEC sys.sp_s3_queue_receive N'q'");
    assert!(
        env["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("on the primary"),
        "{env}"
    );
    drop((primary, standby));
    for path in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(path);
    }
}
