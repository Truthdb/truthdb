//! The streams engine in the server: `stream create|drop topic`, the
//! `sys.stream_topics` and `sys.stream_read` rowsets, who may use them, and
//! the maintenance sweep's purging and trimming.

use crate::engine::test_support::*;
use crate::engine::*;
use crate::storage::RelStorage;
use serde_json::Value;
use truthdb_streams::NewRecord;

fn append(engine: &Engine, topic: &str, partition: u32, values: &[&str], now_ms: u64) {
    let records: Vec<NewRecord> = values
        .iter()
        .map(|value| NewRecord {
            key: Some(b"k".to_vec()),
            value: value.as_bytes().to_vec(),
        })
        .collect();
    // Small records take a chunk each: an append holds as many.
    for batch in records.chunks(truthdb_streams::MAX_APPEND_CHUNKS) {
        engine
            .streams(|file| file.transaction(|tx| tx.append(topic, partition, batch, None, now_ms)))
            .unwrap()
            .unwrap();
    }
}

/// `account_id` of `topic` in `sys.stream_topics`.
fn rows_of(engine: &Engine, topic: &str) -> Vec<Vec<Option<String>>> {
    sql_rows(
        engine,
        &format!("SELECT account_id FROM sys.stream_topics WHERE topic_name = '{topic}'"),
    )
    .1
}

fn hex(text: &str) -> String {
    let digits: String = text.bytes().map(|b| format!("{b:02X}")).collect();
    format!("0x{digits}")
}

fn command_error(engine: &Engine, command: &str) -> String {
    match engine.execute(command) {
        Err(err) => err.to_string(),
        Ok(reply) => panic!("{command} succeeded: {reply}"),
    }
}

fn session(engine: &Engine, login: &str) -> TxnContext {
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
}

#[test]
fn topics_are_created_read_as_views_dropped_and_purged() {
    let path = unique_temp_path("streams-admin");
    let engine = new_engine(&path);
    let reply: Value = serde_json::from_str(
        &engine
            .execute("STREAM CREATE TOPIC events partitions 2 retention 0;")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reply["topic"], "events");
    assert_eq!(reply["partitions"], 2);
    assert_eq!(reply["retention_hours"], 0);
    let reply: Value =
        serde_json::from_str(&engine.execute("stream create topic plain").unwrap()).unwrap();
    assert_eq!(reply["partitions"], 1);
    assert_eq!(reply["retention_hours"], 168, "Kafka's default retention");
    assert_eq!(reply["account"], Value::Null);

    for (command, error) in [
        ("stream create topic events", "already exists"),
        ("stream create topic other partitions", "has no value"),
        ("stream create topic other colour 2", "unknown topic option"),
        (
            "stream create topic other partitions 0",
            "1 to 64 partitions",
        ),
        (
            "stream create topic other partitions 65",
            "1 to 64 partitions",
        ),
        (
            "stream create topic other partitions 99999999999",
            "1 to 64 partitions",
        ),
        ("stream create topic other retention -1", "whole number"),
        (
            "stream create topic other retention 99999999999999999",
            "retention of",
        ),
        (
            "stream create topic other partitions 2 partitions 3",
            "given twice",
        ),
        // Past `bigint`'s range in milliseconds.
        (
            "stream create topic other retention 2562047788016",
            "retention of",
        ),
        ("stream create topic a/b", "letters, digits"),
        (
            "stream create topic other account 999999999999",
            "no S3 account",
        ),
        (
            "stream create topic other account 000000000000 account 000000000000",
            "given twice",
        ),
        ("stream drop topic missing", "no topic 'missing'"),
    ] {
        let message = command_error(&engine, command);
        assert!(message.contains(error), "{command}: {message}");
    }
    assert!(
        !engine
            .streams(|file| file.topics())
            .unwrap()
            .iter()
            .any(|(topic, _)| topic.name == "other"),
        "a refused command creates nothing"
    );

    append(&engine, "events", 1, &["a", "b", "c"], 1_000);
    let (columns, rows) = sql_rows(&engine, "SELECT * FROM sys.stream_topics");
    assert_eq!(
        columns,
        [
            "topic_name",
            "partition_id",
            "start_offset",
            "next_offset",
            "last_record_time",
            "retention_ms",
            "created",
            "account_id"
        ]
    );
    let summary: Vec<_> = rows
        .iter()
        .map(|row| {
            (
                row[0].clone().unwrap(),
                row[1].clone().unwrap(),
                row[3].clone().unwrap(),
                row[4].is_some(),
                row[5].clone().unwrap(),
            )
        })
        .collect();
    let row = |topic: &str, partition: &str, next: &str, written: bool, retention: &str| {
        (
            topic.to_string(),
            partition.to_string(),
            next.to_string(),
            written,
            retention.to_string(),
        )
    };
    assert_eq!(
        summary,
        [
            row("events", "0", "0", false, "0"),
            row("events", "1", "3", true, "0"),
            row("plain", "0", "0", false, "604800000"),
        ]
    );
    // A topic for an S3 account's event notifications.
    let reply: Value = serde_json::from_str(
        &engine
            .execute("stream create topic owned account 000000000000")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reply["account"], "000000000000");
    assert_eq!(
        rows_of(&engine, "owned"),
        [[Some("000000000000".to_string())]]
    );
    assert_eq!(rows_of(&engine, "plain"), [[None]]);
    engine.execute("stream drop topic owned").unwrap();

    let (columns, rows) = sql_rows(&engine, "SELECT * FROM sys.stream_read('events', 1)");
    assert_eq!(columns, ["offset", "timestamp", "key", "value"]);
    let values: Vec<_> = rows
        .iter()
        .map(|row| (row[0].clone().unwrap(), row[3].clone().unwrap()))
        .collect();
    assert_eq!(
        values,
        [
            ("0".to_string(), hex("a")),
            ("1".to_string(), hex("b")),
            ("2".to_string(), hex("c"))
        ]
    );
    assert_eq!(rows[0][2], Some(hex("k")));
    let offsets = |query: &str| -> Vec<i64> { sql_column_i64(&engine, query, 0) };
    assert_eq!(
        offsets("SELECT offset FROM sys.stream_read('events', 1, 1)"),
        [1, 2]
    );
    assert_eq!(
        offsets("SELECT offset FROM sys.stream_read('events', 1, NULL, 2)"),
        [0, 1]
    );
    assert_eq!(
        offsets("SELECT offset FROM sys.stream_read('events', 1, 9)"),
        Vec::<i64>::new()
    );
    assert!(
        sql_rows(&engine, "SELECT * FROM sys.stream_read('events', 0)")
            .1
            .is_empty()
    );
    for (query, number) in [
        ("SELECT * FROM sys.stream_read('missing', 0)", 208),
        ("SELECT * FROM sys.stream_read('missing', 0, 0, 0)", 208),
        ("SELECT * FROM sys.stream_read('events', 2, 0, 0)", 50000),
        ("SELECT * FROM sys.stream_read('events', 2)", 50000),
        ("SELECT * FROM sys.stream_read('events', NULL)", 50000),
        ("SELECT * FROM sys.stream_read('events', -1)", 206),
        ("SELECT * FROM sys.stream_read('events', 0, 'x')", 206),
        ("SELECT * FROM sys.stream_read('events')", 313),
        ("SELECT * FROM sys.stream_read('events', 0, 0, 0, 0)", 8144),
        ("SELECT * FROM sys.stream_topics('events')", 8144),
    ] {
        assert_eq!(sql_error_number(&engine, query), number, "{query}");
    }

    // Only a server administrator administers or reads topics.
    engine
        .execute("CREATE LOGIN owner WITH PASSWORD = 'S3cret!'")
        .unwrap();
    engine.execute("CREATE USER owner FOR LOGIN owner").unwrap();
    engine
        .execute("ALTER ROLE db_owner ADD MEMBER owner")
        .unwrap();
    let mut owner = session(&engine, "owner");
    for (query, number) in [
        ("stream create topic sneaky", 15247),
        ("stream drop topic events", 15247),
        ("SELECT * FROM sys.stream_topics", 229),
        ("SELECT * FROM sys.stream_read('events', 1)", 229),
    ] {
        let outcome = batch(&engine, &mut owner, query);
        assert_eq!(
            outcome.error.as_ref().map(|e| e.number),
            Some(number),
            "{query}"
        );
    }
    assert_eq!(engine.streams(|file| file.topics()).unwrap().len(), 2);

    // A dropped topic is gone at once; the sweep removes its records.
    engine.execute("stream drop topic events;").unwrap();
    assert_eq!(
        sql_error_number(&engine, "SELECT * FROM sys.stream_read('events', 1)"),
        208
    );
    assert!(engine.streams(|file| file.purge_pending()).unwrap());
    engine.streams_maintenance().unwrap();
    assert!(!engine.streams(|file| file.purge_pending()).unwrap());
    engine.execute("stream create topic events").unwrap();
    assert!(
        sql_rows(&engine, "SELECT * FROM sys.stream_read('events', 0)")
            .1
            .is_empty(),
        "a topic of the same name starts empty"
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn the_sweep_trims_records_past_retention() {
    let path = unique_temp_path("streams-trim");
    let engine = new_engine(&path);
    engine
        .execute("stream create topic hourly retention 1")
        .unwrap();
    engine
        .execute("stream create topic kept retention 0")
        .unwrap();
    let now = super::super::now_ms();
    let two_hours_ago = now - 2 * 3_600_000;
    // More than one trim transaction's worth: one sweep takes several.
    let old: Vec<String> = (0..600).map(|i| format!("old{i}")).collect();
    let old: Vec<&str> = old.iter().map(String::as_str).collect();
    append(&engine, "hourly", 0, &old, two_hours_ago);
    append(&engine, "hourly", 0, &["new"], now);
    append(&engine, "kept", 0, &["old"], two_hours_ago);
    engine.streams_maintenance().unwrap();
    assert_eq!(
        sql_column_i64(
            &engine,
            "SELECT offset FROM sys.stream_read('hourly', 0)",
            0
        ),
        [600]
    );
    assert_eq!(
        sql_column_i64(&engine, "SELECT offset FROM sys.stream_read('kept', 0)", 0),
        [0]
    );
    assert_eq!(
        sql_column_i64(
            &engine,
            "SELECT start_offset FROM sys.stream_topics WHERE topic_name = 'hourly'",
            0
        ),
        [600]
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_read_streams_past_one_slice_and_topics_survive_a_reopen() {
    let path = unique_temp_path("streams-reopen");
    let engine = new_engine(&path);
    engine
        .execute("stream create topic big partitions 3 retention 0")
        .unwrap();
    let values: Vec<String> = (0..2_500).map(|i| format!("v{i}")).collect();
    let values: Vec<&str> = values.iter().map(String::as_str).collect();
    append(&engine, "big", 2, &values, 5);
    drop(engine);

    let engine =
        Engine::new(RelStorage::open_with(path.clone(), crate::database_engines).expect("open"))
            .expect("engine");
    let offsets = sql_column_i64(&engine, "SELECT offset FROM sys.stream_read('big', 2)", 0);
    assert_eq!(offsets, (0..2_500).collect::<Vec<i64>>());
    let offsets = sql_column_i64(
        &engine,
        "SELECT offset FROM sys.stream_read('big', 2, 100, 1100)",
        0,
    );
    assert_eq!(offsets, (100..1_200).collect::<Vec<i64>>());
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_standby_refuses_to_read_topics() {
    let primary_path = unique_temp_path("streams-primary");
    let bak = unique_temp_path("streams-bak");
    let standby_path = unique_temp_path("streams-standby");
    let primary = new_engine(&primary_path);
    primary.execute("stream create topic events").unwrap();
    append(&primary, "events", 0, &["a"], 1);
    primary.storage().backup_full(&bak).expect("backup");
    RelStorage::restore_full_standby_with(&standby_path, &bak, &[], crate::database_engines, None)
        .expect("seed");
    let standby = Engine::new(
        RelStorage::open_with(standby_path.clone(), crate::database_engines).expect("open"),
    )
    .expect("engine");
    for query in [
        "SELECT * FROM sys.stream_topics",
        "SELECT * FROM sys.stream_read('events', 0)",
    ] {
        let env = sql(&standby, query);
        assert_eq!(env["error"]["number"], 50000, "{query}: {env}");
        assert!(
            env["error"]["message"]
                .as_str()
                .unwrap()
                .contains("read on the primary"),
            "{env}"
        );
    }
    drop((primary, standby));
    for path in [primary_path, bak, standby_path] {
        let _ = std::fs::remove_file(path);
    }
}
