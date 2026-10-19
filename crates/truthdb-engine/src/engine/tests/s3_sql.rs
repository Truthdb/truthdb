//! The object store's inventory through SQL (`sys.s3_*`): live rows for
//! every committed write, the permission gate, name resolution ahead of the
//! catalog, and scans that stream past one slice without blocking writers.

use crate::engine::test_support::*;
use crate::engine::*;
use truthdb_object::{ObjectAttrs, SealedBody, Versioning};

fn create_bucket(engine: &Engine, bucket: &str, versioning: Versioning) {
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.create_bucket(bucket, 1_000)?.expect("bucket");
                if versioning != Versioning::Unversioned {
                    tx.set_versioning(bucket, versioning)?.expect("versioning");
                }
                Ok(())
            })
        })
        .expect("create bucket");
}

fn put(engine: &Engine, bucket: &str, key: &str, data: &[u8], attrs: &ObjectAttrs) -> u64 {
    engine
        .object(|file| {
            let body = file.begin_body(1)?;
            file.append_segment(body, data, 1)?;
            let sealed = SealedBody { body, md5: [7; 16] };
            file.transaction(|tx| tx.put_object(bucket, key, &sealed, attrs, 2_000, 0))
        })
        .expect("put")
        .expect("put accepted")
        .version
}

fn cells(rows: &[Vec<Option<String>>], column: usize) -> Vec<String> {
    rows.iter()
        .map(|row| row[column].clone().unwrap_or_else(|| "NULL".into()))
        .collect()
}

#[test]
fn the_inventory_lists_objects_versions_tags_and_metadata_live() {
    let path = unique_temp_path("s3-sql-inventory");
    let engine = new_engine(&path);
    create_bucket(&engine, "docs", Versioning::Unversioned);
    let tagged = ObjectAttrs {
        content_type: Some("text/plain".into()),
        user: vec![("owner".into(), "ann".into())],
        tags: vec![
            ("project".into(), "apollo".into()),
            ("tier".into(), "gold".into()),
        ],
        ..ObjectAttrs::default()
    };
    put(&engine, "docs", "logs/2025/error-1.json", b"one", &tagged);
    put(
        &engine,
        "docs",
        "logs/2025/ok.json",
        b"two!",
        &ObjectAttrs::default(),
    );
    put(
        &engine,
        "docs",
        "readme.md",
        b"three",
        &ObjectAttrs::default(),
    );

    // Names by pattern, bounded by a prefix.
    let (columns, rows) = sql_rows(
        &engine,
        "SELECT [key], size, content_type, version_id, storage_class FROM sys.s3_objects('docs', 'logs/') WHERE [key] LIKE '%error%'",
    );
    assert_eq!(
        columns,
        ["key", "size", "content_type", "version_id", "storage_class"]
    );
    assert_eq!(
        rows,
        [vec![
            Some("logs/2025/error-1.json".to_string()),
            Some("3".to_string()),
            Some("text/plain".to_string()),
            Some("null".to_string()),
            Some("STANDARD".to_string()),
        ]]
    );
    let (_, rows) = sql_rows(
        &engine,
        "SELECT [key] FROM sys.s3_objects('docs') ORDER BY size DESC",
    );
    assert_eq!(
        cells(&rows, 0),
        ["readme.md", "logs/2025/ok.json", "logs/2025/error-1.json"]
    );
    // Tags and metadata, one row per pair, joinable to the objects.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT o.[key], t.tag_value FROM sys.s3_objects('docs') o JOIN sys.s3_object_tags('docs') t ON t.[key] = o.[key] WHERE t.tag_key = 'tier'",
    );
    assert_eq!(
        rows,
        [vec![
            Some("logs/2025/error-1.json".into()),
            Some("gold".into())
        ]]
    );
    let (_, rows) = sql_rows(
        &engine,
        "SELECT name, value FROM sys.s3_object_metadata('docs', 'logs/')",
    );
    assert_eq!(rows, [vec![Some("owner".into()), Some("ann".into())]]);

    // A write is visible to the next query.
    put(
        &engine,
        "docs",
        "logs/2025/error-2.json",
        b"four",
        &ObjectAttrs::default(),
    );
    let (_, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*) FROM sys.s3_objects('docs', 'logs/') WHERE [key] LIKE '%error%'",
    );
    assert_eq!(cells(&rows, 0), ["2"]);

    // Every version and delete marker of a versioned bucket.
    create_bucket(&engine, "hist", Versioning::Enabled);
    let v1 = put(&engine, "hist", "k", b"a", &ObjectAttrs::default());
    let v2 = put(&engine, "hist", "k", b"bb", &ObjectAttrs::default());
    let marker = engine
        .object(|file| file.transaction(|tx| tx.delete_object("hist", "k", None, 3_000, 0)))
        .expect("delete")
        .expect("delete accepted")
        .version
        .expect("a marker");
    let (_, rows) = sql_rows(
        &engine,
        "SELECT version_id, is_latest, is_delete_marker, size FROM sys.s3_object_versions('hist')",
    );
    assert_eq!(
        rows,
        [
            vec![
                Some(marker.to_string()),
                Some("1".into()),
                Some("1".into()),
                None
            ],
            vec![
                Some(v2.to_string()),
                Some("0".into()),
                Some("0".into()),
                Some("2".into())
            ],
            vec![
                Some(v1.to_string()),
                Some("0".into()),
                Some("0".into()),
                Some("1".into())
            ],
        ]
    );
    let (_, rows) = sql_rows(&engine, "SELECT [key] FROM sys.s3_objects('hist')");
    assert!(rows.is_empty(), "a deleted key is not current: {rows:?}");

    // The buckets, and a per-bucket function applied to each.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT name, versioning FROM sys.s3_buckets ORDER BY name",
    );
    assert_eq!(
        rows,
        [
            vec![Some("docs".into()), None],
            vec![Some("hist".into()), Some("Enabled".into())],
        ]
    );
    let (_, rows) = sql_rows(
        &engine,
        "SELECT b.name, COUNT(*) FROM sys.s3_buckets b CROSS APPLY sys.s3_object_versions(b.name, '') v GROUP BY b.name ORDER BY b.name",
    );
    assert_eq!(
        rows,
        [
            vec![Some("docs".into()), Some("4".into())],
            vec![Some("hist".into()), Some("3".into())],
        ]
    );

    // A missing bucket and wrong arguments are errors.
    assert_eq!(
        sql_error_number(&engine, "SELECT * FROM sys.s3_objects('nope')"),
        208
    );
    assert_eq!(
        sql_error_number(&engine, "SELECT * FROM sys.s3_objects()"),
        313
    );
    assert_eq!(
        sql_error_number(&engine, "SELECT * FROM sys.s3_objects('a', 'b', 'c')"),
        8144
    );
    assert_eq!(
        sql_error_number(&engine, "SELECT * FROM sys.s3_objects(1)"),
        206
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn only_a_server_administrator_reads_the_inventory() {
    let path = unique_temp_path("s3-sql-perms");
    let engine = new_engine(&path);
    create_bucket(&engine, "secret", Versioning::Unversioned);
    put(&engine, "secret", "k", b"x", &ObjectAttrs::default());
    engine
        .execute("CREATE LOGIN owner WITH PASSWORD = 'S3cret!'")
        .unwrap();
    engine.execute("CREATE USER owner FOR LOGIN owner").unwrap();
    engine
        .execute("ALTER ROLE db_owner ADD MEMBER owner")
        .unwrap();
    // A database owner bypasses object permissions in its database, but the
    // buckets are the server's.
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
    for query in [
        "SELECT * FROM sys.s3_objects('secret')",
        "SELECT * FROM sys.s3_buckets",
    ] {
        let outcome = batch(&engine, &mut ctx, query);
        assert_eq!(
            outcome.error.as_ref().map(|e| e.number),
            Some(229),
            "{query}"
        );
    }
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_user_object_of_the_same_name_does_not_hide_the_inventory() {
    let path = unique_temp_path("s3-sql-shadow");
    let engine = new_engine(&path);
    create_bucket(&engine, "bk1", Versioning::Unversioned);
    put(&engine, "bk1", "real", b"x", &ObjectAttrs::default());
    engine.execute("CREATE TABLE s3_objects (id INT)").unwrap();
    engine.execute("CREATE TABLE s3_buckets (id INT)").unwrap();
    let (_, rows) = sql_rows(&engine, "SELECT [key] FROM sys.s3_objects('bk1')");
    assert_eq!(cells(&rows, 0), ["real"]);
    let (_, rows) = sql_rows(&engine, "SELECT name FROM sys.s3_buckets");
    assert_eq!(cells(&rows, 0), ["bk1"]);
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_scan_streams_past_one_slice_while_writers_keep_writing() {
    let path = unique_temp_path("s3-sql-slices");
    let engine = std::sync::Arc::new(new_engine(&path));
    create_bucket(&engine, "many", Versioning::Unversioned);
    for i in 0..2_500 {
        put(
            &engine,
            "many",
            &format!("k{i:05}"),
            b"x",
            &ObjectAttrs::default(),
        );
    }
    let (_, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*), MIN([key]), MAX([key]) FROM sys.s3_objects('many')",
    );
    assert_eq!(
        rows[0],
        [
            Some("2500".into()),
            Some("k00000".into()),
            Some("k02499".into())
        ]
    );

    // Writers run between slices: neither side waits on the other for the
    // whole scan.
    let writer = {
        let engine = std::sync::Arc::clone(&engine);
        std::thread::spawn(move || {
            for i in 0..200 {
                put(
                    &engine,
                    "many",
                    &format!("w{i:03}"),
                    b"y",
                    &ObjectAttrs::default(),
                );
            }
        })
    };
    for _ in 0..5 {
        let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM sys.s3_objects('many', 'k')");
        assert_eq!(cells(&rows, 0), ["2500"]);
    }
    writer.join().expect("writer");
    let (_, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM sys.s3_objects('many', 'w')");
    assert_eq!(cells(&rows, 0), ["200"]);
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn the_journal_reads_as_a_table_from_a_sequence_on() {
    let path = unique_temp_path("s3-sql-journal");
    let engine = new_engine(&path);
    create_bucket(&engine, "jour", Versioning::Enabled);
    create_bucket(&engine, "other", Versioning::Unversioned);
    let v1 = put(&engine, "jour", "k", b"abc", &ObjectAttrs::default());
    put(&engine, "other", "x", b"x", &ObjectAttrs::default());
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.set_tags(
                    "jour",
                    "k",
                    truthdb_object::VersionSel::Latest,
                    vec![("t".into(), "1".into())],
                    4_000,
                )
            })
        })
        .expect("tag")
        .expect("tag accepted");
    engine
        .object(|file| file.transaction(|tx| tx.delete_object("jour", "k", None, 5_000, 0)))
        .expect("delete")
        .expect("delete accepted");
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.delete_object(
                    "jour",
                    "k",
                    Some(truthdb_object::VersionSel::Seq(v1)),
                    6_000,
                    0,
                )
            })
        })
        .expect("delete version")
        .expect("delete version accepted");

    let (columns, rows) = sql_rows(
        &engine,
        "SELECT record_type, version_id, is_delete_marker, size, e_tag, storage_class, is_multipart FROM sys.s3_journal('jour') ORDER BY sequence_number",
    );
    assert_eq!(
        columns,
        [
            "record_type",
            "version_id",
            "is_delete_marker",
            "size",
            "e_tag",
            "storage_class",
            "is_multipart"
        ]
    );
    let etag = format!("\"{}\"", "07".repeat(16));
    let v1 = v1.to_string();
    assert_eq!(rows.len(), 4, "{rows:?}");
    assert_eq!(
        rows[0],
        [
            Some("CREATE".into()),
            Some(v1.clone()),
            Some("0".into()),
            Some("3".into()),
            Some(etag.clone()),
            Some("STANDARD".into()),
            Some("0".into())
        ]
    );
    assert_eq!(rows[1][0].as_deref(), Some("UPDATE_METADATA"));
    assert_eq!(rows[1][1], Some(v1.clone()));
    assert_eq!(
        rows[2][..4],
        [
            Some("DELETE".into()),
            rows[2][1].clone(),
            Some("1".into()),
            Some("0".into())
        ]
    );
    assert_eq!(
        rows[3],
        [
            Some("DELETE".into()),
            Some(v1),
            None,
            None,
            None,
            None,
            None
        ],
        "a version removed for good carries only its identity"
    );

    // From a sequence on, and per bucket.
    let (_, rows) = sql_rows(
        &engine,
        "SELECT sequence_number FROM sys.s3_journal('jour') ORDER BY sequence_number",
    );
    let sequences = cells(&rows, 0);
    let (_, rows) = sql_rows(
        &engine,
        &format!(
            "SELECT sequence_number FROM sys.s3_journal('jour', {}) ORDER BY sequence_number",
            sequences[1]
        ),
    );
    assert_eq!(cells(&rows, 0), sequences[2..]);
    let (_, rows) = sql_rows(
        &engine,
        "SELECT [key], record_type FROM sys.s3_journal('other')",
    );
    assert_eq!(rows, [vec![Some("x".into()), Some("CREATE".into())]]);
    assert_eq!(
        sql_error_number(&engine, "SELECT * FROM sys.s3_journal('jour', 'x')"),
        206
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}
