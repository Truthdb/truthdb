//! `sys.s3_read`: SQL over stored CSV and JSON objects, streamed a chunk per
//! storage-lock hold under a lease that is released when the read ends.

use crate::engine::test_support::*;
use crate::engine::*;
use truthdb_object::{BodyWriter, ObjectAttrs, VersionSel};

fn bucket(engine: &Engine, name: &str) {
    engine
        .object(|file| file.transaction(|tx| tx.create_bucket(name, 1_000)))
        .expect("create")
        .expect("bucket");
}

/// Writes `data` as `key` in segments, as an upload would.
fn put(engine: &Engine, bucket: &str, key: &str, data: &[u8]) {
    let storage = engine.storage();
    let mut writer = BodyWriter::begin(storage, 1).expect("begin");
    for chunk in data.chunks(300_000) {
        writer.write(storage, chunk, 1).expect("write");
    }
    let sealed = writer.finish(storage, 1).expect("finish");
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.put_object(bucket, key, &sealed, &ObjectAttrs::default(), 2_000, 0)
            })
        })
        .expect("put")
        .expect("put accepted");
}

fn rows(engine: &Engine, sql: &str) -> Vec<Vec<Option<String>>> {
    sql_rows(engine, sql).1
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

/// The object's body is collected once it is deleted: no lease is left.
fn assert_collectable(engine: &Engine, bucket: &str, key: &str) {
    let body = engine
        .object(|file| file.head_object(bucket, key, VersionSel::Latest))
        .expect("head")
        .expect("exists")
        .body;
    engine
        .object(|file| file.transaction(|tx| tx.delete_object(bucket, key, None, 3_000, 0)))
        .expect("delete")
        .expect("delete accepted");
    engine.object_gc().expect("collect");
    let gone = engine.object(|file| file.body_info(body));
    assert!(
        gone.is_err(),
        "the body of {key} is still held after the read"
    );
}

#[test]
fn csv_objects_read_as_rows() {
    let path = unique_temp_path("s3-read-csv");
    let engine = new_engine(&path);
    bucket(&engine, "data");
    put(
        &engine,
        "data",
        "people.csv",
        "name,city,note\nAnn,Oslo,\"likes, commas\"\nBo,Malmö,\"two\nlines\"\nCy,Bergen\n"
            .as_bytes(),
    );
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT * FROM sys.s3_read('data', 'people.csv', 'csv')"
        ),
        (
            vec!["name".into(), "city".into(), "note".into()],
            vec![
                some(&["Ann", "Oslo", "likes, commas"]),
                some(&["Bo", "Malmö", "two\nlines"]),
                vec![Some("Cy".into()), Some("Bergen".into()), None],
            ]
        )
    );
    // SQL over the rows: filters, aggregates, casts.
    assert_eq!(
        rows(
            &engine,
            "SELECT city FROM sys.s3_read('data', 'people.csv', 'csv') WHERE name LIKE 'B%'"
        ),
        [some(&["Malmö"])]
    );
    put(&engine, "data", "plain.txt", b"1;2\n3;4\n");
    assert_eq!(
        sql_rows(
            &engine,
            r#"SELECT c1, c2 FROM sys.s3_read('data', 'plain.txt', 'csv', '{"header": false, "delimiter": ";"}')"#
        )
        .1,
        [some(&["1", "2"]), some(&["3", "4"])]
    );
    assert_eq!(
        rows(
            &engine,
            r#"SELECT COUNT(*) FROM sys.s3_read('data', 'people.csv', 'csv', '{"max_rows": 2}')"#
        ),
        [some(&["2"])]
    );

    // A body over many 1 MiB chunks, with multi-byte text across chunk edges.
    let mut big = String::from("id,word\n");
    for i in 0..150_000 {
        big.push_str(&format!("{i},smörgåsbord-{i}\n"));
    }
    assert!(big.len() > 3 * 1024 * 1024);
    put(&engine, "data", "big.csv", big.as_bytes());
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*), MAX(word) FROM sys.s3_read('data', 'big.csv', 'csv') WHERE word LIKE 'smörgåsbord-%'"
        ),
        [some(&["150000", "smörgåsbord-99999"])]
    );
    assert_collectable(&engine, "data", "big.csv");

    // A row longer than the header is an error, and the lease still goes.
    put(&engine, "data", "bad.csv", b"a,b\n1,2\n1,2,3\n");
    assert_eq!(
        sql_error_number(
            &engine,
            "SELECT * FROM sys.s3_read('data', 'bad.csv', 'csv')"
        ),
        4864
    );
    assert_collectable(&engine, "data", "bad.csv");
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn json_objects_read_as_rows() {
    let path = unique_temp_path("s3-read-json");
    let engine = new_engine(&path);
    bucket(&engine, "data");
    put(
        &engine,
        "data",
        "events.jsonl",
        br#"{"id": 1, "kind": "click", "tags": ["a", "b"]}
{"id": 2, "kind": null}
{"id": 3, "extra": {"deep": true}}
"#,
    );
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT * FROM sys.s3_read('data', 'events.jsonl', 'json')"
        ),
        (
            vec!["id".into(), "kind".into(), "tags".into(), "extra".into()],
            vec![
                vec![
                    Some("1".into()),
                    Some("click".into()),
                    Some(r#"["a", "b"]"#.into()),
                    None
                ],
                vec![Some("2".into()), None, None, None],
                vec![
                    Some("3".into()),
                    None,
                    None,
                    Some(r#"{"deep": true}"#.into())
                ],
            ]
        )
    );
    put(
        &engine,
        "data",
        "array.json",
        br#"[{"n": "x"}, {"n": "y"}]"#,
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT n FROM sys.s3_read('data', 'array.json', 'json')"
        ),
        [some(&["x"]), some(&["y"])]
    );
    put(&engine, "data", "scalars.json", b"1 \"two\" [3]");
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT * FROM sys.s3_read('data', 'scalars.json', 'json')"
        ),
        (
            vec!["value".into()],
            vec![some(&["1"]), some(&["two"]), some(&["[3]"])]
        )
    );
    put(&engine, "data", "broken.json", b"{\"a\": 1}\n{oops");
    assert_eq!(
        sql_error_number(
            &engine,
            "SELECT * FROM sys.s3_read('data', 'broken.json', 'json')"
        ),
        4864
    );
    assert_collectable(&engine, "data", "broken.json");
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_prefix_reads_every_object_under_it_and_a_multipart_body_reads_whole() {
    let path = unique_temp_path("s3-read-prefix");
    let engine = new_engine(&path);
    bucket(&engine, "logs");
    put(
        &engine,
        "logs",
        "2025/01.csv",
        b"level,msg\nINFO,start\nERROR,boom\n",
    );
    put(&engine, "logs", "2025/02.csv", b"level,msg\nERROR,again\n");
    put(&engine, "logs", "2026/01.csv", b"level,msg\nINFO,later\n");
    assert_eq!(
        rows(
            &engine,
            r#"SELECT _s3_key, msg FROM sys.s3_read('logs', '2025/', 'csv', '{"prefix": true}') WHERE level = 'ERROR'"#
        ),
        [
            some(&["2025/01.csv", "boom"]),
            some(&["2025/02.csv", "again"])
        ]
    );
    assert_eq!(
        sql_rows(
            &engine,
            r#"SELECT * FROM sys.s3_read('logs', 'none/', 'csv', '{"prefix": true}')"#
        ),
        (vec!["_s3_key".into()], Vec::<Vec<Option<String>>>::new())
    );
    // CROSS APPLY reads files chosen by the inventory.
    assert_eq!(
        rows(
            &engine,
            "SELECT o.[key], COUNT(*) FROM sys.s3_objects('logs', '2025/') o CROSS APPLY sys.s3_read('logs', o.[key], 'csv') r GROUP BY o.[key] ORDER BY o.[key]"
        ),
        [some(&["2025/01.csv", "2"]), some(&["2025/02.csv", "1"])]
    );

    // A multipart object (a composite of parts) reads across its parts.
    let part1: String = (0..1_100_000).map(|i| format!("{},p1\n", i % 10)).collect();
    let part1 = format!("n,part\n{part1}");
    let part2 = "7,p2\n8,p2\n";
    let upload = engine
        .object(|file| {
            file.transaction(|tx| tx.create_upload("logs", "multi.csv", &ObjectAttrs::default(), 1))
        })
        .expect("create upload")
        .expect("upload");
    let storage = engine.storage();
    let mut md5s = Vec::new();
    for (number, data) in [(1u32, part1.as_bytes()), (2, part2.as_bytes())] {
        let mut writer = BodyWriter::begin(storage, 1).unwrap();
        writer.write(storage, data, 1).unwrap();
        let sealed = writer.finish(storage, 1).unwrap();
        let part = engine
            .object(|file| file.transaction(|tx| tx.upload_part(upload, number, &sealed, 1, 0)))
            .expect("part")
            .expect("part accepted");
        md5s.push((number, part.md5));
    }
    assert!(part1.len() as u64 >= truthdb_object::MIN_PART_BYTES);
    engine
        .object(|file| file.transaction(|tx| tx.complete_upload(upload, &md5s, 2_000, 0)))
        .expect("complete")
        .expect("completed");
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*), SUM(CASE WHEN part = 'p2' THEN 1 ELSE 0 END) FROM sys.s3_read('logs', 'multi.csv', 'csv')"
        ),
        [some(&["1100002", "2"])]
    );

    // Errors: no such object, an unknown format or option.
    assert_eq!(
        sql_error_number(
            &engine,
            "SELECT * FROM sys.s3_read('logs', 'nope.csv', 'csv')"
        ),
        208
    );
    assert_eq!(
        sql_error_number(
            &engine,
            "SELECT * FROM sys.s3_read('logs', '2025/01.csv', 'xml')"
        ),
        4864
    );
    assert_eq!(
        sql_error_number(
            &engine,
            r#"SELECT * FROM sys.s3_read('logs', '2025/01.csv', 'csv', '{"nope": 1}')"#
        ),
        4864
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn reads_hold_up_to_odd_files() {
    let path = unique_temp_path("s3-read-odd");
    let engine = new_engine(&path);
    bucket(&engine, "odd");
    // Prefix mode: later files by their header names; folder markers.
    put(&engine, "odd", "ord/", b"");
    put(&engine, "odd", "ord/a.csv", b"x,y\n1,2\n");
    put(&engine, "odd", "ord/b.csv", b"y,x\n20,10\n");
    put(&engine, "odd", "ord/c.csv", b"x\n100\n");
    assert_eq!(
        rows(
            &engine,
            r#"SELECT * FROM sys.s3_read('odd', 'ord/', 'csv', '{"prefix": true}')"#
        ),
        [
            some(&["ord/a.csv", "1", "2"]),
            some(&["ord/b.csv", "10", "20"]),
            vec![Some("ord/c.csv".into()), Some("100".into()), None],
        ]
    );
    put(&engine, "odd", "bad/a.csv", b"x\n1\n");
    put(&engine, "odd", "bad/b.csv", b"z\n2\n");
    assert_eq!(
        sql_error_number(
            &engine,
            r#"SELECT * FROM sys.s3_read('odd', 'bad/', 'csv', '{"prefix": true}')"#
        ),
        4864
    );
    put(&engine, "odd", "js/", b"");
    put(&engine, "odd", "js/a.json", br#"{"x": 1, "y": 2}"#);
    assert_eq!(
        sql_rows(
            &engine,
            r#"SELECT * FROM sys.s3_read('odd', 'js/', 'json', '{"prefix": true}')"#
        ),
        (
            vec!["_s3_key".into(), "x".into(), "y".into()],
            vec![some(&["js/a.json", "1", "2"])]
        )
    );

    // JSON: a byte order mark, records that are arrays, exact numbers, and
    // keys in the order written.
    put(&engine, "odd", "bom.json", b"\xEF\xBB\xBF{\"x\": 1}");
    assert_eq!(
        rows(
            &engine,
            "SELECT x FROM sys.s3_read('odd', 'bom.json', 'json')"
        ),
        [some(&["1"])]
    );
    put(&engine, "odd", "arrays.jsonl", b"[1,2]\n[3,4]\n");
    assert_eq!(
        rows(
            &engine,
            "SELECT value FROM sys.s3_read('odd', 'arrays.jsonl', 'json')"
        ),
        [some(&["[1,2]"]), some(&["[3,4]"])]
    );
    put(
        &engine,
        "odd",
        "numbers.json",
        br#"{"zeta": 12345678901234567890123, "alpha": 0.1000000000000000055511151231257827, "mid": 1.50}"#,
    );
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT * FROM sys.s3_read('odd', 'numbers.json', 'json')"
        ),
        (
            vec!["zeta".into(), "alpha".into(), "mid".into()],
            vec![some(&[
                "12345678901234567890123",
                "0.1000000000000000055511151231257827",
                "1.50"
            ])]
        )
    );

    // Column names made unique, the prefix key reserved; no columns at all
    // and bad separators are errors.
    put(&engine, "odd", "dups.csv", b"a,a,,_s3_key\n1,2,3,4\n");
    assert_eq!(
        sql_rows(
            &engine,
            "SELECT * FROM sys.s3_read('odd', 'dups.csv', 'csv')"
        )
        .0,
        ["a", "a_2", "c3", "_s3_key"]
    );
    put(&engine, "odd", "p/dups.csv", b"_s3_key,b\n1,2\n");
    assert_eq!(
        sql_rows(
            &engine,
            r#"SELECT * FROM sys.s3_read('odd', 'p/', 'csv', '{"prefix": true}')"#
        )
        .0,
        ["_s3_key", "_s3_key_2", "b"]
    );
    put(&engine, "odd", "empty.csv", b"");
    put(&engine, "odd", "empty.json", b"{}\n{}\n");
    for query in [
        "SELECT * FROM sys.s3_read('odd', 'empty.csv', 'csv')",
        "SELECT * FROM sys.s3_read('odd', 'empty.json', 'json')",
        r#"SELECT * FROM sys.s3_read('odd', 'dups.csv', 'csv', '{"delimiter": "\n"}')"#,
        r#"SELECT * FROM sys.s3_read('odd', 'dups.csv', 'csv', '{"delimiter": "\""}')"#,
    ] {
        assert_eq!(sql_error_number(&engine, query), 4864, "{query}");
    }

    // One record without an end is refused, not read whole.
    let endless = vec![b'x'; 4 * 1024 * 1024];
    put(&engine, "odd", "endless.csv", &endless);
    assert_eq!(
        sql_error_number(
            &engine,
            r#"SELECT COUNT(*) FROM sys.s3_read('odd', 'endless.csv', 'csv', '{"header": false}')"#
        ),
        4864
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}
