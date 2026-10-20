//! The full-text indexer: enabling a bucket backfills it, the journal keeps
//! it current (overwrites, deletes, delete markers), only text within the
//! size is indexed, a restart or a repeated row changes nothing, and
//! disabling purges it.

use crate::engine::test_support::*;
use crate::engine::*;
use truthdb_object::{BodyWriter, FtKey, FtState, FtStatus, ObjectAttrs, VersionSel, Versioning};

fn now() -> u64 {
    2_000
}

fn put(engine: &Engine, bucket: &str, key: &str, data: &[u8], content_type: &str) -> u64 {
    put_with(
        engine,
        bucket,
        key,
        data,
        ObjectAttrs {
            content_type: Some(content_type.to_string()),
            ..ObjectAttrs::default()
        },
    )
}

fn put_with(engine: &Engine, bucket: &str, key: &str, data: &[u8], attrs: ObjectAttrs) -> u64 {
    let storage = engine.storage();
    let mut writer = BodyWriter::begin(storage, now()).expect("begin");
    for chunk in data.chunks(300_000) {
        writer.write(storage, chunk, now()).expect("write");
    }
    let sealed = writer.finish(storage, now()).expect("finish");
    engine
        .object(|file| file.transaction(|tx| tx.put_object(bucket, key, &sealed, &attrs, now(), 0)))
        .expect("put")
        .expect("put accepted")
        .version
}

/// Runs indexer passes until no work remains.
fn index(engine: &Engine) -> usize {
    let mut passes = 1;
    while engine.fulltext_pass(&|| false).expect("pass") {
        passes += 1;
        assert!(passes < 1000, "the indexer does not finish");
    }
    passes
}

/// The keys whose latest version matches `condition`, sorted.
fn hits(engine: &Engine, bucket: &str, condition: &str) -> Vec<String> {
    let mut search = truthdb_fulltext::Search::new(condition).expect("condition");
    let docs = engine
        .object(|file| {
            assert!(file.fulltext_step(&mut search, bucket, usize::MAX).unwrap());
            Ok(search.rank(file.fulltext_stats(bucket)?))
        })
        .expect("search");
    let mut keys: Vec<String> = engine
        .object(|file| {
            let listing = file
                .list_objects(bucket, "", None, None, 1000)?
                .expect("bucket");
            Ok(listing
                .objects
                .into_iter()
                .filter(|object| docs.iter().any(|hit| hit.doc == object.info.version))
                .map(|object| object.key)
                .collect())
        })
        .expect("list");
    assert_eq!(keys.len(), docs.len(), "every hit is a latest version");
    keys.sort();
    keys
}

fn held(engine: &Engine, bucket: &str, key: &str) -> Option<FtKey> {
    engine
        .object(|file| file.fulltext_key(bucket, key))
        .expect("key")
}

fn command(engine: &Engine, text: &str) -> String {
    engine.execute(text).expect("command")
}

fn states(engine: &Engine) -> Vec<(String, FtState)> {
    engine
        .object(|file| file.fulltext_buckets())
        .expect("buckets")
        .into_iter()
        .map(|bucket| (bucket.name, bucket.state))
        .collect()
}

#[test]
fn enabling_backfills_text_objects_and_skips_the_rest() {
    let path = unique_temp_path("ft-backfill");
    let engine = new_engine(&path);
    engine
        .object(|file| {
            file.transaction(|tx| tx.create_bucket("docs", truthdb_object::DEFAULT_ACCOUNT, now()))
        })
        .unwrap()
        .unwrap();
    // More keys than one backfill pass takes.
    for n in 0..150 {
        put(
            &engine,
            "docs",
            &format!("note/{n:03}"),
            format!("note number{n} common").as_bytes(),
            "text/plain; charset=utf-8",
        );
    }
    put(
        &engine,
        "docs",
        "data.json",
        br#"{"city": "Lisbon"}"#,
        "application/json",
    );
    put(
        &engine,
        "docs",
        "feed.xml",
        b"<t>Lisbon feed</t>",
        "application/atom+xml",
    );
    put(&engine, "docs", "photo.jpg", b"Lisbon pixels", "image/jpeg");
    put(&engine, "docs", "untyped", b"Lisbon untyped", "");
    put_with(
        &engine,
        "docs",
        "packed.txt",
        b"Lisbon packed",
        ObjectAttrs {
            content_type: Some("text/plain".into()),
            content_encoding: Some("gzip".into()),
            ..ObjectAttrs::default()
        },
    );
    put(
        &engine,
        "docs",
        "big.txt",
        &b"Lisbon ".repeat(200),
        "text/plain",
    );

    assert!(command(&engine, "s3 enable fulltext docs 1000").contains("backfilling"));
    assert_eq!(
        states(&engine),
        [("docs".to_string(), FtState::Backfilling(None))]
    );
    assert!(index(&engine) > 2);
    assert_eq!(states(&engine), [("docs".to_string(), FtState::On)]);

    assert_eq!(hits(&engine, "docs", "lisbon"), ["data.json", "feed.xml"]);
    assert_eq!(hits(&engine, "docs", "common").len(), 150);
    assert_eq!(hits(&engine, "docs", "number42"), ["note/042"]);
    let status = |key| held(&engine, "docs", key).map(|held| held.status);
    assert_eq!(status("photo.jpg"), Some(FtStatus::NotText));
    assert_eq!(status("untyped"), Some(FtStatus::NotText));
    assert_eq!(status("packed.txt"), Some(FtStatus::Encoded));
    assert_eq!(status("big.txt"), Some(FtStatus::TooLarge));

    // Enabling again with a larger size looks at every key anew: the large
    // one is indexed now, and the rest are left as they are.
    command(&engine, "s3 enable fulltext docs 100000");
    index(&engine);
    assert_eq!(
        hits(&engine, "docs", "lisbon"),
        ["big.txt", "data.json", "feed.xml"]
    );
    assert_eq!(hits(&engine, "docs", "common").len(), 150);

    // Errors: no such bucket, a bad size, disabling twice.
    assert!(engine.execute("s3 enable fulltext nope").is_err());
    assert!(engine.execute("s3 enable fulltext docs -5").is_err());
    assert!(command(&engine, "s3 disable fulltext docs").contains("purging"));
    assert!(engine.execute("s3 disable fulltext docs").is_err());
    index(&engine);
    assert!(states(&engine).is_empty());
    assert!(hits(&engine, "docs", "common").is_empty());
    assert!(held(&engine, "docs", "note/001").is_none());
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn the_journal_keeps_the_index_on_each_keys_latest_version() {
    let path = unique_temp_path("ft-journal");
    let engine = new_engine(&path);
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.create_bucket("plain", truthdb_object::DEFAULT_ACCOUNT, now())?
                    .unwrap();
                tx.create_bucket("hist", truthdb_object::DEFAULT_ACCOUNT, now())?
                    .unwrap();
                tx.set_versioning("hist", Versioning::Enabled)
            })
        })
        .unwrap()
        .unwrap();
    command(&engine, "s3 enable fulltext plain");
    command(&engine, "s3 enable fulltext hist");
    index(&engine);

    // An overwrite replaces the old text; a delete removes it.
    put(&engine, "plain", "a", b"red apple", "text/plain");
    put(&engine, "plain", "b", b"red berry", "text/plain");
    index(&engine);
    assert_eq!(hits(&engine, "plain", "red"), ["a", "b"]);
    put(&engine, "plain", "a", b"green apple", "text/plain");
    index(&engine);
    assert_eq!(hits(&engine, "plain", "red"), ["b"]);
    assert_eq!(hits(&engine, "plain", "green"), ["a"]);
    engine
        .object(|file| file.transaction(|tx| tx.delete_object("plain", "b", None, now(), 0)))
        .unwrap()
        .unwrap();
    // Overwritten with something that is not text: it leaves the index.
    put(&engine, "plain", "a", b"green", "image/png");
    index(&engine);
    assert!(hits(&engine, "plain", "red or green").is_empty());
    assert!(held(&engine, "plain", "b").is_none());
    assert_eq!(
        held(&engine, "plain", "a").unwrap().status,
        FtStatus::NotText
    );

    // Versioned: a delete marker hides the text; removing the marker brings
    // the older version back into the index.
    let v1 = put(&engine, "hist", "k", b"first draft", "text/plain");
    let v2 = put(&engine, "hist", "k", b"second draft", "text/plain");
    index(&engine);
    assert_eq!(hits(&engine, "hist", "draft"), ["k"]);
    assert_eq!(held(&engine, "hist", "k").unwrap().version, v2);
    let marker = engine
        .object(|file| file.transaction(|tx| tx.delete_object("hist", "k", None, now(), 0)))
        .unwrap()
        .unwrap()
        .version
        .unwrap();
    index(&engine);
    assert!(held(&engine, "hist", "k").is_none());
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.delete_object("hist", "k", Some(VersionSel::Seq(marker)), now(), 0)
            })
        })
        .unwrap()
        .unwrap();
    index(&engine);
    assert_eq!(held(&engine, "hist", "k").unwrap().version, v2);
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.delete_object("hist", "k", Some(VersionSel::Seq(v2)), now(), 0)
            })
        })
        .unwrap()
        .unwrap();
    index(&engine);
    assert_eq!(held(&engine, "hist", "k").unwrap().version, v1);
    assert_eq!(hits(&engine, "hist", "first"), ["k"]);
    assert!(hits(&engine, "hist", "second").is_empty());

    // A bucket that is not indexed is left alone.
    engine
        .object(|file| {
            file.transaction(|tx| tx.create_bucket("other", truthdb_object::DEFAULT_ACCOUNT, now()))
        })
        .unwrap()
        .unwrap();
    put(&engine, "other", "x", b"red", "text/plain");
    index(&engine);
    assert!(held(&engine, "other", "x").is_none());
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_restart_or_a_row_applied_twice_changes_nothing() {
    let path = unique_temp_path("ft-restart");
    let engine = new_engine(&path);
    engine
        .object(|file| {
            file.transaction(|tx| tx.create_bucket("docs", truthdb_object::DEFAULT_ACCOUNT, now()))
        })
        .unwrap()
        .unwrap();
    command(&engine, "s3 enable fulltext docs");
    index(&engine);
    put(&engine, "docs", "a", b"one two", "text/plain");
    put(&engine, "docs", "b", b"two three", "text/plain");
    // The rows are applied but the cursor is not advanced (as when the
    // indexer crashes between the two), then the store goes down uncleanly.
    let bucket = engine
        .object(|file| file.fulltext_buckets())
        .unwrap()
        .remove(0);
    assert!(engine.fulltext_sync(&bucket, "a", &|| false).unwrap());
    assert!(engine.fulltext_sync(&bucket, "b", &|| false).unwrap());
    let cursor = engine.object(|file| file.fulltext_cursor()).unwrap();
    drop(engine);
    let engine =
        Engine::new(RelStorage::open_with(path.clone(), crate::database_engines).expect("open"))
            .expect("engine");
    assert_eq!(
        engine.object(|file| file.fulltext_cursor()).unwrap(),
        cursor
    );
    assert_eq!(hits(&engine, "docs", "two"), ["a", "b"]);
    // Applying the rows again (and backfilling again) changes nothing.
    index(&engine);
    command(&engine, "s3 enable fulltext docs");
    index(&engine);
    assert_eq!(hits(&engine, "docs", "two"), ["a", "b"]);
    assert_eq!(hits(&engine, "docs", "three"), ["b"]);
    assert_eq!(
        engine.object(|file| file.fulltext_stats("docs")).unwrap(),
        truthdb_fulltext::Stats {
            docs: 2,
            total_len: 4
        }
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

fn small_ring_engine(path: &std::path::Path) -> Engine {
    let storage = RelStorage::create_with_wal_bounds_with(
        path.to_path_buf(),
        test_storage_options(),
        8 * 1024 * 1024,
        8 * 1024 * 1024,
        crate::database_engines,
    )
    .expect("create");
    Engine::new(storage).expect("engine")
}

fn reopen(path: &std::path::Path) -> Engine {
    Engine::new(RelStorage::open_with(path.to_path_buf(), crate::database_engines).expect("open"))
        .expect("engine")
}

#[test]
fn a_document_too_large_for_one_transaction_is_indexed_over_several() {
    // An 8 MiB ring: a 2 MiB compensation reserve, so one transaction may
    // write a quarter MiB of log; forty thousand distinct words take far
    // more, and are written (and later removed) a transaction at a time.
    let path = unique_temp_path("ft-big-doc");
    let engine = small_ring_engine(&path);
    engine
        .object(|file| {
            file.transaction(|tx| tx.create_bucket("docs", truthdb_object::DEFAULT_ACCOUNT, now()))
        })
        .unwrap()
        .unwrap();
    command(&engine, "s3 enable fulltext docs");
    let big: String = (0..40_000).map(|n| format!("word{n} ")).collect();
    put(&engine, "docs", "big.txt", big.as_bytes(), "text/plain");
    put(&engine, "docs", "small.txt", b"word7 small", "text/plain");
    index(&engine);
    assert_eq!(
        held(&engine, "docs", "big.txt").unwrap().status,
        FtStatus::Indexed
    );
    assert_eq!(hits(&engine, "docs", "word7"), ["big.txt", "small.txt"]);
    assert_eq!(hits(&engine, "docs", "word39999"), ["big.txt"]);
    // Overwritten: the large document is removed over several transactions
    // too. The store goes on: a write, then a clean reopen.
    put(&engine, "docs", "big.txt", b"replaced", "text/plain");
    put(&engine, "docs", "after.txt", b"after", "text/plain");
    index(&engine);
    assert_eq!(hits(&engine, "docs", "word7"), ["small.txt"]);
    drop(engine);
    let engine = reopen(&path);
    assert_eq!(hits(&engine, "docs", "after"), ["after.txt"]);
    assert_eq!(hits(&engine, "docs", "replaced"), ["big.txt"]);
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn purging_a_large_index_through_a_small_log_takes_bounded_transactions() {
    let path = unique_temp_path("ft-big-purge");
    let engine = small_ring_engine(&path);
    engine
        .object(|file| {
            file.transaction(|tx| tx.create_bucket("docs", truthdb_object::DEFAULT_ACCOUNT, now()))
        })
        .unwrap()
        .unwrap();
    command(&engine, "s3 enable fulltext docs");
    for n in 0..300 {
        let text: String = (0..200).map(|w| format!("d{n}w{w} ")).collect();
        put(
            &engine,
            "docs",
            &format!("k{n:03}"),
            text.as_bytes(),
            "text/plain",
        );
    }
    index(&engine);
    assert_eq!(hits(&engine, "docs", "d150w7"), ["k150"]);
    command(&engine, "s3 disable fulltext docs");
    let passes = index(&engine);
    assert!(passes > 2, "{passes} passes");
    assert!(states(&engine).is_empty());
    put(&engine, "docs", "after.txt", b"after", "text/plain");
    drop(engine);
    let engine = reopen(&path);
    assert!(
        engine
            .object(|file| file.fulltext_buckets())
            .unwrap()
            .is_empty()
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_pass_from_before_a_re_enable_changes_nothing() {
    let path = unique_temp_path("ft-generation");
    let engine = new_engine(&path);
    engine
        .object(|file| {
            file.transaction(|tx| tx.create_bucket("docs", truthdb_object::DEFAULT_ACCOUNT, now()))
        })
        .unwrap()
        .unwrap();
    for n in 0..100 {
        put(
            &engine,
            "docs",
            &format!("k{n:03}"),
            b"some words here",
            "text/plain",
        );
    }
    command(&engine, "s3 enable fulltext docs 5");
    let stale = engine
        .object(|file| file.fulltext_buckets())
        .unwrap()
        .remove(0);
    // Enabled again (a larger size) while a pass holds the old state.
    command(&engine, "s3 enable fulltext docs 1000");
    // The stale pass syncs a key with its old size and finishes its backfill:
    // neither lands.
    engine.fulltext_sync(&stale, "k000", &|| false).unwrap();
    assert!(held(&engine, "docs", "k000").is_none());
    engine
        .object(|file| {
            file.transaction(|tx| tx.set_fulltext_backfill("docs", stale.generation, None))
        })
        .unwrap();
    assert_eq!(
        states(&engine),
        [("docs".to_string(), FtState::Backfilling(None))]
    );
    index(&engine);
    assert_eq!(hits(&engine, "docs", "words").len(), 100);
    // Too large a size is refused.
    assert!(
        engine
            .execute("s3 enable fulltext docs 999999999999")
            .is_err()
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}

/// The raw documents of `bucket`'s index matching `condition`, version rows
/// or not (a document left behind would show here, not in `hits`).
fn raw_docs(engine: &Engine, bucket: &str, condition: &str) -> Vec<u64> {
    let mut search = truthdb_fulltext::Search::new(condition).expect("condition");
    engine
        .object(|file| {
            assert!(file.fulltext_step(&mut search, bucket, usize::MAX).unwrap());
            Ok(search
                .rank(file.fulltext_stats(bucket)?)
                .iter()
                .map(|hit| hit.doc)
                .collect())
        })
        .expect("search")
}

/// A pass from before a re-enable dropped the key's delete row and moved
/// the cursor past it (as a stale pass does).
fn skip_journal(engine: &Engine) {
    let last = engine
        .object(|file| Ok(file.journal(0, 100_000)?.last().map(|row| row.sequence)))
        .unwrap()
        .unwrap();
    engine
        .object(|file| file.transaction(|tx| tx.set_fulltext_cursor(last)))
        .unwrap();
}

#[test]
fn a_key_deleted_while_a_stale_pass_ran_is_removed_by_the_new_backfill() {
    // A whole document left behind.
    let path = unique_temp_path("ft-stale-delete");
    let engine = new_engine(&path);
    engine
        .object(|file| {
            file.transaction(|tx| tx.create_bucket("docs", truthdb_object::DEFAULT_ACCOUNT, now()))
        })
        .unwrap()
        .unwrap();
    command(&engine, "s3 enable fulltext docs");
    put(&engine, "docs", "k", b"hello world", "text/plain");
    index(&engine);
    let stale = engine
        .object(|file| file.fulltext_buckets())
        .unwrap()
        .remove(0);
    engine
        .object(|file| file.transaction(|tx| tx.delete_object("docs", "k", None, now(), 0)))
        .unwrap()
        .unwrap();
    command(&engine, "s3 enable fulltext docs");
    assert!(engine.fulltext_sync(&stale, "k", &|| false).unwrap());
    skip_journal(&engine);
    index(&engine);
    assert!(held(&engine, "docs", "k").is_none());
    assert!(raw_docs(&engine, "docs", "hello").is_empty());
    assert_eq!(
        engine
            .object(|file| file.fulltext_stats("docs"))
            .unwrap()
            .docs,
        0
    );
    drop(engine);
    let _ = std::fs::remove_file(path);

    // A large document part removed when the re-enable came.
    let path = unique_temp_path("ft-stale-partial");
    let engine = small_ring_engine(&path);
    engine
        .object(|file| {
            file.transaction(|tx| tx.create_bucket("docs", truthdb_object::DEFAULT_ACCOUNT, now()))
        })
        .unwrap()
        .unwrap();
    command(&engine, "s3 enable fulltext docs");
    let big: String = (0..3000).map(|n| format!("t{n} ")).collect();
    put(&engine, "docs", "k", big.as_bytes(), "text/plain");
    index(&engine);
    let stale = engine
        .object(|file| file.fulltext_buckets())
        .unwrap()
        .remove(0);
    engine
        .object(|file| file.transaction(|tx| tx.delete_object("docs", "k", None, now(), 0)))
        .unwrap()
        .unwrap();
    assert_eq!(
        engine
            .object(|file| file.transaction(|tx| tx.fulltext_remove_step(
                "docs",
                "k",
                stale.generation
            )))
            .unwrap(),
        Some(false),
        "one step does not remove three thousand terms"
    );
    assert_eq!(
        held(&engine, "docs", "k").unwrap().status,
        FtStatus::Partial
    );
    command(&engine, "s3 enable fulltext docs");
    skip_journal(&engine);
    index(&engine);
    assert!(held(&engine, "docs", "k").is_none());
    assert!(raw_docs(&engine, "docs", "t42 or t2999").is_empty());
    drop(engine);
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_stopped_pass_leaves_nothing_skipped() {
    let path = unique_temp_path("ft-stop");
    let engine = small_ring_engine(&path);
    engine
        .object(|file| {
            file.transaction(|tx| tx.create_bucket("docs", truthdb_object::DEFAULT_ACCOUNT, now()))
        })
        .unwrap()
        .unwrap();
    command(&engine, "s3 enable fulltext docs");
    index(&engine);
    let big: String = (0..3000).map(|n| format!("t{n} ")).collect();
    put(&engine, "docs", "big", big.as_bytes(), "text/plain");
    put(&engine, "docs", "small", b"t7 small", "text/plain");
    // Stopped after a few steps, again and again: every pass leaves the
    // big document part written, and the next one finishes it.
    for _ in 0..20 {
        let calls = std::cell::Cell::new(0);
        let stop = || {
            calls.set(calls.get() + 1);
            calls.get() > 3
        };
        let _ = engine.fulltext_pass(&stop).unwrap();
    }
    index(&engine);
    assert_eq!(hits(&engine, "docs", "t7"), ["big", "small"]);
    assert_eq!(hits(&engine, "docs", "t2999"), ["big"]);
    drop(engine);
    let _ = std::fs::remove_file(path);
}
