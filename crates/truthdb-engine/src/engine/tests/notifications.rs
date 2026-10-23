//! Bucket event notifications: the journal consumer sends each matching
//! change's event to its topic once, even when a batch is sent again (a
//! crash between the append and the consumer's move), as its bucket was
//! configured when the change was made; drops the events of a topic that is
//! gone or not the bucket owner's; and starts at the journal's end.

use crate::engine::test_support::*;
use crate::engine::*;
use truthdb_object::{
    Destination, NotificationConfig, NotificationRule, ObjectAttrs, SealedBody, StorageClass,
};

fn put(engine: &Engine, bucket: &str, key: &str) {
    engine
        .object(|file| {
            let body = file.begin_body(1)?;
            file.append_segment(body, b"data", 1)?;
            let sealed = SealedBody {
                body,
                md5: [7; 16],
                size: None,
                pack_level: 0,
            };
            file.transaction(|tx| {
                tx.put_object(
                    bucket,
                    key,
                    &sealed,
                    &ObjectAttrs::default(),
                    StorageClass::Standard,
                    2_000,
                    0,
                )
            })
        })
        .expect("put")
        .expect("put accepted");
}

fn delete(engine: &Engine, bucket: &str, key: &str) {
    engine
        .object(|file| file.transaction(|tx| tx.delete_object(bucket, key, None, false, 3_000, 0)))
        .expect("delete")
        .expect("delete accepted");
}

fn rule(id: &str, event: &str, prefix: Option<&str>, suffix: Option<&str>) -> NotificationRule {
    NotificationRule {
        id: id.into(),
        events: vec![event.into()],
        prefix: prefix.map(str::to_string),
        suffix: suffix.map(str::to_string),
        destination: Destination::Topic("arn:aws:sns:us-east-1:000000000000:events".into()),
    }
}

fn configure(engine: &Engine, rules: Vec<NotificationRule>) {
    configure_bucket(engine, "photos", rules);
}

fn configure_bucket(engine: &Engine, bucket: &str, rules: Vec<NotificationRule>) {
    let config = NotificationConfig {
        region: "us-east-1".into(),
        rules,
    };
    engine
        .object(|file| file.transaction(|tx| tx.put_notification(bucket, &config)))
        .expect("configure");
}

/// Every event in the topic: (partition, event name, key, sequencer).
fn events(engine: &Engine) -> Vec<(u32, String, String, String)> {
    let mut out = Vec::new();
    for partition in 0..2 {
        let (records, _) = engine
            .streams(|file| file.read("events", partition, 0, 1000, usize::MAX))
            .unwrap()
            .unwrap();
        for record in records {
            let message: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
            let event = &message["Records"][0];
            out.push((
                partition,
                event["eventName"].as_str().unwrap().to_string(),
                event["s3"]["object"]["key"].as_str().unwrap().to_string(),
                event["s3"]["object"]["sequencer"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            ));
        }
    }
    out
}

fn drain(engine: &Engine) {
    while engine.notifications_step().unwrap() {}
}

#[test]
fn changes_send_their_events_once_even_when_a_batch_is_sent_again() {
    let path = unique_temp_path("notifications");
    let engine = new_engine(&path);
    engine
        .execute("stream create topic events partitions 2 retention 0 account 000000000000")
        .unwrap();
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.create_bucket("photos", truthdb_object::DEFAULT_ACCOUNT, 1_000)?
                    .expect("bucket");
                Ok(())
            })
        })
        .unwrap();
    // Before any configuration: no consumer, no event.
    put(&engine, "photos", "in/before.jpg");
    assert!(!engine.notifications_step().unwrap());
    configure(
        &engine,
        vec![
            rule("created", "s3:ObjectCreated:*", Some("in/"), None),
            rule("removed", "s3:ObjectRemoved:*", None, Some(".jpg")),
        ],
    );
    put(&engine, "photos", "in/1.jpg");
    put(&engine, "photos", "in/2.txt");
    put(&engine, "photos", "out/3.txt");
    delete(&engine, "photos", "in/1.jpg");
    delete(&engine, "photos", "out/3.txt");

    // A batch sent, then sent again before the consumer moved past it.
    let cursor = engine
        .object(|file| file.notifications_cursor())
        .unwrap()
        .expect("the consumer");
    let (through, more) = engine.notifications_send(cursor).unwrap().unwrap();
    assert!(!more);
    assert_eq!(
        engine.notifications_send(cursor).unwrap(),
        Some((through, more))
    );
    engine
        .object(|file| file.transaction(|tx| tx.advance_notifications(through)))
        .unwrap();
    assert!(!engine.notifications_step().unwrap());

    let mut sent = events(&engine);
    // One key's events stay in one partition, in order.
    let partition_of = |key: &str| {
        sent.iter()
            .filter(|(_, _, k, _)| k == key)
            .map(|(p, ..)| *p)
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(partition_of("in%2F1.jpg").len(), 1);
    sent.sort_by(|a, b| a.3.cmp(&b.3));
    let names: Vec<(String, String)> = sent
        .into_iter()
        .map(|(_, name, key, _)| (name, key))
        .collect();
    assert_eq!(
        names,
        [
            ("ObjectCreated:Put".to_string(), "in%2F1.jpg".to_string()),
            ("ObjectCreated:Put".to_string(), "in%2F2.txt".to_string()),
            ("ObjectRemoved:Delete".to_string(), "in%2F1.jpg".to_string()),
        ]
    );

    // A topic that is gone drops its events: the consumer still moves on.
    engine.execute("stream drop topic events").unwrap();
    put(&engine, "photos", "in/4.jpg");
    drain(&engine);
    let (cursor, last) = engine
        .object(|file| Ok((file.notifications_cursor()?, file.last_change()?)))
        .unwrap();
    assert_eq!(cursor, Some(last));

    // A change is sent as its bucket was configured when it was made: a
    // bucket configured after its change, a configuration replaced, and a
    // bucket emptied and dropped (then made again) before the consumer
    // came to their changes.
    engine
        .execute("stream create topic events partitions 2 retention 0 account 000000000000")
        .unwrap();
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.create_bucket("other", truthdb_object::DEFAULT_ACCOUNT, 1_000)?
                    .expect("bucket");
                Ok(())
            })
        })
        .unwrap();
    put(&engine, "other", "in/early.jpg");
    configure_bucket(
        &engine,
        "other",
        vec![rule("created", "s3:ObjectCreated:*", None, None)],
    );
    put(&engine, "other", "in/late.jpg");
    put(&engine, "photos", "in/old.jpg");
    for key in ["in/2.txt", "in/4.jpg", "in/old.jpg", "in/before.jpg"] {
        delete(&engine, "photos", key);
    }
    engine
        .object(|file| {
            file.transaction(|tx| {
                tx.delete_bucket("photos")?.expect("dropped");
                tx.create_bucket("photos", truthdb_object::DEFAULT_ACCOUNT, 1_000)?
                    .expect("made again");
                Ok(())
            })
        })
        .unwrap();
    // Made again, the bucket is not configured until it is.
    put(&engine, "photos", "in/between.jpg");
    delete(&engine, "photos", "in/between.jpg");
    configure(
        &engine,
        vec![rule("created", "s3:ObjectCreated:*", Some("in/"), None)],
    );
    put(&engine, "photos", "in/new.jpg");
    delete(&engine, "photos", "in/new.jpg");
    drain(&engine);
    let mut sent = events(&engine);
    sent.sort_by(|a, b| a.3.cmp(&b.3));
    let names: Vec<(String, String)> = sent
        .into_iter()
        .map(|(_, name, key, _)| (name, key))
        .collect();
    let event = |name: &str, key: &str| (name.to_string(), key.to_string());
    assert_eq!(
        names,
        [
            event("ObjectCreated:Put", "in%2Flate.jpg"),
            event("ObjectCreated:Put", "in%2Fold.jpg"),
            event("ObjectRemoved:Delete", "in%2F4.jpg"),
            event("ObjectRemoved:Delete", "in%2Fold.jpg"),
            event("ObjectRemoved:Delete", "in%2Fbefore.jpg"),
            event("ObjectCreated:Put", "in%2Fnew.jpg"),
        ]
    );

    // A topic the bucket's owner does not own gets none of its events.
    engine.execute("stream drop topic events").unwrap();
    engine
        .execute("stream create topic events partitions 2 retention 0")
        .unwrap();
    put(&engine, "photos", "in/foreign.jpg");
    drain(&engine);
    assert!(events(&engine).is_empty());

    // No configurations: no events.
    engine.execute("stream drop topic events").unwrap();
    engine
        .execute("stream create topic events partitions 2 retention 0 account 000000000000")
        .unwrap();
    configure(&engine, Vec::new());
    put(&engine, "photos", "in/5.jpg");
    drain(&engine);
    assert!(events(&engine).is_empty());
    assert!(
        engine
            .object(|file| file.notification("photos"))
            .unwrap()
            .is_none()
    );
    drop(engine);
    let _ = std::fs::remove_file(path);
}
