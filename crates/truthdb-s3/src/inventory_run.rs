//! S3 Inventory reports: a task beside the S3 listener that makes each
//! enabled configuration's report when it is due (see [`due`]), one at a
//! time. A run lists the source bucket a page at a time, each page under
//! one storage-lock hold, into a gzipped CSV body; then one transaction
//! writes the list, its `manifest.json`, `manifest.checksum` and Hive
//! `symlink.txt` to the destination and records the run, once it has checked
//! that the source bucket and the configuration are still the ones listed
//! and that the destination is still the source owner's. A run cut short (a
//! crash, a shutdown) leaves nothing behind but a staging body, which
//! garbage collection reclaims, and starts again from the beginning. A
//! standby runs none (its log is the primary's).

use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use tokio::sync::watch;
use truthdb_core::storage::{Storage, StorageError};
use truthdb_object::{
    BUCKET_INVENTORY, BodyWriter, InventoryConfig, InventoryFormat, InventoryFrequency, ListAfter,
    ListedObject, ObjFile, ObjectAttrs, ObjectLock, Origin, RetentionMode, SealedBody,
    StorageClass, Versioning, time::iso8601,
};

use crate::batch_run::{REPORT_KEY, object};
use crate::objects::{DROP_GRACE_MS, now_ms};
use crate::server::S3State;

/// How long the runner waits when no report is due, and after a run failed
/// (a storage error).
const IDLE: Duration = Duration::from_secs(1);
const RETRY: Duration = Duration::from_secs(60);
/// Versions listed a page (one lock hold).
const PAGE: usize = 1000;
const DAY_MS: u64 = 86_400_000;

/// The optional columns, in the order S3 writes them (after Bucket, Key,
/// and with every version VersionId, IsLatest, IsDeleteMarker). The event
/// hold's duration brings its unit's column after it.
const OPTIONAL_COLUMNS: [&str; 19] = [
    "Size",
    "LastModifiedDate",
    "ETag",
    "StorageClass",
    "IsMultipartUploaded",
    "ReplicationStatus",
    "EncryptionStatus",
    "ObjectLockRetainUntilDate",
    "ObjectLockMode",
    "ObjectLockLegalHoldStatus",
    "ObjectLockEventHoldStatus",
    "ObjectLockEventHoldDuration",
    "IntelligentTieringAccessTier",
    "BucketKeyStatus",
    "ChecksumAlgorithm",
    "ObjectAccessControlList",
    "ObjectOwner",
    "LifecycleExpirationDate",
    "IntelligentTieringReferenceDate",
];

/// Makes reports until `shutdown` turns true.
pub async fn run(state: Arc<S3State>, mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let wait = if state.storage.is_standby() {
            IDLE
        } else {
            match step(&state, &shutdown).await {
                Ok(true) => continue,
                Ok(false) => IDLE,
                Err(err) => {
                    eprintln!("inventory reports: {err}");
                    RETRY
                }
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.changed() => {}
        }
    }
}

/// Whether a report last run at `last_ms` (0: never) is due at `now_ms`:
/// the first at once, then a daily one each UTC day, a weekly one each
/// Sunday (as S3 makes them).
fn due(frequency: InventoryFrequency, last_ms: u64, now_ms: u64) -> bool {
    if last_ms == 0 {
        return true;
    }
    let (last_day, today) = (last_ms / DAY_MS, now_ms / DAY_MS);
    match frequency {
        InventoryFrequency::Daily => last_day < today,
        // Day 0 (1970-01-01) was a Thursday: Sundays are the days d with
        // (d + 4) % 7 == 0.
        InventoryFrequency::Weekly => last_day < today - (today + 4) % 7,
    }
}

/// A report to make: its configuration (and the stored document, to tell
/// it unchanged at the end), and the source bucket as it was when the run
/// began.
struct Due {
    bucket: String,
    document: Vec<u8>,
    config: InventoryConfig,
    owner: String,
    created_ms: u64,
    versioning: Versioning,
}

/// Makes the first due report; whether there was one.
async fn step(state: &S3State, shutdown: &watch::Receiver<bool>) -> Result<bool, String> {
    let now = now_ms();
    let next = object(state, move |file| {
        for (bucket, id, last) in file.inventory_runs()? {
            let Some(document) = file.bucket_config(&bucket, BUCKET_INVENTORY, &id)? else {
                continue;
            };
            let Some(config) = InventoryConfig::decode(&document) else {
                continue;
            };
            // Only CSV reports are made.
            if !config.enabled
                || config.format != InventoryFormat::Csv
                || !due(config.frequency, last, now)
            {
                continue;
            }
            let Some(info) = file.bucket(&bucket)? else {
                continue;
            };
            return Ok(Some(Due {
                bucket,
                document,
                config,
                owner: info.owner,
                created_ms: info.created_ms,
                versioning: info.versioning,
            }));
        }
        Ok(None)
    })
    .await?;
    let Some(due) = next else {
        return Ok(false);
    };
    make_report(state, due, now, shutdown).await?;
    Ok(true)
}

/// The list being written: gzip into a body.
struct List {
    encoder: flate2::write::GzEncoder<Vec<u8>>,
    writer: BodyWriter,
    /// Compressed bytes written so far.
    size: u64,
}

impl List {
    /// Adds `text`, writing what the encoder has compressed so far.
    fn add(&mut self, storage: &Storage, text: &[u8]) -> Result<(), String> {
        self.encoder
            .write_all(text)
            .map_err(|err| err.to_string())?;
        let out = std::mem::take(self.encoder.get_mut());
        self.size += out.len() as u64;
        // The body's time is now (not the run's start): a body untouched
        // for long is taken for abandoned.
        self.writer
            .write(storage, &out, now_ms())
            .map_err(|err| err.to_string())
    }

    fn finish(mut self, storage: &Storage) -> Result<(SealedBody, u64), String> {
        let out = self.encoder.finish().map_err(|err| err.to_string())?;
        self.size += out.len() as u64;
        self.writer
            .write(storage, &out, now_ms())
            .map_err(|err| err.to_string())?;
        let sealed = self
            .writer
            .finish(storage, now_ms())
            .map_err(|err| err.to_string())?;
        Ok((sealed, self.size))
    }
}

/// Runs `op` on the list without holding up the runtime.
async fn on_list<T: Send + 'static>(
    state: &S3State,
    list: List,
    op: impl FnOnce(&Storage, List) -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    let storage = Arc::clone(&state.storage);
    tokio::task::spawn_blocking(move || op(&storage, list))
        .await
        .map_err(|err| err.to_string())?
}

/// Lists the source bucket into a report and delivers it. Returns early,
/// recording nothing, when the source bucket goes or the server shuts down.
async fn make_report(
    state: &S3State,
    due: Due,
    started: u64,
    shutdown: &watch::Receiver<bool>,
) -> Result<(), String> {
    let Due {
        bucket,
        document,
        config,
        owner,
        created_ms,
        versioning,
    } = due;
    let storage = Arc::clone(&state.storage);
    let mut list = tokio::task::spawn_blocking(move || {
        BodyWriter::begin(&storage, now_ms()).map(|writer| List {
            encoder: flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default()),
            writer,
            size: 0,
        })
    })
    .await
    .map_err(|err| err.to_string())?
    .map_err(|err| err.to_string())?;
    let encrypted = state.storage.is_encrypted();
    let row = Arc::new(Row::new(&config, &bucket, &owner, versioning, encrypted));
    let mut after: Option<(String, Option<u64>)> = None;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let (name, prefix, all, row) = (
            bucket.clone(),
            config.prefix.clone().unwrap_or_default(),
            config.all_versions,
            Arc::clone(&row),
        );
        let resume = after.clone();
        let page = object(state, move |file| {
            if file
                .bucket(&name)?
                .is_none_or(|info| info.created_ms != created_ms)
            {
                return Ok(None);
            }
            let after = resume.as_ref().map(|(key, version)| match version {
                Some(version) => ListAfter::Version(key, *version),
                None => ListAfter::Key(key),
            });
            let listing = match all {
                true => file.list_versions(&name, &prefix, None, after, PAGE)?,
                false => file.list_objects(&name, &prefix, None, after, PAGE)?,
            };
            let Ok(listing) = listing else {
                return Ok(None);
            };
            let mut text = String::new();
            for object in &listing.objects {
                if !all && (!object.latest || object.info.delete_marker) {
                    continue;
                }
                let lock = match row.locks {
                    true => file.object_lock(object.info.version)?,
                    false => ObjectLock::default(),
                };
                text.push_str(&row.line(object, &lock, started));
            }
            let next = listing.last.map(|(key, _)| (key, listing.last_version));
            Ok(Some((text, next, listing.truncated)))
        })
        .await?;
        let Some((text, next, truncated)) = page else {
            return Ok(());
        };
        list = on_list(state, list, move |storage, mut list| {
            list.add(storage, text.as_bytes())?;
            Ok(list)
        })
        .await?;
        if !truncated || next.is_none() {
            break;
        }
        after = next;
    }
    let (data, size) = on_list(state, list, |storage, list| list.finish(storage)).await?;
    let due = Due {
        bucket,
        document,
        config,
        owner,
        created_ms,
        versioning,
    };
    let files = report_files(state, &due, started, data, size).await?;
    let delivery = object(state, move |file| {
        deliver_in(file, &due, started, &files, now_ms())
    })
    .await?;
    if let Delivery::Refused(problem) = delivery {
        // Made again when next due.
        eprintln!("inventory reports: no report for {problem}");
    }
    Ok(())
}

/// A stored body of `bytes`.
async fn small_body(state: &S3State, bytes: Vec<u8>) -> Result<SealedBody, String> {
    let storage = Arc::clone(&state.storage);
    tokio::task::spawn_blocking(move || {
        let now = now_ms();
        let mut writer = BodyWriter::begin(&storage, now)?;
        writer.write(&storage, &bytes, now)?;
        writer.finish(&storage, now)
    })
    .await
    .map_err(|err| err.to_string())?
    .map_err(|err| err.to_string())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A report's files: its key, body and content type each.
type Files = Vec<(String, SealedBody, &'static str)>;

/// The report's files for a run started at `started`: the list (`data`,
/// `size` bytes), its manifest, checksum and Hive symlink, keyed as S3 keys
/// them under the destination's prefix.
async fn report_files(
    state: &S3State,
    due: &Due,
    started: u64,
    data: SealedBody,
    size: u64,
) -> Result<Files, String> {
    let (bucket, config) = (&due.bucket, &due.config);
    let base = match config
        .destination_prefix
        .as_deref()
        .unwrap_or("")
        .trim_end_matches('/')
    {
        "" => format!("{bucket}/{}", config.id),
        prefix => format!("{prefix}/{bucket}/{}", config.id),
    };
    let name = hex(&ring::digest::digest(
        &ring::digest::SHA256,
        format!("{bucket}/{}/{started}", config.id).as_bytes(),
    )
    .as_ref()[..16]);
    let data_key = format!("{base}/data/{name}.csv.gz");
    let stamp = iso8601(started);
    let (date, hour, minute) = (&stamp[..10], &stamp[11..13], &stamp[14..16]);
    let manifest = serde_json::json!({
        "sourceBucket": bucket,
        "destinationBucket": format!("arn:aws:s3:::{}", config.destination),
        "version": "2016-11-30",
        "creationTimestamp": started.to_string(),
        "fileFormat": "CSV",
        "fileSchema": schema(config),
        "files": [{"key": data_key, "size": size, "MD5checksum": hex(&data.md5)}],
    })
    .to_string();
    let checksum = {
        use md5::Digest as _;
        hex(&md5::Md5::digest(manifest.as_bytes()))
    };
    let symlink = format!("s3://{}/{data_key}\n", config.destination);
    Ok(vec![
        (data_key, data, "application/x-gzip"),
        (
            format!("{base}/{date}T{hour}-{minute}Z/manifest.json"),
            small_body(state, manifest.into_bytes()).await?,
            "application/json",
        ),
        (
            format!("{base}/{date}T{hour}-{minute}Z/manifest.checksum"),
            small_body(state, checksum.into_bytes()).await?,
            "text/plain",
        ),
        (
            format!("{base}/hive/dt={date}-{hour}-{minute}/symlink.txt"),
            small_body(state, symlink.into_bytes()).await?,
            "text/plain",
        ),
    ])
}

/// What became of a run's report.
#[derive(Debug, PartialEq, Eq)]
enum Delivery {
    /// Its files were written and the run recorded.
    Written,
    /// The source bucket or the configuration changed meanwhile: nothing
    /// was written or recorded (a changed configuration runs anew).
    Abandoned,
    /// The destination is gone or no longer the source owner's, or a file
    /// cannot be written there: the run is recorded without its files (made
    /// again when next due), and why.
    Refused(String),
}

/// Writes a report's `files` and records the run started at `started`, in
/// one transaction, once the source bucket and the configuration are
/// checked to be the ones listed and the destination the source owner's.
fn deliver_in(
    file: &mut ObjFile<'_>,
    due: &Due,
    started: u64,
    files: &Files,
    now: u64,
) -> Result<Delivery, StorageError> {
    let (bucket, id) = (&due.bucket, &due.config.id);
    if file
        .bucket(bucket)?
        .is_none_or(|info| info.created_ms != due.created_ms)
        || file.bucket_config(bucket, BUCKET_INVENTORY, id)?.as_ref() != Some(&due.document)
    {
        return Ok(Delivery::Abandoned);
    }
    let destination = &due.config.destination;
    let refused = match file.bucket(destination)? {
        None => Some("its destination bucket does not exist"),
        Some(target) if target.owner != due.owner => {
            Some("its destination bucket is not the source bucket owner's")
        }
        Some(_) => None,
    };
    let (bucket_lock, default) = crate::object_lock::bucket_state(file, destination)?;
    let lock = crate::object_lock::new_object_lock(None, bucket_lock, default, now)
        .ok()
        .flatten();
    file.set_origin(Origin {
        operation: "PutObject".to_string(),
        request_id: format!("inventory-{started}"),
        account: Some(due.owner.clone()),
        ..Origin::default()
    });
    let problem = match refused {
        Some(problem) => problem,
        None => match file.transaction(|tx| {
            for (key, body, content_type) in files {
                let attrs = ObjectAttrs {
                    content_type: Some(content_type.to_string()),
                    ..ObjectAttrs::default()
                };
                match tx.put_object(
                    destination,
                    key,
                    body,
                    &attrs,
                    StorageClass::Standard,
                    now,
                    DROP_GRACE_MS,
                )? {
                    Ok(info) => {
                        if let Some(lock) = &lock {
                            tx.set_lock(info.version, lock)?;
                        }
                    }
                    // The files are written all or none: the transaction
                    // rolls back.
                    Err(_) => return Err(StorageError::InvalidConfig(UNWRITTEN.to_string())),
                }
            }
            tx.inventory_ran(bucket, id, started)
        }) {
            Ok(_) => return Ok(Delivery::Written),
            Err(StorageError::InvalidConfig(message)) if message == UNWRITTEN => {
                "its files could not be written"
            }
            Err(err) => return Err(err),
        },
    };
    file.transaction(|tx| tx.inventory_ran(bucket, id, started))?;
    Ok(Delivery::Refused(format!("{bucket}/{id}: {problem}")))
}

/// What a delivery's transaction fails with when a file cannot be written:
/// it rolls back, and the run is recorded without its files.
const UNWRITTEN: &str = "an inventory file cannot be written";

/// The report's columns (its manifest's fileSchema).
fn schema(config: &InventoryConfig) -> String {
    let mut columns = vec!["Bucket", "Key"];
    if config.all_versions {
        columns.extend(["VersionId", "IsLatest", "IsDeleteMarker"]);
    }
    for column in OPTIONAL_COLUMNS {
        if config.optional_fields.iter().any(|field| field == column) {
            columns.push(column);
            if column == "ObjectLockEventHoldDuration" {
                columns.push("ObjectLockEventHoldDurationUnit");
            }
        }
    }
    columns.join(", ")
}

/// How a configuration's rows are written.
struct Row {
    bucket: String,
    all_versions: bool,
    versioned: bool,
    columns: Vec<&'static str>,
    /// Whether any Object Lock column is asked for (each row then reads its
    /// version's lock).
    locks: bool,
    encrypted: bool,
    /// The owner's canonical id, and its full control as an ACL document
    /// (base64), as S3 writes them.
    owner_id: String,
    acl: String,
}

impl Row {
    fn new(
        config: &InventoryConfig,
        bucket: &str,
        owner: &str,
        versioning: Versioning,
        encrypted: bool,
    ) -> Row {
        let columns: Vec<&'static str> = OPTIONAL_COLUMNS
            .into_iter()
            .filter(|column| config.optional_fields.iter().any(|field| field == column))
            .collect();
        let owner_id = truthdb_sigv4::sha256_hex(owner.as_bytes());
        // In S3's order of its members.
        let acl = format!(
            "{{\"version\":\"2022-11-10\",\"status\":\"AVAILABLE\",\"grants\":[{{\"canonicalId\":\"{owner_id}\",\"type\":\"CanonicalUser\",\"permission\":\"FULL_CONTROL\"}}]}}"
        );
        Row {
            bucket: bucket.to_string(),
            all_versions: config.all_versions,
            versioned: versioning != Versioning::Unversioned,
            locks: columns
                .iter()
                .any(|column| column.starts_with("ObjectLock")),
            columns,
            encrypted,
            owner_id,
            acl: base64::engine::general_purpose::STANDARD.encode(acl),
        }
    }

    /// `object`'s line, every value quoted, at `now_ms` (an event hold's
    /// retain-until date moves with it).
    fn line(&self, object: &ListedObject, lock: &ObjectLock, now_ms: u64) -> String {
        let info = &object.info;
        let marker = info.delete_marker;
        let flag = |on: bool| if on { "True" } else { "False" }.to_string();
        let mut values = vec![
            self.bucket.clone(),
            percent_encoding::utf8_percent_encode(&object.key, REPORT_KEY).to_string(),
        ];
        if self.all_versions {
            // A null version's id is empty (as S3 writes it).
            values.push(match self.versioned && !info.null_version {
                true => info.version.to_string(),
                false => String::new(),
            });
            values.push(flag(object.latest));
            values.push(flag(marker));
        }
        let retention = lock.retention;
        let event = retention.and_then(|retention| retention.event);
        for column in &self.columns {
            match *column {
                "Size" if !marker => values.push(info.size.to_string()),
                "LastModifiedDate" => values.push(iso8601(info.mtime_ms)),
                "ETag" if !marker => values.push(info.etag().trim_matches('"').to_string()),
                "StorageClass" if !marker => values.push(info.storage_class.as_str().to_string()),
                "IsMultipartUploaded" if !marker => values.push(flag(info.parts > 0)),
                "EncryptionStatus" if !marker => values.push(
                    match (info.attrs.customer_key.is_some(), self.encrypted) {
                        (true, _) => "SSE-C",
                        (false, true) => "SSE-S3",
                        (false, false) => "NOT-SSE",
                    }
                    .to_string(),
                ),
                "ObjectLockRetainUntilDate" => values.push(
                    retention
                        .map(|retention| iso8601(retention.effective_until(now_ms)))
                        .unwrap_or_default(),
                ),
                "ObjectLockMode" => values.push(
                    match retention.map(|retention| retention.mode) {
                        Some(RetentionMode::Governance) => "GOVERNANCE",
                        Some(RetentionMode::Compliance) => "COMPLIANCE",
                        None => "",
                    }
                    .to_string(),
                ),
                "ObjectLockLegalHoldStatus" => values.push(
                    match lock.legal_hold == Some(true) {
                        true => "ON",
                        false => "OFF",
                    }
                    .to_string(),
                ),
                "ObjectLockEventHoldStatus" => values.push(
                    event
                        .map(|event| if event.on { "ON" } else { "OFF" }.to_string())
                        .unwrap_or_default(),
                ),
                "ObjectLockEventHoldDuration" => {
                    values.push(
                        event
                            .map(|event| event.count.to_string())
                            .unwrap_or_default(),
                    );
                    values.push(
                        event
                            .map(|event| if event.years { "YEARS" } else { "DAYS" }.to_string())
                            .unwrap_or_default(),
                    );
                }
                "ObjectAccessControlList" if !marker => values.push(self.acl.clone()),
                "ObjectOwner" => values.push(self.owner_id.clone()),
                // Nothing kept fills the others (and a delete marker has no
                // size, ETag, class or encryption).
                _ => values.push(String::new()),
            }
        }
        let mut line = values
            .iter()
            // No value holds a quote (a key is URL-encoded).
            .map(|value| format!("\"{value}\""))
            .collect::<Vec<_>>()
            .join(",");
        line.push('\n');
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use truthdb_core::storage::StorageOptions;
    use truthdb_object::VersionSel;

    fn create(path: &std::path::Path) -> Storage {
        const RING: u64 = 8 * 1024 * 1024;
        Storage::create_with_wal_bounds(
            path.to_path_buf(),
            StorageOptions {
                size_bytes: 64 << 20,
                wal_ratio: 0.05,
                metadata_ratio: 0.08,
                snapshot_ratio: 0.02,
                allocator_ratio: 0.02,
                reserved_ratio: 0.17,
                default_collation: None,
            },
            RING,
            RING,
            truthdb_engine::database_engines,
        )
        .expect("create")
    }

    /// A report's files: `keys`, each with a body of its own.
    fn files(storage: &Storage, keys: &[String]) -> Files {
        keys.iter()
            .map(|key| {
                let mut writer = BodyWriter::begin(storage, 1).unwrap();
                writer.write(storage, key.as_bytes(), 1).unwrap();
                (
                    key.clone(),
                    writer.finish(storage, 1).unwrap(),
                    "text/plain",
                )
            })
            .collect()
    }

    #[test]
    fn a_report_is_delivered_only_while_its_source_and_destination_hold() {
        let path = std::env::temp_dir().join(format!(
            "truthdb-s3-inventory-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let storage = create(&path);
        let (me, them) = ("111111111111", "222222222222");
        let config = InventoryConfig {
            id: "daily".into(),
            enabled: true,
            prefix: None,
            destination: "lists".into(),
            destination_account: None,
            destination_prefix: None,
            format: InventoryFormat::Csv,
            sse_s3: false,
            all_versions: false,
            optional_fields: Vec::new(),
            frequency: InventoryFrequency::Daily,
        };
        let document = config.encode();
        {
            let mut lock = storage.lock();
            let mut file = ObjFile::new(&mut lock);
            file.transaction(|tx| {
                tx.create_bucket("source", me, 5)?.unwrap();
                tx.create_bucket("lists", me, 5)?.unwrap();
                tx.put_bucket_config("source", BUCKET_INVENTORY, "daily", &document)?;
                tx.track_inventory("source", "daily")
            })
            .unwrap();
        }
        let due = Due {
            bucket: "source".into(),
            document: document.clone(),
            config: config.clone(),
            owner: me.into(),
            created_ms: 5,
            versioning: Versioning::Unversioned,
        };
        let deliver = |started: u64, keys: &[String]| {
            let files = files(&storage, keys);
            deliver_in(
                &mut ObjFile::new(&mut storage.lock()),
                &due,
                started,
                &files,
                7,
            )
            .unwrap()
        };
        let last = || ObjFile::new(&mut storage.lock()).inventory_runs().unwrap()[0].2;
        let exists = |key: &str| {
            ObjFile::new(&mut storage.lock())
                .head_object("lists", key, VersionSel::Latest)
                .unwrap()
                .is_ok()
        };
        let key = |name: &str| name.to_string();

        assert_eq!(deliver(100, &[key("a"), key("b")]), Delivery::Written);
        assert!(exists("a") && exists("b"));
        assert_eq!(last(), 100);

        // A file that cannot be written: none is, and the run is recorded.
        let refused = deliver(150, &[key("c"), "k".repeat(2000)]);
        assert!(matches!(&refused, Delivery::Refused(why) if why.contains("could not be written")));
        assert!(!exists("c"));
        assert_eq!(last(), 150);

        // The destination deleted, and taken by another account.
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                for key in ["a", "b"] {
                    tx.delete_object("lists", key, None, false, 6, 0)?.unwrap();
                }
                tx.delete_bucket("lists")?.unwrap();
                tx.create_bucket("lists", them, 6)?.unwrap();
                Ok(())
            })
            .unwrap();
        let refused = deliver(200, &[key("d")]);
        assert!(
            matches!(&refused, Delivery::Refused(why) if why.contains("not the source bucket owner"))
        );
        assert!(!exists("d"));
        assert_eq!(last(), 200);

        // The configuration changed: abandoned, nothing recorded.
        let mut changed = config.clone();
        changed.prefix = Some("docs/".into());
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.put_bucket_config("source", BUCKET_INVENTORY, "daily", &changed.encode())
            })
            .unwrap();
        assert_eq!(deliver(300, &[key("e")]), Delivery::Abandoned);
        assert_eq!(last(), 200);

        // The source bucket deleted and created again with the same
        // configuration: abandoned too.
        ObjFile::new(&mut storage.lock())
            .transaction(|tx| {
                tx.delete_bucket("source")?.unwrap();
                tx.create_bucket("source", me, 8)?.unwrap();
                tx.put_bucket_config("source", BUCKET_INVENTORY, "daily", &document)?;
                tx.track_inventory("source", "daily")
            })
            .unwrap();
        assert_eq!(deliver(400, &[key("f")]), Delivery::Abandoned);
        assert_eq!(last(), 0);
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }

    const SUNDAY: u64 = 20_415 * DAY_MS; // 2025-11-23, a Sunday

    #[test]
    fn reports_are_due_daily_or_on_sundays_and_the_first_at_once() {
        use InventoryFrequency::{Daily, Weekly};
        assert!(due(Daily, 0, 5));
        assert!(due(Weekly, 0, 5));
        let saturday = SUNDAY - DAY_MS;
        // Daily: once each UTC day.
        assert!(!due(Daily, saturday + 1, saturday + DAY_MS - 1));
        assert!(due(Daily, saturday + DAY_MS - 1, SUNDAY));
        // Weekly: a first report on Saturday is followed by one on Sunday,
        // then none until the next Sunday.
        assert!(!due(Weekly, saturday + 1, saturday + DAY_MS - 1));
        assert!(due(Weekly, saturday + 1, SUNDAY));
        assert!(!due(Weekly, SUNDAY + 5, SUNDAY + 6 * DAY_MS + DAY_MS - 1));
        assert!(due(Weekly, SUNDAY + 5, SUNDAY + 7 * DAY_MS));
        assert!(due(Weekly, SUNDAY - 8 * DAY_MS, SUNDAY + 3 * DAY_MS));
    }

    #[test]
    fn a_line_holds_each_column_as_s3_writes_it() {
        use truthdb_object::{CustomerKey, EventHold, ObjectInfo, Retention};
        let config = InventoryConfig {
            id: "r".into(),
            enabled: true,
            prefix: None,
            destination: "d".into(),
            destination_account: None,
            destination_prefix: None,
            format: InventoryFormat::Csv,
            sse_s3: false,
            all_versions: true,
            optional_fields: OPTIONAL_COLUMNS
                .iter()
                .map(|field| field.to_string())
                .collect(),
            frequency: InventoryFrequency::Daily,
        };
        let object = |delete_marker: bool, null_version: bool| ListedObject {
            key: "a b/\"c\".txt".into(),
            info: ObjectInfo {
                version: 7,
                null_version,
                delete_marker,
                body: 1,
                size: 12,
                md5: [0xab; 16],
                parts: 3,
                mtime_ms: 1_000,
                storage_class: StorageClass::StandardIa,
                attrs: ObjectAttrs::default(),
            },
            latest: !null_version,
        };
        let lock = ObjectLock {
            retention: Some(Retention {
                mode: RetentionMode::Compliance,
                until_ms: 2_000,
                event: Some(EventHold {
                    on: true,
                    years: false,
                    count: 2,
                }),
            }),
            legal_hold: Some(true),
        };
        let row = Row::new(&config, "src", "111111111111", Versioning::Enabled, true);
        let owner = truthdb_sigv4::sha256_hex(b"111111111111");
        let acl = base64::engine::general_purpose::STANDARD.encode(format!(
            "{{\"version\":\"2022-11-10\",\"status\":\"AVAILABLE\",\"grants\":[{{\"canonicalId\":\"{owner}\",\"type\":\"CanonicalUser\",\"permission\":\"FULL_CONTROL\"}}]}}"
        ));
        let fields = |line: String| -> Vec<String> {
            line.trim_end()
                .split(',')
                .map(|field| field.trim_matches('"').to_string())
                .collect()
        };
        // An event hold on: retain-until moves with the report's time.
        let until = iso8601(10 * DAY_MS + 2 * DAY_MS);
        assert_eq!(
            fields(row.line(&object(false, false), &lock, 10 * DAY_MS)),
            [
                "src",
                "a%20b/%22c%22.txt",
                "7",
                "True",
                "False",
                "12",
                "1970-01-01T00:00:01.000Z",
                &format!("{}-3", "ab".repeat(16)),
                "STANDARD_IA",
                "True",
                "",
                "SSE-S3",
                &until,
                "COMPLIANCE",
                "ON",
                "ON",
                "2",
                "DAYS",
                "",
                "",
                "",
                &acl,
                &owner,
                "",
                "",
            ]
            .map(str::to_string)
        );
        // A null version's id is empty; a delete marker has no size, ETag,
        // class, upload flag, encryption or ACL; no lock reads OFF.
        let mut keyed = object(true, true);
        keyed.info.attrs.customer_key = Some(CustomerKey {
            key_md5: String::new(),
            verifier: String::new(),
        });
        let none = ObjectLock::default();
        let line = fields(row.line(&keyed, &none, 0));
        assert_eq!(
            &line[2..12],
            [
                "",
                "False",
                "True",
                "",
                "1970-01-01T00:00:01.000Z",
                "",
                "",
                "",
                "",
                ""
            ]
        );
        assert_eq!(&line[12..18], ["", "", "OFF", "", "", ""]);
        keyed.info.delete_marker = false;
        assert_eq!(fields(row.line(&keyed, &none, 0))[11], "SSE-C");
        let plain = Row::new(&config, "src", "111111111111", Versioning::Enabled, false);
        assert_eq!(
            fields(plain.line(&object(false, false), &none, 0))[11],
            "NOT-SSE"
        );
    }

    #[test]
    fn a_reports_schema_lists_its_columns_in_s3s_order() {
        let config = InventoryConfig {
            id: "r".into(),
            enabled: true,
            prefix: None,
            destination: "d".into(),
            destination_account: None,
            destination_prefix: None,
            format: InventoryFormat::Csv,
            sse_s3: false,
            all_versions: true,
            optional_fields: vec![
                "ObjectOwner".into(),
                "ObjectLockEventHoldDuration".into(),
                "Size".into(),
            ],
            frequency: InventoryFrequency::Daily,
        };
        assert_eq!(
            schema(&config),
            "Bucket, Key, VersionId, IsLatest, IsDeleteMarker, Size, \
             ObjectLockEventHoldDuration, ObjectLockEventHoldDurationUnit, ObjectOwner"
        );
    }
}
