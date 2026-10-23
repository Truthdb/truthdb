//! Imports from another S3 service (`s3 create import`), run on a thread of
//! their own (see `session::pool`): each step takes the running import that
//! waited longest a little further. It lists the source after the import's
//! place; an object the destination already holds with the same size and
//! ETag is skipped; any other is read from the source and written as a new
//! object, its place moving in the same transaction (see
//! `truthdb_object::Import`).
//!
//! An object the source refuses (gone, forbidden) is recorded and passed
//! over. A source that refuses the import itself (its keys, its bucket, a
//! redirect), or that lists out of order, fails it. A source that cannot be
//! reached or answers 5xx, while listing or while sending an object, is
//! tried again later from the same place. A storage error ends the step
//! without moving on. A standby imports nothing.

use truthdb_object::{
    BodyWriter, Import, ImportState, ObjectAttrs, ObjectError, Origin, StorageClass, VersionSel,
};
use truthdb_s3client::{Client, Config, Error as S3Error, Listed};

use super::{Engine, EngineError};

/// Source objects one step looks at, so the thread checks for shutdown and
/// for a dropped import often enough.
const STEP_OBJECTS: usize = 100;

/// How long a dropped object's body outlives a version an import replaced.
const IMPORT_DROP_GRACE_MS: u64 = 15 * 60 * 1000;

/// What became of one source object.
enum Outcome {
    /// Copied: the import as it now stands.
    Copied(Box<Import>),
    Skipped,
    /// The source refused this object: recorded and passed over.
    Failed(String),
    /// The source could not be reached: the same object, later.
    Retry(String),
    /// The import cannot go on (its destination is gone or has Object
    /// Lock).
    Fatal(String),
    /// The import was dropped meanwhile.
    Dropped,
    /// The server is stopping.
    Stopped,
}

/// Whether a source error will not clear by waiting: anything but a
/// transport failure or a 5xx answer.
fn permanent(err: &S3Error) -> bool {
    match err {
        S3Error::Invalid(_) => true,
        S3Error::Service { status, .. } => *status < 500,
        S3Error::Transport(_) => false,
    }
}

impl Engine {
    /// One step of the running import that waited longest; returns whether
    /// more steps are wanted at once. `stopping` says when the server is
    /// shutting down.
    pub(crate) fn import_step(
        &self,
        runtime: &tokio::runtime::Runtime,
        stopping: &dyn Fn() -> bool,
    ) -> Result<bool, EngineError> {
        if self.storage.is_standby() || !self.runs_objects() {
            return Ok(false);
        }
        // Turn by turn: one that keeps waiting for its source does not hold
        // the others back.
        let Some(mut import) = self
            .object(|file| file.imports())?
            .into_iter()
            .filter(|import| import.state == ImportState::Running)
            .min_by_key(|import| import.updated_ms)
        else {
            return Ok(false);
        };
        let client = match Client::new(Config {
            endpoint: import.source.endpoint.clone(),
            region: import.source.region.clone(),
            access_key: import.source.access_key.clone(),
            secret: import.source.secret.clone(),
            ca_pem: None,
        }) {
            Ok(client) => client,
            Err(err) => return self.end_import(import, ImportState::Failed, err.to_string()),
        };
        let page = match runtime.block_on(client.list_objects_after(
            &import.source.bucket,
            &import.source.prefix,
            &import.after,
        )) {
            Ok(page) => page,
            Err(err) if permanent(&err) => {
                return self.end_import(import, ImportState::Failed, err.to_string());
            }
            Err(err) => return self.wait_import(import, err.to_string()),
        };
        if page.objects.is_empty() && page.next.is_some() {
            return self.end_import(
                import,
                ImportState::Failed,
                "the source listed an empty page with more to come".to_string(),
            );
        }
        let last_page = page.next.is_none() && page.objects.len() <= STEP_OBJECTS;
        for object in page.objects.iter().take(STEP_OBJECTS) {
            // Keys come in order, after the place: else the import would
            // never end (or count objects twice).
            if !import.after.is_empty() && object.key <= import.after {
                let error = format!("the source listed {} out of order", object.key);
                return self.end_import(import, ImportState::Failed, error);
            }
            match self.import_object(runtime, &client, &import, object, stopping)? {
                Outcome::Copied(copied) => import = *copied,
                Outcome::Dropped => return Ok(true),
                Outcome::Fatal(error) => {
                    return self.end_import(import, ImportState::Failed, error);
                }
                Outcome::Retry(error) => return self.wait_import(import, error),
                Outcome::Stopped => {
                    import.updated_ms = super::now_ms();
                    self.save_import(&import)?;
                    return Ok(false);
                }
                // Saved with the next write (a crash only skips it again).
                Outcome::Skipped => {
                    import.skipped += 1;
                    import.after = object.key.clone();
                }
                Outcome::Failed(error) => {
                    import.failed += 1;
                    import.after = object.key.clone();
                    import.error = error.clone();
                    import.updated_ms = super::now_ms();
                    let (key, saved) = (object.key.clone(), import.clone());
                    let kept = self.object(|file| {
                        file.transaction(|tx| {
                            if tx.import(saved.id)?.is_none() {
                                return Ok(false);
                            }
                            tx.record_import_failure(saved.id, &key, &error)?;
                            tx.update_import(&saved)?;
                            Ok(true)
                        })
                    })?;
                    if !kept {
                        return Ok(true);
                    }
                }
            }
        }
        if last_page {
            return self.end_import(import, ImportState::Done, String::new());
        }
        import.updated_ms = super::now_ms();
        self.save_import(&import)?;
        Ok(true)
    }

    /// Copies one source object, unless the destination holds it already;
    /// a copy is written together with `import` moved past it.
    fn import_object(
        &self,
        runtime: &tokio::runtime::Runtime,
        client: &Client,
        import: &Import,
        object: &Listed,
        stopping: &dyn Fn() -> bool,
    ) -> Result<Outcome, EngineError> {
        let Some(relative) = object.key.strip_prefix(&import.source.prefix) else {
            return Ok(Outcome::Failed(format!(
                "the source listed {} outside its prefix",
                object.key
            )));
        };
        let key = format!("{}{relative}", import.prefix);
        let (bucket, lookup) = (import.bucket.clone(), key.clone());
        let held =
            self.object(move |file| file.head_object(&bucket, &lookup, VersionSel::Latest))?;
        match held {
            Ok(info) if info.size == object.size && info.etag() == object.etag => {
                return Ok(Outcome::Skipped);
            }
            Err(ObjectError::NoSuchBucket) => {
                return Ok(Outcome::Fatal(format!(
                    "the bucket {} is gone",
                    import.bucket
                )));
            }
            _ => {}
        }
        let mut download =
            match runtime.block_on(client.get_object(&import.source.bucket, &object.key)) {
                Ok(download) => download,
                Err(err) if permanent(&err) => return Ok(Outcome::Failed(err.to_string())),
                Err(err) => return Ok(Outcome::Retry(err.to_string())),
            };
        // A storage error ends the step (the staging body it leaves is
        // collected as abandoned).
        let storage: &truthdb_core::storage::Storage = &self.storage;
        let mut writer = BodyWriter::begin(storage, super::now_ms())?;
        let mut size = 0u64;
        loop {
            if stopping() {
                return Ok(Outcome::Stopped);
            }
            match runtime.block_on(download.next_chunk()) {
                Ok(Some(data)) => {
                    size += data.len() as u64;
                    writer.write(storage, &data, super::now_ms())?;
                }
                Ok(None) => break,
                Err(err) => return Ok(Outcome::Retry(err.to_string())),
            }
        }
        let sealed = writer.finish(storage, super::now_ms())?;
        // An ETag that is an MD5 (not a multipart one, nor one of an object
        // under a KMS key, which is not) must be the bytes'.
        let etag = object.etag.trim_matches('"').to_ascii_lowercase();
        let kms = download
            .head
            .server_side_encryption
            .as_deref()
            .is_some_and(|algorithm| algorithm.starts_with("aws:kms"));
        let md5: String = sealed
            .md5
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if size != object.size || (!kms && etag.len() == 32 && !etag.contains('-') && etag != md5) {
            return Ok(Outcome::Failed(format!(
                "the bytes read ({size}, MD5 {md5}) are not the object listed ({}, ETag {})",
                object.size, object.etag
            )));
        }
        let attrs = ObjectAttrs {
            content_type: download.head.content_type,
            user: download.head.metadata,
            ..ObjectAttrs::default()
        };
        let mut moved = import.clone();
        moved.copied += 1;
        moved.bytes += size;
        moved.after = object.key.clone();
        moved.updated_ms = super::now_ms();
        let saved = moved.clone();
        enum Stored {
            Dropped,
            Locked,
            Put(Result<(), ObjectError>),
        }
        let stored = self.object(move |file| {
            // Decided in the write's own lock hold: an import does not give
            // objects a bucket's default retention.
            if file
                .bucket(&saved.bucket)?
                .is_some_and(|bucket| bucket.object_lock)
            {
                return Ok(Stored::Locked);
            }
            file.set_origin(Origin {
                operation: "Import".to_string(),
                ..Origin::default()
            });
            file.transaction(|tx| {
                // Dropped meanwhile: nothing is written.
                if tx.import(saved.id)?.is_none() {
                    return Ok(Stored::Dropped);
                }
                let stored = tx.put_object(
                    &saved.bucket,
                    &key,
                    &sealed,
                    &attrs,
                    StorageClass::Standard,
                    super::now_ms(),
                    IMPORT_DROP_GRACE_MS,
                )?;
                if stored.is_ok() {
                    tx.update_import(&saved)?;
                }
                Ok(Stored::Put(stored.map(|_| ())))
            })
        })?;
        Ok(match stored {
            Stored::Dropped => Outcome::Dropped,
            Stored::Locked => Outcome::Fatal(format!(
                "the bucket {} has Object Lock; imports into it are not offered",
                import.bucket
            )),
            Stored::Put(Ok(_)) => Outcome::Copied(Box::new(moved)),
            Stored::Put(Err(ObjectError::NoSuchBucket)) => {
                Outcome::Fatal(format!("the bucket {} is gone", import.bucket))
            }
            Stored::Put(Err(err)) => Outcome::Failed(format!("{err:?}")),
        })
    }

    fn save_import(&self, import: &Import) -> Result<(), EngineError> {
        let import = import.clone();
        // A dropped import stays dropped.
        self.object(move |file| {
            file.transaction(|tx| {
                if tx.import(import.id)?.is_some() {
                    tx.update_import(&import)?;
                }
                Ok(())
            })
        })?;
        Ok(())
    }

    /// Records why the import waits, and lets the thread rest before the
    /// next try.
    fn wait_import(&self, mut import: Import, error: String) -> Result<bool, EngineError> {
        import.error = error;
        import.updated_ms = super::now_ms();
        self.save_import(&import)?;
        Ok(false)
    }

    fn end_import(
        &self,
        mut import: Import,
        state: ImportState,
        error: String,
    ) -> Result<bool, EngineError> {
        import.state = state;
        if !error.is_empty() {
            import.error = error;
        }
        import.updated_ms = super::now_ms();
        self.save_import(&import)?;
        Ok(false)
    }
}
