//! Lifecycle configuration (S3 PutBucketLifecycleConfiguration) and the
//! daily pass that applies it. A bucket's rules are kept as one bucket
//! configuration; the lifecycle state tree holds, for every bucket with
//! rules, where its pass stands. A pass walks the bucket's versions in
//! order (each key's newest first, so whether a version is current, when its
//! successor was written and how many newer noncurrent versions it has are
//! known as it goes), then its multipart uploads. Each version gets at most
//! one action a day: a permanent removal before a transition, a transition
//! before a delete marker. Every action is an ordinary engine operation
//! (Object Lock applies) committed together with the pass's new place, so a
//! crash neither repeats nor skips one.

use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::buckets::Versioning;
use crate::documents::BUCKET_LIFECYCLE;
use crate::journal::Origin;
use crate::objects::{StorageClass, VersionSel, decode_version, version_from_key};
use crate::{LIFECYCLE_STATE_TREE, OBJECTS_TREE, ObjFile, ObjTxn};

pub const DAY_MS: u64 = 86_400_000;

/// `days` after `from_ms`, rounded up to the next midnight UTC: when S3
/// counts an age of `days` as reached.
pub fn days_after(from_ms: u64, days: u32) -> u64 {
    let end = from_ms.saturating_add(u64::from(days).saturating_mul(DAY_MS));
    (end / DAY_MS).saturating_add(1).saturating_mul(DAY_MS)
}

/// Objects under this size are not transitioned by default.
const SMALL_OBJECT: u64 = 128 * 1024;

/// Which small objects may transition
/// (`x-amz-transition-default-minimum-object-size`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MinTransitionSize {
    /// No object under 128 KiB transitions (`all_storage_classes_128K`).
    #[default]
    AllClasses128K,
    /// Objects under 128 KiB go to GLACIER or DEEP_ARCHIVE only
    /// (`varies_by_storage_class`).
    VariesByClass,
}

/// A bucket's lifecycle configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LifecycleConfig {
    pub min_size: MinTransitionSize,
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub id: String,
    pub enabled: bool,
    pub filter: Filter,
    pub expiration: Option<Expiration>,
    pub transitions: Vec<Transition>,
    pub noncurrent_expiration: Option<NoncurrentExpiration>,
    pub noncurrent_transitions: Vec<NoncurrentTransition>,
    /// AbortIncompleteMultipartUpload's DaysAfterInitiation.
    pub abort_upload_days: Option<u32>,
}

/// Which objects a rule applies to, in the form it was given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    /// The rule's own (deprecated) `Prefix`, without a `Filter`.
    Legacy(String),
    /// An empty `Filter`: every object.
    All,
    Prefix(String),
    Tag(String, String),
    SizeGreaterThan(u64),
    SizeLessThan(u64),
    And {
        prefix: Option<String>,
        tags: Vec<(String, String)>,
        size_greater_than: Option<u64>,
        size_less_than: Option<u64>,
    },
}

impl Filter {
    /// The key prefix it asks for (empty: any).
    pub fn prefix(&self) -> &str {
        match self {
            Filter::Legacy(prefix) | Filter::Prefix(prefix) => prefix,
            Filter::And {
                prefix: Some(prefix),
                ..
            } => prefix,
            _ => "",
        }
    }

    /// Whether it asks for tags.
    pub fn uses_tags(&self) -> bool {
        match self {
            Filter::Tag(..) => true,
            Filter::And { tags, .. } => !tags.is_empty(),
            _ => false,
        }
    }

    /// Whether it asks for an object size.
    pub fn uses_size(&self) -> bool {
        match self {
            Filter::SizeGreaterThan(_) | Filter::SizeLessThan(_) => true,
            Filter::And {
                size_greater_than,
                size_less_than,
                ..
            } => size_greater_than.is_some() || size_less_than.is_some(),
            _ => false,
        }
    }

    /// Whether an object with this key, size and tags is one it applies to
    /// (the size bounds exclude their values).
    pub fn matches(&self, key: &str, size: u64, tags: &[(String, String)]) -> bool {
        let has_tag = |name: &String, value: &String| {
            tags.iter()
                .any(|(tag, tag_value)| tag == name && tag_value == value)
        };
        if !key.starts_with(self.prefix()) {
            return false;
        }
        match self {
            Filter::Legacy(_) | Filter::All | Filter::Prefix(_) => true,
            Filter::Tag(name, value) => has_tag(name, value),
            Filter::SizeGreaterThan(bound) => size > *bound,
            Filter::SizeLessThan(bound) => size < *bound,
            Filter::And {
                tags: wanted,
                size_greater_than,
                size_less_than,
                ..
            } => {
                wanted.iter().all(|(name, value)| has_tag(name, value))
                    && size_greater_than.is_none_or(|bound| size > bound)
                    && size_less_than.is_none_or(|bound| size < bound)
            }
        }
    }
}

/// When an action is due: an age in days, or a date (midnight UTC).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum When {
    Days(u32),
    Date(u64),
}

impl When {
    /// When it falls for something written at `from_ms`.
    pub fn at(self, from_ms: u64) -> u64 {
        match self {
            When::Days(days) => days_after(from_ms, days),
            When::Date(date_ms) => date_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiration {
    At(When),
    /// `ExpiredObjectDeleteMarker` (only `true` does anything).
    ExpiredObjectDeleteMarker(bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition {
    pub when: When,
    pub class: StorageClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoncurrentExpiration {
    pub days: u32,
    /// How many of the newest noncurrent versions are kept regardless.
    pub newer_versions: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoncurrentTransition {
    pub days: u32,
    pub newer_versions: Option<u32>,
    pub class: StorageClass,
}

/// Whether a lifecycle transition may move a version from `from` to `to`
/// (S3's supported transitions: only towards colder classes, and not
/// every pair). REDUCED_REDUNDANCY goes where STANDARD goes.
pub fn transition_allowed(from: StorageClass, to: StorageClass) -> bool {
    use StorageClass::*;
    match from {
        Standard | ReducedRedundancy => matches!(
            to,
            StandardIa | IntelligentTiering | OnezoneIa | GlacierIr | Glacier | DeepArchive
        ),
        StandardIa => matches!(
            to,
            IntelligentTiering | OnezoneIa | GlacierIr | Glacier | DeepArchive
        ),
        IntelligentTiering => matches!(to, OnezoneIa | GlacierIr | Glacier | DeepArchive),
        OnezoneIa | GlacierIr => matches!(to, Glacier | DeepArchive),
        Glacier => to == DeepArchive,
        DeepArchive => false,
    }
}

/// How cold a transition target is: of several due at once, the coldest
/// (the least costly to keep) is taken.
fn coldness(class: StorageClass) -> u8 {
    use StorageClass::*;
    match class {
        Standard | ReducedRedundancy => 0,
        StandardIa => 1,
        IntelligentTiering => 2,
        OnezoneIa => 3,
        GlacierIr => 4,
        Glacier => 5,
        DeepArchive => 6,
    }
}

/// A version as the pass sees it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Seen<'a> {
    pub key: &'a str,
    pub size: u64,
    pub delete_marker: bool,
    pub class: StorageClass,
    pub mtime_ms: u64,
    /// The key's newest version.
    pub current: bool,
    /// The key's only version.
    pub only_version: bool,
    /// When the next newer version was written (for a noncurrent one).
    pub successor_ms: u64,
    /// How many newer versions of the key are noncurrent.
    pub newer_noncurrent: u32,
    pub tags: &'a [(String, String)],
}

/// What the pass does to a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    /// Removes the version for good.
    Remove,
    /// Moves the version to a colder class.
    Transition(StorageClass),
    /// Expires the key as a DeleteObject would: a delete marker in a
    /// versioned bucket, a removal in an unversioned one.
    Expire,
}

impl LifecycleConfig {
    fn enabled(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter().filter(|rule| rule.enabled)
    }

    /// Whether any enabled rule asks for tags (the pass then reads them).
    pub(crate) fn uses_tags(&self) -> bool {
        self.enabled().any(|rule| rule.filter.uses_tags())
    }

    /// Whether any enabled rule aborts multipart uploads.
    fn aborts_uploads(&self) -> bool {
        self.enabled().any(|rule| rule.abort_upload_days.is_some())
    }

    /// When the current version of `key` (with this size, tags and write
    /// time) expires, and by which rule: the earliest expiration of the
    /// enabled rules that apply (S3's `x-amz-expiration`).
    pub fn expiration_of(
        &self,
        key: &str,
        size: u64,
        tags: &[(String, String)],
        mtime_ms: u64,
    ) -> Option<(u64, &str)> {
        self.enabled()
            .filter(|rule| rule.filter.matches(key, size, tags))
            .filter_map(|rule| match rule.expiration {
                Some(Expiration::At(when)) => Some((when.at(mtime_ms), rule.id.as_str())),
                _ => None,
            })
            .min_by_key(|(at, _)| *at)
    }

    /// The action due on `seen` at `now_ms`, if any. `versioned`: whether
    /// the bucket's versioning was ever enabled (an expiration then adds a
    /// delete marker rather than removing the object).
    pub(crate) fn decide(&self, seen: &Seen<'_>, versioned: bool, now_ms: u64) -> Option<Action> {
        let mut remove = false;
        let mut expire = false;
        let mut target: Option<StorageClass> = None;
        let mut consider = |class: StorageClass| {
            let size_ok = seen.size >= SMALL_OBJECT
                || (self.min_size == MinTransitionSize::VariesByClass
                    && matches!(class, StorageClass::Glacier | StorageClass::DeepArchive));
            if transition_allowed(seen.class, class)
                && size_ok
                && target.is_none_or(|current| coldness(class) > coldness(current))
            {
                target = Some(class);
            }
        };
        for rule in self
            .enabled()
            .filter(|rule| rule.filter.matches(seen.key, seen.size, seen.tags))
        {
            if seen.current && seen.delete_marker {
                // An expired object delete marker: the key's only version.
                if seen.only_version {
                    match rule.expiration {
                        Some(Expiration::ExpiredObjectDeleteMarker(true)) => remove = true,
                        Some(Expiration::At(When::Days(days)))
                            if now_ms >= days_after(seen.mtime_ms, days) =>
                        {
                            remove = true
                        }
                        _ => {}
                    }
                }
            } else if seen.current {
                if let Some(Expiration::At(when)) = rule.expiration
                    && now_ms >= when.at(seen.mtime_ms)
                {
                    expire = true;
                }
                for transition in &rule.transitions {
                    if now_ms >= transition.when.at(seen.mtime_ms) {
                        consider(transition.class);
                    }
                }
            } else {
                let due = |days: u32, newer: Option<u32>| {
                    now_ms >= days_after(seen.successor_ms, days)
                        && newer.is_none_or(|keep| seen.newer_noncurrent >= keep)
                };
                if let Some(expiration) = rule.noncurrent_expiration
                    && due(expiration.days, expiration.newer_versions)
                {
                    remove = true;
                }
                if !seen.delete_marker {
                    for transition in &rule.noncurrent_transitions {
                        if due(transition.days, transition.newer_versions) {
                            consider(transition.class);
                        }
                    }
                }
            }
        }
        // A removal before a transition, a transition before a marker; in an
        // unversioned bucket an expiration is a removal.
        if remove || (expire && !versioned) {
            Some(if remove {
                Action::Remove
            } else {
                Action::Expire
            })
        } else if let Some(class) = target {
            Some(Action::Transition(class))
        } else if expire {
            Some(Action::Expire)
        } else {
            None
        }
    }

    /// The rule aborting an upload of `key` started at `initiated_ms`, due
    /// at `now_ms`.
    fn aborts(&self, key: &str, initiated_ms: u64, now_ms: u64) -> bool {
        self.enabled().any(|rule| {
            rule.abort_upload_days
                .is_some_and(|days| now_ms >= days_after(initiated_ms, days))
                && key.starts_with(rule.filter.prefix())
                && !rule.filter.uses_tags()
                && !rule.filter.uses_size()
        })
    }
}

// The stored form: a version byte, then each value in order. Strings are
// `[len u32][bytes]`, options a presence byte, lists `[count u32]`.

const FORMAT: u8 = 1;

#[derive(Default)]
struct Writer(Vec<u8>);

impl Writer {
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn str(&mut self, value: &str) {
        self.u32(value.len() as u32);
        self.0.extend_from_slice(value.as_bytes());
    }
    fn opt_u32(&mut self, value: Option<u32>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u32(value);
            }
            None => self.u8(0),
        }
    }
    fn opt_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
            None => self.u8(0),
        }
    }
    fn tags(&mut self, tags: &[(String, String)]) {
        self.u32(tags.len() as u32);
        for (name, value) in tags {
            self.str(name);
            self.str(value);
        }
    }
    fn when(&mut self, when: When) {
        match when {
            When::Days(days) => {
                self.u8(0);
                self.u64(u64::from(days));
            }
            When::Date(date_ms) => {
                self.u8(1);
                self.u64(date_ms);
            }
        }
    }
    fn class(&mut self, class: StorageClass) {
        self.u8(class.code());
    }
}

struct Reader<'a>(&'a [u8]);

fn corrupt() -> StorageError {
    StorageError::InvalidFile("corrupt lifecycle configuration".to_string())
}

impl Reader<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8], StorageError> {
        if self.0.len() < len {
            return Err(corrupt());
        }
        let (head, tail) = self.0.split_at(len);
        self.0 = tail;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn str(&mut self) -> Result<String, StorageError> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| corrupt())
    }
    fn present(&mut self) -> Result<bool, StorageError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(corrupt()),
        }
    }
    fn opt_u32(&mut self) -> Result<Option<u32>, StorageError> {
        Ok(if self.present()? {
            Some(self.u32()?)
        } else {
            None
        })
    }
    fn opt_u64(&mut self) -> Result<Option<u64>, StorageError> {
        Ok(if self.present()? {
            Some(self.u64()?)
        } else {
            None
        })
    }
    fn count(&mut self) -> Result<usize, StorageError> {
        let count = self.u32()? as usize;
        // Every item takes at least a byte: a count past the bytes left is
        // corrupt, not a huge allocation.
        if count > self.0.len() {
            return Err(corrupt());
        }
        Ok(count)
    }
    fn tags(&mut self) -> Result<Vec<(String, String)>, StorageError> {
        (0..self.count()?)
            .map(|_| Ok((self.str()?, self.str()?)))
            .collect()
    }
    fn when(&mut self) -> Result<When, StorageError> {
        match self.u8()? {
            0 => Ok(When::Days(
                u32::try_from(self.u64()?).map_err(|_| corrupt())?,
            )),
            1 => Ok(When::Date(self.u64()?)),
            _ => Err(corrupt()),
        }
    }
    fn class(&mut self) -> Result<StorageClass, StorageError> {
        match self.u8()? {
            code @ 0..8 => Ok(StorageClass::from_code(code)),
            _ => Err(corrupt()),
        }
    }
}

impl LifecycleConfig {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u8(FORMAT);
        out.u8(match self.min_size {
            MinTransitionSize::AllClasses128K => 0,
            MinTransitionSize::VariesByClass => 1,
        });
        out.u32(self.rules.len() as u32);
        for rule in &self.rules {
            out.str(&rule.id);
            out.u8(rule.enabled as u8);
            match &rule.filter {
                Filter::Legacy(prefix) => {
                    out.u8(0);
                    out.str(prefix);
                }
                Filter::All => out.u8(1),
                Filter::Prefix(prefix) => {
                    out.u8(2);
                    out.str(prefix);
                }
                Filter::Tag(name, value) => {
                    out.u8(3);
                    out.str(name);
                    out.str(value);
                }
                Filter::SizeGreaterThan(bound) => {
                    out.u8(4);
                    out.u64(*bound);
                }
                Filter::SizeLessThan(bound) => {
                    out.u8(5);
                    out.u64(*bound);
                }
                Filter::And {
                    prefix,
                    tags,
                    size_greater_than,
                    size_less_than,
                } => {
                    out.u8(6);
                    match prefix {
                        Some(prefix) => {
                            out.u8(1);
                            out.str(prefix);
                        }
                        None => out.u8(0),
                    }
                    out.tags(tags);
                    out.opt_u64(*size_greater_than);
                    out.opt_u64(*size_less_than);
                }
            }
            match rule.expiration {
                None => out.u8(0),
                Some(Expiration::At(when)) => {
                    out.u8(1);
                    out.when(when);
                }
                Some(Expiration::ExpiredObjectDeleteMarker(on)) => {
                    out.u8(2);
                    out.u8(on as u8);
                }
            }
            out.u32(rule.transitions.len() as u32);
            for transition in &rule.transitions {
                out.when(transition.when);
                out.class(transition.class);
            }
            match rule.noncurrent_expiration {
                None => out.u8(0),
                Some(expiration) => {
                    out.u8(1);
                    out.u32(expiration.days);
                    out.opt_u32(expiration.newer_versions);
                }
            }
            out.u32(rule.noncurrent_transitions.len() as u32);
            for transition in &rule.noncurrent_transitions {
                out.u32(transition.days);
                out.opt_u32(transition.newer_versions);
                out.class(transition.class);
            }
            out.opt_u32(rule.abort_upload_days);
        }
        out.0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader(bytes);
        if input.u8()? != FORMAT {
            return Err(corrupt());
        }
        let min_size = match input.u8()? {
            0 => MinTransitionSize::AllClasses128K,
            1 => MinTransitionSize::VariesByClass,
            _ => return Err(corrupt()),
        };
        let mut rules = Vec::new();
        for _ in 0..input.count()? {
            let id = input.str()?;
            let enabled = input.present()?;
            let filter = match input.u8()? {
                0 => Filter::Legacy(input.str()?),
                1 => Filter::All,
                2 => Filter::Prefix(input.str()?),
                3 => Filter::Tag(input.str()?, input.str()?),
                4 => Filter::SizeGreaterThan(input.u64()?),
                5 => Filter::SizeLessThan(input.u64()?),
                6 => Filter::And {
                    prefix: if input.present()? {
                        Some(input.str()?)
                    } else {
                        None
                    },
                    tags: input.tags()?,
                    size_greater_than: input.opt_u64()?,
                    size_less_than: input.opt_u64()?,
                },
                _ => return Err(corrupt()),
            };
            let expiration = match input.u8()? {
                0 => None,
                1 => Some(Expiration::At(input.when()?)),
                2 => Some(Expiration::ExpiredObjectDeleteMarker(input.present()?)),
                _ => return Err(corrupt()),
            };
            let transitions = (0..input.count()?)
                .map(|_| {
                    Ok(Transition {
                        when: input.when()?,
                        class: input.class()?,
                    })
                })
                .collect::<Result<_, StorageError>>()?;
            let noncurrent_expiration = if input.present()? {
                Some(NoncurrentExpiration {
                    days: input.u32()?,
                    newer_versions: input.opt_u32()?,
                })
            } else {
                None
            };
            let noncurrent_transitions = (0..input.count()?)
                .map(|_| {
                    Ok(NoncurrentTransition {
                        days: input.u32()?,
                        newer_versions: input.opt_u32()?,
                        class: input.class()?,
                    })
                })
                .collect::<Result<_, StorageError>>()?;
            rules.push(Rule {
                id,
                enabled,
                filter,
                expiration,
                transitions,
                noncurrent_expiration,
                noncurrent_transitions,
                abort_upload_days: input.opt_u32()?,
            });
        }
        if !input.0.is_empty() {
            return Err(corrupt());
        }
        Ok(LifecycleConfig { min_size, rules })
    }
}

/// Where a bucket's pass stands.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PassState {
    /// The day (since the epoch) the latest pass started.
    started_day: u64,
    finished: bool,
    /// 0: versions; 1: multipart uploads.
    phase: u8,
    /// The last version row (phase 0) or upload key (phase 1) done.
    cursor: Vec<u8>,
    /// Phase 0: how many noncurrent versions of the cursor's key came after
    /// its current one, and when the cursor's version was written.
    newer_noncurrent: u32,
    last_mtime_ms: u64,
    /// Phase 1: the last upload done.
    upload: u64,
}

impl PassState {
    /// `[started day u64][finished u8][phase u8][newer u32][mtime u64]
    /// [upload u64][cursor]`.
    fn encode(&self) -> Vec<u8> {
        let mut out = Writer::default();
        out.u64(self.started_day);
        out.u8(self.finished as u8);
        out.u8(self.phase);
        out.u32(self.newer_noncurrent);
        out.u64(self.last_mtime_ms);
        out.u64(self.upload);
        out.0.extend_from_slice(&self.cursor);
        out.0
    }

    fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut input = Reader(bytes);
        let state = PassState {
            started_day: input.u64()?,
            finished: input.present()?,
            phase: input.u8()?,
            newer_noncurrent: input.u32()?,
            last_mtime_ms: input.u64()?,
            upload: input.u64()?,
            cursor: Vec::new(),
        };
        if state.phase > 1 {
            return Err(corrupt());
        }
        Ok(PassState {
            cursor: input.0.to_vec(),
            ..state
        })
    }
}

/// How much one step of the pass may do before it yields the lock.
#[derive(Debug, Clone, Copy)]
pub struct StepBudget {
    /// Versions (or uploads) looked at.
    pub rows: usize,
    /// Actions taken, each a forced transaction.
    pub actions: usize,
}

impl ObjTxn<'_, '_> {
    /// Sets the bucket's lifecycle configuration (the caller checks the
    /// bucket exists), and has its pass start over.
    pub fn put_lifecycle(
        &mut self,
        bucket: &str,
        config: &LifecycleConfig,
    ) -> Result<(), StorageError> {
        self.put_bucket_config(bucket, BUCKET_LIFECYCLE, "", &config.encode())?;
        self.write_pass(bucket, &PassState::default())
    }

    /// Deletes the bucket's lifecycle configuration; whether it had one.
    pub fn delete_lifecycle(&mut self, bucket: &str) -> Result<bool, StorageError> {
        self.forget_pass(bucket)?;
        self.delete_bucket_config(bucket, BUCKET_LIFECYCLE, "")
    }

    /// Removes the bucket's pass state (with its configuration, or the
    /// bucket).
    pub(crate) fn forget_pass(&mut self, bucket: &str) -> Result<(), StorageError> {
        self.trees.lifecycle_state().delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            bucket.as_bytes(),
        )?;
        Ok(())
    }

    fn write_pass(&mut self, bucket: &str, state: &PassState) -> Result<(), StorageError> {
        let tree = self.trees.lifecycle_state();
        let mode = &mut OpMode::Txn(self.txn);
        tree.delete(self.ctx, mode, bucket.as_bytes())?;
        tree.insert_unique(self.ctx, mode, bucket.as_bytes(), &state.encode())?;
        Ok(())
    }
}

impl ObjFile<'_> {
    /// The bucket's lifecycle configuration, if it has one.
    pub fn lifecycle(&mut self, bucket: &str) -> Result<Option<LifecycleConfig>, StorageError> {
        self.bucket_config(bucket, BUCKET_LIFECYCLE, "")?
            .map(|bytes| LifecycleConfig::decode(&bytes))
            .transpose()
    }

    /// One step of the lifecycle passes at `now_ms`: the next bucket whose
    /// pass is due (once a day) goes on from where it stands, within
    /// `budget`. Dropped bodies are collected after `grace_ms`. Returns
    /// whether a pass did anything (more may be due).
    pub fn lifecycle_step(
        &mut self,
        now_ms: u64,
        grace_ms: u64,
        budget: StepBudget,
    ) -> Result<bool, StorageError> {
        let Some(states) = self.existing_tree(LIFECYCLE_STATE_TREE)? else {
            return Ok(false);
        };
        let today = now_ms / DAY_MS;
        let mut due = None;
        for (bucket, row) in states.scan(&mut self.core.page_ctx(0))? {
            let state = PassState::decode(&row)?;
            if !state.finished || state.started_day < today {
                due = Some((bucket, state));
                break;
            }
        }
        let Some((bucket, mut state)) = due else {
            return Ok(false);
        };
        let bucket = String::from_utf8(bucket).map_err(|_| corrupt())?;
        if state.finished {
            state = PassState {
                started_day: today,
                ..PassState::default()
            };
        }
        let config = self.lifecycle(&bucket)?.unwrap_or_default();
        let versioned = self
            .bucket(&bucket)?
            .is_some_and(|info| info.versioning != Versioning::Unversioned);
        let done = if state.phase == 0 {
            self.lifecycle_versions(
                &bucket, &config, versioned, &mut state, now_ms, grace_ms, budget,
            )?
        } else {
            false
        };
        let finished = if state.phase == 0 && !done {
            false
        } else {
            if state.phase == 0 {
                state.phase = 1;
                state.cursor.clear();
            }
            !config.aborts_uploads()
                || self.lifecycle_uploads(&bucket, &config, &mut state, now_ms, grace_ms, budget)?
        };
        state.finished = finished;
        let bucket_name = bucket.clone();
        self.origin = None;
        self.transaction(|tx| tx.write_pass(&bucket_name, &state))?;
        Ok(true)
    }

    /// Phase 0 within `budget`: returns whether every version was looked at.
    #[allow(clippy::too_many_arguments)]
    fn lifecycle_versions(
        &mut self,
        bucket: &str,
        config: &LifecycleConfig,
        versioned: bool,
        state: &mut PassState,
        now_ms: u64,
        grace_ms: u64,
        budget: StepBudget,
    ) -> Result<bool, StorageError> {
        let Some(objects) = self.existing_tree(OBJECTS_TREE)? else {
            return Ok(true);
        };
        let start = [bucket.as_bytes(), &[0]].concat();
        let lower = if state.cursor.is_empty() {
            start.clone()
        } else {
            // Just past the cursor: no row key has another as its prefix.
            [state.cursor.as_slice(), &[0]].concat()
        };
        let upper = [bucket.as_bytes(), &[1]].concat();
        let rows = objects.scan_range_limit(
            &mut self.core.page_ctx(0),
            Some(&lower),
            Some(&upper),
            budget.rows + 1,
        )?;
        let key_of = |row_key: &[u8]| -> Result<String, StorageError> {
            let key = row_key
                .get(start.len()..row_key.len().saturating_sub(9))
                .ok_or_else(corrupt)?;
            String::from_utf8(key.to_vec()).map_err(|_| corrupt())
        };
        let read_tags = config.uses_tags();
        let mut actions = 0;
        for (index, (row_key, row)) in rows.iter().enumerate().take(budget.rows) {
            let key = key_of(row_key)?;
            let version = version_from_key(row_key)?;
            let info = decode_version(version, row)?;
            let current = state.cursor.is_empty() || key_of(&state.cursor)? != key;
            let only_version = current
                && rows
                    .get(index + 1)
                    .map(|(next, _)| key_of(next))
                    .transpose()?
                    .is_none_or(|next| next != key);
            let tags = if read_tags && !info.delete_marker {
                self.version_attrs(version)?.tags
            } else {
                Vec::new()
            };
            let seen = crate::lifecycle::Seen {
                key: &key,
                size: info.size,
                delete_marker: info.delete_marker,
                class: info.storage_class,
                mtime_ms: info.mtime_ms,
                current,
                only_version,
                successor_ms: state.last_mtime_ms,
                newer_noncurrent: if current { 0 } else { state.newer_noncurrent },
                tags: &tags,
            };
            let action = if actions < budget.actions {
                config.decide(&seen, versioned, now_ms)
            } else {
                None
            };
            if action.is_none() && actions >= budget.actions {
                // Out of actions: the rest waits for the next step.
                return Ok(false);
            }
            state.newer_noncurrent = if current {
                0
            } else {
                state.newer_noncurrent + 1
            };
            state.last_mtime_ms = info.mtime_ms;
            state.cursor = row_key.clone();
            let Some(action) = action else {
                continue;
            };
            actions += 1;
            self.origin = Some(Origin {
                operation: match action {
                    Action::Transition(_) => "LifecycleTransition",
                    Action::Remove | Action::Expire => "LifecycleExpiration",
                }
                .to_string(),
                ..Origin::default()
            });
            let state_now = state.clone();
            self.transaction(|tx| {
                // Refused by Object Lock: the version stays, the pass goes on.
                let _ = match action {
                    Action::Remove => tx
                        .delete_object(
                            bucket,
                            &key,
                            Some(VersionSel::Seq(version)),
                            false,
                            now_ms,
                            grace_ms,
                        )?
                        .map(|_| ()),
                    Action::Expire => tx
                        .delete_object(bucket, &key, None, false, now_ms, grace_ms)?
                        .map(|_| ()),
                    Action::Transition(class) => {
                        tx.transition_version(bucket, &key, version, class, now_ms)?
                    }
                };
                tx.write_pass(bucket, &state_now)
            })?;
        }
        Ok(rows.len() <= budget.rows)
    }

    /// Phase 1 within `budget`: aborts the uploads rules say are due.
    /// Returns whether every upload was looked at.
    fn lifecycle_uploads(
        &mut self,
        bucket: &str,
        config: &LifecycleConfig,
        state: &mut PassState,
        now_ms: u64,
        grace_ms: u64,
        budget: StepBudget,
    ) -> Result<bool, StorageError> {
        let after_key = String::from_utf8(state.cursor.clone()).map_err(|_| corrupt())?;
        let after = (!state.cursor.is_empty()).then_some((after_key.as_str(), state.upload));
        let (uploads, truncated) = self.list_uploads(bucket, "", after, budget.rows)?;
        let mut actions = 0;
        for upload in uploads {
            if config.aborts(&upload.key, upload.initiated_ms, now_ms) {
                if actions == budget.actions {
                    return Ok(false);
                }
                actions += 1;
                state.cursor = upload.key.clone().into_bytes();
                state.upload = upload.upload;
                let state_now = state.clone();
                self.origin = None;
                self.transaction(|tx| {
                    // Gone meanwhile (completed or aborted) is fine.
                    let _ = tx.abort_upload(upload.upload, now_ms, grace_ms)?;
                    tx.write_pass(bucket, &state_now)
                })?;
            } else {
                state.cursor = upload.key.into_bytes();
                state.upload = upload.upload;
            }
        }
        Ok(!truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(filter: Filter) -> Rule {
        Rule {
            id: "r".to_string(),
            enabled: true,
            filter,
            expiration: None,
            transitions: Vec::new(),
            noncurrent_expiration: None,
            noncurrent_transitions: Vec::new(),
            abort_upload_days: None,
        }
    }

    fn seen<'a>(key: &'a str, tags: &'a [(String, String)]) -> Seen<'a> {
        Seen {
            key,
            size: 1 << 20,
            delete_marker: false,
            class: StorageClass::Standard,
            mtime_ms: 0,
            current: true,
            only_version: true,
            successor_ms: 0,
            newer_noncurrent: 0,
            tags,
        }
    }

    #[test]
    fn ages_round_up_to_the_next_midnight_utc() {
        // 2014-01-15 10:30 UTC plus 3 days is due 2014-01-19 00:00 UTC (the
        // example S3 documents).
        assert_eq!(days_after(1_389_781_800_000, 3), 1_390_089_600_000);
        assert_eq!(days_after(DAY_MS * 4, 0), DAY_MS * 5);
        assert_eq!(days_after(u64::MAX, u32::MAX), u64::MAX);
    }

    #[test]
    fn filters_match_by_prefix_tags_and_exclusive_sizes() {
        let tags = vec![("team".to_string(), "blue".to_string())];
        assert!(Filter::All.matches("a", 0, &[]));
        assert!(Filter::Legacy("logs/".into()).matches("logs/x", 0, &[]));
        assert!(!Filter::Prefix("logs/".into()).matches("log", 0, &[]));
        assert!(Filter::Tag("team".into(), "blue".into()).matches("k", 0, &tags));
        assert!(!Filter::Tag("team".into(), "red".into()).matches("k", 0, &tags));
        assert!(!Filter::SizeGreaterThan(10).matches("k", 10, &[]));
        assert!(Filter::SizeGreaterThan(10).matches("k", 11, &[]));
        assert!(!Filter::SizeLessThan(10).matches("k", 10, &[]));
        let and = Filter::And {
            prefix: Some("p/".into()),
            tags: tags.clone(),
            size_greater_than: Some(5),
            size_less_than: Some(100),
        };
        assert!(and.matches("p/k", 50, &tags));
        assert!(!and.matches("p/k", 50, &[]));
        assert!(!and.matches("q/k", 50, &tags));
        assert!(!and.matches("p/k", 100, &tags));
        assert!(and.uses_tags() && and.uses_size());
    }

    #[test]
    fn transitions_only_go_colder_along_s3s_paths() {
        use StorageClass::*;
        assert!(transition_allowed(Standard, DeepArchive));
        assert!(transition_allowed(StandardIa, IntelligentTiering));
        assert!(!transition_allowed(OnezoneIa, GlacierIr));
        assert!(!transition_allowed(Glacier, StandardIa));
        assert!(!transition_allowed(DeepArchive, Glacier));
        assert!(!transition_allowed(Glacier, Glacier));
    }

    #[test]
    fn a_removal_beats_a_transition_which_beats_a_marker() {
        let mut expiring = rule(Filter::All);
        expiring.expiration = Some(Expiration::At(When::Days(1)));
        expiring.transitions = vec![
            Transition {
                when: When::Days(0),
                class: StorageClass::StandardIa,
            },
            Transition {
                when: When::Days(0),
                class: StorageClass::Glacier,
            },
        ];
        let config = LifecycleConfig {
            min_size: MinTransitionSize::AllClasses128K,
            rules: vec![expiring],
        };
        let now = DAY_MS * 3;
        // The coldest transition first, in a versioned bucket.
        assert_eq!(
            config.decide(&seen("k", &[]), true, now),
            Some(Action::Transition(StorageClass::Glacier))
        );
        // Unversioned, the expiration removes the object.
        assert_eq!(
            config.decide(&seen("k", &[]), false, now),
            Some(Action::Expire)
        );
        // Once cold, the marker comes.
        let cold = Seen {
            class: StorageClass::Glacier,
            ..seen("k", &[])
        };
        assert_eq!(config.decide(&cold, true, now), Some(Action::Expire));
        // Small objects stay where they are.
        let small = Seen {
            size: 1000,
            ..seen("k", &[])
        };
        assert_eq!(config.decide(&small, true, now), Some(Action::Expire));
        let varies = LifecycleConfig {
            min_size: MinTransitionSize::VariesByClass,
            ..config.clone()
        };
        assert_eq!(
            varies.decide(&small, true, now),
            Some(Action::Transition(StorageClass::Glacier))
        );
        // Not yet due.
        assert_eq!(config.decide(&seen("k", &[]), true, 0), None);
        let disabled = LifecycleConfig {
            rules: vec![Rule {
                enabled: false,
                ..config.rules[0].clone()
            }],
            ..config.clone()
        };
        assert_eq!(disabled.decide(&seen("k", &[]), true, now), None);
    }

    #[test]
    fn noncurrent_versions_go_by_their_successors_age_and_rank() {
        let mut keep_two = rule(Filter::Prefix("k".into()));
        keep_two.noncurrent_expiration = Some(NoncurrentExpiration {
            days: 2,
            newer_versions: Some(2),
        });
        let config = LifecycleConfig {
            min_size: MinTransitionSize::default(),
            rules: vec![keep_two],
        };
        let old = |newer_noncurrent| Seen {
            current: false,
            only_version: false,
            successor_ms: DAY_MS,
            newer_noncurrent,
            ..seen("k", &[])
        };
        assert_eq!(config.decide(&old(1), true, DAY_MS * 9), None);
        assert_eq!(
            config.decide(&old(2), true, DAY_MS * 9),
            Some(Action::Remove)
        );
        // Successor written on day 1: due at the end of day 3.
        assert_eq!(config.decide(&old(2), true, DAY_MS * 4 - 1), None);
        assert_eq!(
            config.decide(&old(2), true, DAY_MS * 4),
            Some(Action::Remove)
        );
    }

    #[test]
    fn an_expired_delete_marker_goes_when_it_is_the_only_version() {
        let mut cleanup = rule(Filter::All);
        cleanup.expiration = Some(Expiration::ExpiredObjectDeleteMarker(true));
        let mut by_age = rule(Filter::All);
        by_age.expiration = Some(Expiration::At(When::Days(5)));
        let marker = Seen {
            delete_marker: true,
            size: 0,
            ..seen("k", &[])
        };
        let with_older = Seen {
            only_version: false,
            ..marker
        };
        let config = |rule: Rule| LifecycleConfig {
            min_size: MinTransitionSize::default(),
            rules: vec![rule],
        };
        assert_eq!(
            config(cleanup.clone()).decide(&marker, true, 0),
            Some(Action::Remove)
        );
        assert_eq!(config(cleanup).decide(&with_older, true, 0), None);
        assert_eq!(
            config(by_age.clone()).decide(&marker, true, DAY_MS * 5),
            None
        );
        assert_eq!(
            config(by_age).decide(&marker, true, DAY_MS * 6),
            Some(Action::Remove)
        );
    }

    #[test]
    fn the_expiration_header_names_the_earliest_rule() {
        let mut late = rule(Filter::All);
        late.id = "late".into();
        late.expiration = Some(Expiration::At(When::Days(30)));
        let mut early = rule(Filter::Prefix("logs/".into()));
        early.id = "early".into();
        early.expiration = Some(Expiration::At(When::Date(DAY_MS * 10)));
        let config = LifecycleConfig {
            min_size: MinTransitionSize::default(),
            rules: vec![late, early],
        };
        assert_eq!(
            config.expiration_of("logs/a", 1, &[], 0),
            Some((DAY_MS * 10, "early"))
        );
        assert_eq!(
            config.expiration_of("data", 1, &[], 0),
            Some((DAY_MS * 31, "late"))
        );
    }

    #[test]
    fn a_configuration_round_trips_and_corruption_is_refused() {
        let config = LifecycleConfig {
            min_size: MinTransitionSize::VariesByClass,
            rules: vec![
                Rule {
                    id: "every-thing".into(),
                    enabled: false,
                    filter: Filter::And {
                        prefix: Some("p/".into()),
                        tags: vec![("a".into(), "b".into())],
                        size_greater_than: Some(1),
                        size_less_than: None,
                    },
                    expiration: Some(Expiration::At(When::Date(DAY_MS * 9))),
                    transitions: vec![Transition {
                        when: When::Days(30),
                        class: StorageClass::DeepArchive,
                    }],
                    noncurrent_expiration: Some(NoncurrentExpiration {
                        days: 3,
                        newer_versions: Some(4),
                    }),
                    noncurrent_transitions: vec![NoncurrentTransition {
                        days: 1,
                        newer_versions: None,
                        class: StorageClass::GlacierIr,
                    }],
                    abort_upload_days: Some(7),
                },
                Rule {
                    expiration: Some(Expiration::ExpiredObjectDeleteMarker(true)),
                    ..rule(Filter::Legacy("old/".into()))
                },
                rule(Filter::Tag("t".into(), "v".into())),
                rule(Filter::SizeLessThan(9)),
                rule(Filter::SizeGreaterThan(9)),
                rule(Filter::All),
                rule(Filter::Prefix(String::new())),
            ],
        };
        let bytes = config.encode();
        assert_eq!(LifecycleConfig::decode(&bytes).unwrap(), config);
        for cut in 0..bytes.len() {
            assert!(
                LifecycleConfig::decode(&bytes[..cut]).is_err(),
                "cut at {cut}"
            );
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(LifecycleConfig::decode(&longer).is_err());
        let state = PassState {
            started_day: 9,
            finished: true,
            phase: 1,
            cursor: b"bkt\0key".to_vec(),
            newer_noncurrent: 3,
            last_mtime_ms: 77,
            upload: 5,
        };
        assert_eq!(PassState::decode(&state.encode()).unwrap(), state);
    }
}
