//! Authorizing S3 requests. The policies that can apply (the caller's own,
//! when it is a user, and those of the buckets the request names) are loaded
//! once, in one engine call; each operation then checks its action and
//! resource against them, with the request's condition keys.
//!
//! A bucket's owner is recorded as it was loaded, and the request's scope
//! (see `scope`) holds every later engine call to it: a bucket deleted and
//! created again meanwhile fails the request rather than letting it act on
//! a bucket it was not authorized against.

use std::collections::HashMap;

use hyper::HeaderMap;
use truthdb_core::storage::StorageError;
use truthdb_iam::{Context, Decision, Identity, Policy, PolicyKind, evaluate_all};
use truthdb_object::{BUCKET_POLICY, ObjFile, VersionSel};

use crate::error::S3Error;
use crate::server::{Connection, Principal, S3State, Signed};

/// A bucket as authorization sees it.
#[derive(Debug, Clone)]
pub struct BucketGrant {
    /// The owning account; `None` when the bucket does not exist.
    pub owner: Option<String>,
    pub policy: Option<Policy>,
    /// Whether the policy grants anything to the public (see
    /// `truthdb_iam::is_public`).
    pub policy_public: bool,
    /// The Block Public Access settings that apply (see `public_access`).
    pub public_block: u8,
    /// A directory bucket's zone (none: a general purpose bucket's).
    pub zone: Option<String>,
    /// When the bucket was created: with its owner, which bucket of its
    /// name it is (one deleted and made again is another).
    pub created_ms: Option<u64>,
    /// An S3 Tables table's storage bucket: its table, and the policies of
    /// its table bucket and table (its own has none).
    pub warehouse: Option<WarehouseGrant>,
}

/// An S3 Tables table's storage bucket, as authorization sees it.
#[derive(Debug, Clone)]
pub struct WarehouseGrant {
    pub table_bucket: String,
    pub table_id: String,
    /// Its table was deleted: the bucket is no longer there.
    pub dropped: bool,
    pub policies: Vec<Policy>,
}

/// What a request is authorized against.
#[derive(Debug, Clone)]
pub struct Authz {
    pub identity: Identity,
    policies: Vec<Policy>,
    pub buckets: HashMap<String, BucketGrant>,
    /// The tags of the objects the request names (when a policy reads
    /// them), by bucket and key: `s3:ExistingObjectTag/...` of each.
    existing_tags: HashMap<(String, String), Vec<(String, String)>>,
    context: Context,
    /// The server's region, for a directory bucket's ARN; none outside a
    /// request (a batch job's or replication's work, which acts on no
    /// directory bucket).
    region: Option<String>,
    /// The session the request was signed with.
    session: Option<crate::session::Scope>,
    /// The access point the request is made through.
    via: Option<crate::access_points::Via>,
}

/// The changes of a bucket (not its objects') a ReadOnly session may not
/// make (CreateBucket no session makes).
const BUCKET_WRITES: [&str; 7] = [
    "s3:DeleteBucket",
    "s3:PutBucketPolicy",
    "s3:DeleteBucketPolicy",
    "s3:PutEncryptionConfiguration",
    "s3:PutInventoryConfiguration",
    "s3:PutLifecycleConfiguration",
    "s3express:CreateSession",
];

/// The resource an operation acts on.
#[derive(Debug, Clone, Copy)]
pub enum Resource<'a> {
    /// The service: ListBuckets.
    Service,
    Bucket(&'a str),
    Object(&'a str, &'a str),
    /// A resource by its ARN, of no bucket (an access point's).
    Arn(&'a str),
}

fn corrupt_policy(what: &str, err: truthdb_iam::MalformedPolicy) -> StorageError {
    StorageError::InvalidFile(format!("stored {what} is not a valid policy: {}", err.0))
}

/// The bucket policy and user policies of a request, and (when a policy
/// reads `s3:ExistingObjectTag/...`) the tags of the objects it names.
struct Loaded {
    identity: Identity,
    policies: Vec<Policy>,
    buckets: HashMap<String, BucketGrant>,
    existing_tags: HashMap<(String, String), Vec<(String, String)>>,
}

/// An object a request names (its target, or a copy's source), and which
/// version.
pub type Named = (String, String, VersionSel);

fn load(
    file: &mut ObjFile<'_>,
    principal: Option<&Principal>,
    buckets: &[String],
    objects: &[Named],
) -> Result<Loaded, StorageError> {
    let mut texts = Vec::new();
    let (identity, policies) = match principal {
        None => (Identity::Anonymous, Vec::new()),
        Some(principal) => match &principal.user {
            None => (
                Identity::Root {
                    account: principal.account.clone(),
                },
                Vec::new(),
            ),
            Some(name) => {
                let user_id = file
                    .users(&principal.account)?
                    .into_iter()
                    .find(|user| &user.name == name)
                    .map(|user| user.user_id)
                    // A user dropped since its key was checked: no policy holds.
                    .unwrap_or_default();
                let mut policies = Vec::new();
                for (policy_name, document) in file.user_policies(&principal.account, name)? {
                    let text = String::from_utf8_lossy(&document).into_owned();
                    policies.push(Policy::parse(&text, PolicyKind::UserInline).map_err(|err| {
                        corrupt_policy(&format!("user policy {policy_name}"), err)
                    })?);
                    texts.push(text);
                }
                for policy_name in file.attached_policies(&principal.account, name)? {
                    // A policy detached meanwhile is gone: it grants nothing.
                    let Some(managed) = file.managed_policy(&principal.account, &policy_name)?
                    else {
                        continue;
                    };
                    let text = String::from_utf8_lossy(&managed.document).into_owned();
                    policies.push(Policy::parse(&text, PolicyKind::Managed).map_err(|err| {
                        corrupt_policy(&format!("managed policy {policy_name}"), err)
                    })?);
                    texts.push(text);
                }
                (
                    Identity::User {
                        account: principal.account.clone(),
                        name: name.clone(),
                        user_id,
                    },
                    policies,
                )
            }
        },
    };
    let mut grants = HashMap::new();
    for bucket in buckets {
        let info = file.bucket(bucket)?;
        let zone = info.as_ref().and_then(|info| info.zone.clone());
        let warehouse = match &info {
            Some(info) => match &info.warehouse {
                Some(warehouse) => {
                    let table_id = bucket.strip_suffix("--table-s3").unwrap_or(bucket);
                    let mut policies = Vec::new();
                    for table in [None, Some(table_id)] {
                        if let Some(document) =
                            file.table_policy((&info.owner, &warehouse.table_bucket), table)?
                        {
                            let text = String::from_utf8_lossy(&document);
                            policies.push(
                                Policy::parse(&text, PolicyKind::Bucket)
                                    .map_err(|err| corrupt_policy("table policy", err))?,
                            );
                        }
                    }
                    Some(WarehouseGrant {
                        table_bucket: warehouse.table_bucket.clone(),
                        table_id: table_id.to_string(),
                        dropped: warehouse.dropped,
                        policies,
                    })
                }
                None => None,
            },
            None => None,
        };
        let created_ms = info.as_ref().map(|info| info.created_ms);
        let owner = info.map(|info| info.owner);
        let policy = match owner {
            Some(_) => file
                .bucket_config(bucket, BUCKET_POLICY, "")?
                .map(|document| {
                    let text = String::from_utf8_lossy(&document).into_owned();
                    let policy = Policy::parse(&text, PolicyKind::Bucket)
                        .map_err(|err| corrupt_policy("bucket policy", err));
                    texts.push(text);
                    policy
                })
                .transpose()?,
            None => None,
        };
        let public_block = match &owner {
            Some(owner) => crate::public_access::effective(file, bucket, owner)?,
            None => 0,
        };
        grants.insert(
            bucket.clone(),
            BucketGrant {
                policy_public: policy.as_ref().is_some_and(truthdb_iam::is_public),
                owner,
                policy,
                public_block,
                zone,
                created_ms,
                warehouse,
            },
        );
    }
    let reads_tags = texts
        .iter()
        .any(|text| text.to_ascii_lowercase().contains("existingobjecttag"));
    let mut existing_tags = HashMap::new();
    if reads_tags {
        for (bucket, key, which) in objects {
            if let Ok(info) = file.head_object(bucket, key, *which)?
                && !info.delete_marker
            {
                existing_tags.insert((bucket.clone(), key.clone()), info.attrs.tags);
            }
        }
    }
    Ok(Loaded {
        identity,
        policies,
        buckets: grants,
        existing_tags,
    })
}

/// Sets `s3:RequestObjectTag/<key>` and `s3:RequestObjectTagKeys`.
pub fn set_request_tags(context: &mut Context, tags: &[(String, String)]) {
    for (key, value) in tags {
        context.set(&format!("s3:RequestObjectTag/{key}"), vec![value.clone()]);
    }
    context.set(
        "s3:RequestObjectTagKeys",
        tags.iter().map(|(key, _)| key.clone()).collect(),
    );
}

/// The request's condition keys: its connection, time, signature, headers
/// and query (only well-formed values: a malformed one is left out).
fn context(
    headers: &HeaderMap,
    raw_query: &str,
    query: &[(String, String)],
    connection: Option<Connection>,
    signed: Option<Signed>,
    now_ms: u64,
) -> Context {
    let mut context = Context::default();
    let mut one = |key: &str, value: String| context.set(key, vec![value]);
    if let Some(connection) = connection {
        one("aws:SecureTransport", connection.tls.to_string());
        one(
            "aws:SourceIp",
            connection.peer.ip().to_canonical().to_string(),
        );
    }
    one("aws:CurrentTime", crate::time::iso8601(now_ms));
    one("aws:EpochTime", (now_ms / 1000).to_string());
    // The signature's keys, for a signed request only (an anonymous one has
    // none: a condition on them sees them absent).
    if let Some(signed) = signed {
        one("s3:authType", signed.auth_type.to_string());
        one("s3:signatureversion", "AWS4-HMAC-SHA256".to_string());
        if let Some(at_ms) = signed.at_ms {
            one("s3:signatureAge", now_ms.saturating_sub(at_ms).to_string());
        }
    }
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    for (key, name) in [
        ("aws:UserAgent", "user-agent"),
        ("aws:Referer", "referer"),
        ("s3:x-amz-acl", "x-amz-acl"),
        ("s3:x-amz-copy-source", "x-amz-copy-source"),
        ("s3:x-amz-metadata-directive", "x-amz-metadata-directive"),
        ("s3:x-amz-storage-class", "x-amz-storage-class"),
        (
            "s3:x-amz-server-side-encryption",
            "x-amz-server-side-encryption",
        ),
        (
            "s3:x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-algorithm",
        ),
        ("s3:x-amz-content-sha256", "x-amz-content-sha256"),
    ] {
        if let Some(value) = header(name) {
            one(key, value.to_string());
        }
    }
    let param = |name: &str| {
        query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    };
    for (key, name) in [("s3:prefix", "prefix"), ("s3:delimiter", "delimiter")] {
        if let Some(value) = param(name) {
            one(key, value);
        }
    }
    if let Some(max_keys) = param("max-keys").and_then(|value| value.parse::<u32>().ok()) {
        one("s3:max-keys", max_keys.to_string());
    }
    // The version as the operation reads it (the raw `versionId=` pair), in
    // its canonical form (`007` is version 7).
    if let Some(version) = raw_query
        .split('&')
        .find_map(|pair| pair.strip_prefix("versionId="))
    {
        let canonical = match version.parse::<u64>() {
            Ok(number) => number.to_string(),
            Err(_) => version.to_string(),
        };
        one("s3:VersionId", canonical);
    }
    context
}

impl Authz {
    /// Loads what a request is authorized against: the principal's policies
    /// and the buckets it names (and `object`'s tags, when a policy reads
    /// them).
    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        state: &S3State,
        principal: Option<&Principal>,
        signed: Option<Signed>,
        headers: &HeaderMap,
        raw_query: &str,
        connection: Option<Connection>,
        buckets: Vec<String>,
        objects: Vec<Named>,
    ) -> Result<Authz, S3Error> {
        let who = principal.cloned();
        let loaded = state
            .engine
            .run_object(move |file| load(file, who.as_ref(), &buckets, &objects))
            .await
            .map_err(|err| S3Error::internal(err.to_string()))?;
        let context = context(
            headers,
            raw_query,
            &crate::listing::query_params(raw_query),
            connection,
            signed,
            crate::objects::now_ms(),
        );
        Ok(Authz {
            identity: loaded.identity,
            policies: loaded.policies,
            buckets: loaded.buckets,
            existing_tags: loaded.existing_tags,
            context,
            region: Some(state.region.clone()),
            session: None,
            via: None,
        })
    }

    /// [`Self::load`] for work done for `principal` outside a request (a
    /// batch job's tasks, as the user that created the job), in the caller's
    /// hold of the storage lock: no request headers or connection count.
    pub fn load_now(
        file: &mut ObjFile<'_>,
        principal: &Principal,
        buckets: Vec<String>,
        objects: Vec<Named>,
        now_ms: u64,
    ) -> Result<Authz, StorageError> {
        let loaded = load(file, Some(principal), &buckets, &objects)?;
        Ok(Authz {
            identity: loaded.identity,
            policies: loaded.policies,
            buckets: loaded.buckets,
            existing_tags: loaded.existing_tags,
            context: context(&HeaderMap::new(), "", &[], None, None, now_ms),
            region: None,
            session: None,
            via: None,
        })
    }

    /// This authorization with condition keys set as the equivalent request
    /// would carry them (for work done outside a request).
    pub fn with_keys(&self, keys: &[(&str, String)]) -> Authz {
        let mut authz = self.clone();
        for (key, value) in keys {
            authz.context.set(key, vec![value.clone()]);
        }
        authz
    }

    /// Checks `action` on `resource` with `tags` as its
    /// `s3:RequestObjectTag/...` keys (a tag set being stored).
    pub fn check_tags(
        &self,
        action: &str,
        resource: Resource<'_>,
        tags: &[(String, String)],
    ) -> Result<(), S3Error> {
        let mut context = self.context.clone();
        set_request_tags(&mut context, tags);
        self.check_with(action, resource, &context)
    }

    /// Checks `action` on `resource` for an operation that stores the tags of
    /// an `x-amz-tagging` header (`tags`: `None` when it stores none): the
    /// tags are its `s3:RequestObjectTag/...`, and storing them also needs
    /// `s3:PutObjectTagging`, as in AWS. `lock` is the Object Lock the write
    /// asks for, in its `s3:object-lock-*` keys.
    pub fn check_storing(
        &self,
        action: &str,
        resource: Resource<'_>,
        tags: Option<&[(String, String)]>,
        lock: Option<&truthdb_object::ObjectLock>,
    ) -> Result<(), S3Error> {
        let mut context = self.context.clone();
        if let Some(tags) = tags {
            set_request_tags(&mut context, tags);
        }
        if let Some(lock) = lock {
            crate::object_lock::set_condition_keys(&mut context, lock, crate::objects::now_ms());
        }
        self.check_with(action, resource, &context)?;
        if tags.is_some() {
            self.check_with("s3:PutObjectTagging", resource, &context)?;
        }
        Ok(())
    }

    /// The owners the request's buckets had when they were loaded (for the
    /// request's scope).
    pub fn owners(&self) -> Vec<(String, Option<String>)> {
        self.buckets
            .iter()
            .map(|(name, grant)| (name.clone(), grant.owner.clone()))
            .collect()
    }

    /// Checks `action` on `resource`: AccessDenied unless the policies allow
    /// it. An operation on a bucket that does not exist passes (the
    /// operation reports it); CreateBucket checks the caller's own policies.
    pub fn check(&self, action: &str, resource: Resource<'_>) -> Result<(), S3Error> {
        self.check_with(action, resource, &self.context)
    }

    /// [`Self::check`] with condition keys the operation found itself (a
    /// PutObjectTagging body's tags).
    pub fn check_with(
        &self,
        action: &str,
        resource: Resource<'_>,
        context: &Context,
    ) -> Result<(), S3Error> {
        // A ReadOnly session changes nothing of its bucket's, whoever made
        // it (not even its policy, which its owner's root otherwise may).
        if self
            .session
            .as_ref()
            .is_some_and(|scope| scope.read_only && BUCKET_WRITES.contains(&action))
        {
            return Err(S3Error::access_denied("Access Denied"));
        }
        let (arn, bucket) = match resource {
            Resource::Service => ("arn:aws:s3:::*".to_string(), None),
            Resource::Bucket(bucket) => (format!("arn:aws:s3:::{bucket}"), Some(bucket)),
            Resource::Object(bucket, key) => (format!("arn:aws:s3:::{bucket}/{key}"), Some(bucket)),
            Resource::Arn(arn) => (arn.to_string(), None),
        };
        // A session's bucket is the very one it was made for (its name,
        // owner and creation): one of its name made since is refused it
        // outright, for every operation.
        if let (Some(scope), Some(bucket)) = (&self.session, bucket)
            && scope.bucket == bucket
            && let Some(grant) = self.buckets.get(bucket)
            && grant.owner.is_some()
            && (grant.owner.as_deref() != Some(scope.owner.as_str())
                || grant.created_ms != Some(scope.created_ms))
        {
            return Err(S3Error::access_denied("Access Denied"));
        }
        // Creating a bucket is the caller's own policies' to allow (one that
        // exists already is the operation's conflict to report).
        let grant = bucket
            .filter(|_| action != "s3:CreateBucket")
            .and_then(|bucket| self.buckets.get(bucket));
        let owner = grant.and_then(|grant| grant.owner.as_deref());
        if grant.is_some() && owner.is_none() {
            return Ok(());
        }
        if let (Some(warehouse), Some(owner)) =
            (grant.and_then(|grant| grant.warehouse.as_ref()), owner)
        {
            return self.check_warehouse(action, owner, warehouse, context);
        }
        // An object's own tags, as `s3:ExistingObjectTag/...`.
        let tagged;
        let context = match resource {
            Resource::Object(bucket, key) => {
                match self
                    .existing_tags
                    .get(&(bucket.to_string(), key.to_string()))
                {
                    Some(tags) => {
                        let mut with = context.clone();
                        for (tag, value) in tags {
                            with.set(&format!("s3:ExistingObjectTag/{tag}"), vec![value.clone()]);
                        }
                        tagged = with;
                        &tagged
                    }
                    None => context,
                }
            }
            _ => context,
        };
        // Through an access point, on its bucket: as both allow.
        if let Some(via) = &self.via
            && bucket == Some(via.point.bucket.as_str())
        {
            return self.check_through(action, &resource, via, grant, context);
        }
        // The owning account's root always manages its bucket's policy, so a
        // policy cannot lock it out.
        if matches!(
            action,
            "s3:GetBucketPolicy" | "s3:PutBucketPolicy" | "s3:DeleteBucketPolicy"
        ) && let (Identity::Root { account }, Some(owner)) = (&self.identity, owner)
            && account == owner
        {
            return Ok(());
        }
        // RestrictPublicBuckets: while the policy is public, nothing outside
        // the owner's account is allowed (only a policy could allow it), and
        // within it the policy's public grants count for no one.
        let restricted = grant.is_some_and(|grant| {
            grant.policy_public
                && grant.public_block & crate::public_access::RESTRICT_PUBLIC_BUCKETS != 0
        });
        if restricted && self.identity.account() != owner {
            return Err(S3Error::access_denied("Access Denied"));
        }
        let unrestricted;
        let policy = match grant.and_then(|grant| grant.policy.as_ref()) {
            Some(policy) if restricted => {
                unrestricted = truthdb_iam::without_public_grants(policy);
                Some(&unrestricted)
            }
            policy => policy,
        };
        if let (Some(grant), Some(owner), Some(bucket)) = (grant, owner, bucket)
            && grant.zone.is_some()
        {
            return self.check_directory(action, bucket, owner, policy, context);
        }
        self.decide(action, &arn, owner, policy, context)
    }

    /// An S3 Tables table's storage bucket's check: reading its files
    /// (GetObject, HeadObject, ListParts) is `s3tables:GetTableData` on its
    /// table, writing them (PutObject and the multipart uploads') is
    /// `s3tables:PutTableData`, with its table bucket's and table's
    /// policies; nothing else is allowed on it. Once its table is deleted,
    /// it is not there.
    fn check_warehouse(
        &self,
        action: &str,
        owner: &str,
        warehouse: &WarehouseGrant,
        context: &Context,
    ) -> Result<(), S3Error> {
        if warehouse.dropped {
            let bucket = format!("{}--table-s3", warehouse.table_id);
            return Err(crate::api::bucket_error(
                truthdb_object::BucketError::NoSuchBucket,
                &bucket,
            ));
        }
        let table_action = match action {
            "s3:GetObject" | "s3:ListMultipartUploadParts" => "s3tables:GetTableData",
            "s3:PutObject" | "s3:AbortMultipartUpload" => "s3tables:PutTableData",
            _ => return Err(S3Error::access_denied("Access Denied")),
        };
        let Some(region) = &self.region else {
            return Err(S3Error::access_denied("Access Denied"));
        };
        let arn = format!(
            "arn:aws:s3tables:{region}:{owner}:bucket/{}/table/{}",
            warehouse.table_bucket, warehouse.table_id
        );
        let policies: Vec<&Policy> = warehouse.policies.iter().collect();
        self.decide_all(table_action, &arn, Some(owner), &policies, context)
    }

    /// A directory bucket's check: its bucket operations are S3 Express's
    /// actions on `arn:aws:s3express:<region>:<owner>:bucket/<name>`, and
    /// its object operations are what a session allows, so
    /// `s3express:CreateSession` of a mode that allows them (`ReadOnly`
    /// allows the reads; `ReadWrite`, everything).
    fn check_directory(
        &self,
        action: &str,
        bucket: &str,
        owner: &str,
        policy: Option<&Policy>,
        context: &Context,
    ) -> Result<(), S3Error> {
        let Some(region) = &self.region else {
            return Err(S3Error::access_denied("Access Denied"));
        };
        let arn = format!("arn:aws:s3express:{region}:{owner}:bucket/{bucket}");
        const BUCKET_ACTIONS: [&str; 11] = [
            "s3:CreateBucket",
            "s3:DeleteBucket",
            "s3:PutBucketPolicy",
            "s3:GetBucketPolicy",
            "s3:DeleteBucketPolicy",
            "s3:PutEncryptionConfiguration",
            "s3:GetEncryptionConfiguration",
            "s3:PutInventoryConfiguration",
            "s3:GetInventoryConfiguration",
            "s3:PutLifecycleConfiguration",
            "s3:GetLifecycleConfiguration",
        ];
        const READS: [&str; 7] = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:GetObjectAttributes",
            "s3:GetObjectVersionAttributes",
            "s3:ListBucket",
            "s3:ListBucketMultipartUploads",
            "s3:ListMultipartUploadParts",
        ];
        if BUCKET_ACTIONS.contains(&action) {
            let express = action.replacen("s3:", "s3express:", 1);
            return self.decide(&express, &arn, Some(owner), policy, context);
        }
        // A new session: its mode is the context's (never a session's own).
        if action == "s3express:CreateSession" {
            return self.decide(action, &arn, Some(owner), policy, context);
        }
        // A session of this bucket (the very one: see `check_with`) allows
        // what its mode does: S3 Express authorizes its requests at
        // CreateSession.
        if let Some(scope) = &self.session
            && scope.bucket == bucket
        {
            return match !scope.read_only || READS.contains(&action) {
                true => Ok(()),
                false => Err(S3Error::access_denied("Access Denied")),
            };
        }
        let modes: &[&str] = if READS.contains(&action) {
            &["ReadWrite", "ReadOnly"]
        } else {
            &["ReadWrite"]
        };
        let mut denied = Err(S3Error::access_denied("Access Denied"));
        for mode in modes {
            let mut with = context.clone();
            with.set("s3express:SessionMode", vec![mode.to_string()]);
            denied = self.decide("s3express:CreateSession", &arn, Some(owner), policy, &with);
            if denied.is_ok() {
                break;
            }
        }
        denied
    }

    /// Checks that the caller may create a session of directory bucket
    /// `bucket`, ReadOnly or ReadWrite (`s3express:CreateSession`, with its
    /// `s3express:SessionMode`).
    pub fn check_session(&self, bucket: &str, read_only: bool) -> Result<(), S3Error> {
        let mut context = self.context.clone();
        let mode = if read_only { "ReadOnly" } else { "ReadWrite" };
        context.set("s3express:SessionMode", vec![mode.to_string()]);
        self.check_with(
            "s3express:CreateSession",
            Resource::Bucket(bucket),
            &context,
        )
    }

    /// This authorization for a request made through access point `via`.
    pub(crate) fn through(mut self, via: crate::access_points::Via) -> Authz {
        self.via = Some(via);
        self
    }

    /// A request through access point `via` on its bucket: allowed when
    /// both allow it ("both the access point and the underlying bucket ...
    /// must permit the request"):
    /// - the access point's side: the caller's own policies and the access
    ///   point's policy, on the access point's ARN (`.../object/<key>` for
    ///   an object), weighed as a resource of the access point's account;
    /// - the bucket's side: its policy on the bucket's ARN (a grant to an
    ///   account counting only as that account's policies allow the
    ///   access point's request), or its owner's root when the policy does
    ///   not deny it.
    ///
    /// Both see `s3:DataAccessPointArn`, `s3:DataAccessPointAccount` and
    /// `s3:AccessPointNetworkOrigin`; RestrictPublicBuckets of the access
    /// point, the bucket or its owner's account restricts both.
    fn check_through(
        &self,
        action: &str,
        resource: &Resource<'_>,
        via: &crate::access_points::Via,
        grant: Option<&BucketGrant>,
        context: &Context,
    ) -> Result<(), S3Error> {
        let denied = || S3Error::access_denied("Access Denied");
        let (Some(region), Some(grant)) = (&self.region, grant) else {
            return Err(denied());
        };
        let owner = grant.owner.as_deref();
        let point_arn = via.arn(region);
        let (through_arn, bucket_arn) = match resource {
            Resource::Bucket(bucket) => (point_arn.clone(), format!("arn:aws:s3:::{bucket}")),
            Resource::Object(bucket, key) => (
                format!("{point_arn}/object/{key}"),
                format!("arn:aws:s3:::{bucket}/{key}"),
            ),
            _ => return Err(denied()),
        };
        let mut context = context.clone();
        context.set("s3:DataAccessPointArn", vec![point_arn]);
        context.set("s3:DataAccessPointAccount", vec![via.point.account.clone()]);
        context.set("s3:AccessPointNetworkOrigin", vec!["Internet".to_string()]);
        let restrict = via.point.block_public[3]
            || grant.public_block & crate::public_access::RESTRICT_PUBLIC_BUCKETS != 0;
        // The access point's side.
        let point_public = via.policy.as_ref().is_some_and(truthdb_iam::is_public);
        if restrict && point_public && self.identity.account() != Some(via.point.account.as_str()) {
            return Err(denied());
        }
        let unrestricted;
        let point_policies: Vec<&Policy> = match via.policy.as_ref() {
            Some(policy) if restrict && point_public => {
                unrestricted = truthdb_iam::without_public_grants(policy);
                vec![&unrestricted]
            }
            policy => policy.into_iter().collect(),
        };
        self.decide_all(
            action,
            &through_arn,
            Some(&via.point.account),
            &point_policies,
            &context,
        )?;
        // The bucket's side.
        if restrict && grant.policy_public && self.identity.account() != owner {
            return Err(denied());
        }
        let bucket_unrestricted;
        let bucket_policies: Vec<&Policy> = match grant.policy.as_ref() {
            Some(policy) if restrict && grant.policy_public => {
                bucket_unrestricted = truthdb_iam::without_public_grants(policy);
                vec![&bucket_unrestricted]
            }
            policy => policy.into_iter().collect(),
        };
        if let Some(owner) = owner {
            context.set("s3:ResourceAccount", vec![owner.to_string()]);
        }
        let policies: Vec<&Policy> = self.policies.iter().collect();
        let identity_allows = truthdb_iam::evaluate_identity(
            &truthdb_iam::Request {
                identity: &self.identity,
                action,
                resource: &through_arn,
                context: &context,
            },
            &policies,
        ) == Decision::Allow;
        match truthdb_iam::evaluate_resource(
            &truthdb_iam::Request {
                identity: &self.identity,
                action,
                resource: &bucket_arn,
                context: &context,
            },
            &bucket_policies,
            identity_allows,
        ) {
            Decision::Allow => Ok(()),
            Decision::ExplicitDeny => Err(denied()),
            // Its owner's root, unless the policy denies it.
            _ if matches!(&self.identity, Identity::Root { account } if Some(account.as_str()) == owner) => {
                Ok(())
            }
            _ => Err(denied()),
        }
    }

    /// This authorization for a request signed with session `scope`.
    pub fn with_session(mut self, scope: crate::session::Scope) -> Authz {
        self.session = Some(scope);
        self
    }

    /// The owner and creation time of `bucket`, if it exists.
    pub fn incarnation(&self, bucket: &str) -> Option<(String, u64)> {
        let grant = self.buckets.get(bucket)?;
        Some((grant.owner.clone()?, grant.created_ms?))
    }

    /// The zone of `bucket` if it is a directory bucket the request names.
    pub fn zone(&self, bucket: &str) -> Option<&str> {
        self.buckets
            .get(bucket)
            .and_then(|grant| grant.zone.as_deref())
    }

    /// Checks `action` on the resource `arn` of a bucket `owner` owns, which
    /// has no bucket policy (an S3 Vectors bucket).
    pub fn check_arn(&self, action: &str, arn: &str, owner: &str) -> Result<(), S3Error> {
        self.decide(action, arn, Some(owner), None, &self.context)
    }

    /// [`Authz::check_arn`] on a resource with its own policies (an S3
    /// Tables table bucket's, and a table's), all weighed.
    pub fn check_arn_with(
        &self,
        action: &str,
        arn: &str,
        owner: &str,
        resource_policies: &[&Policy],
    ) -> Result<(), S3Error> {
        // The owning account's root always manages its policies (as a
        // bucket's), so a policy cannot lock it out.
        if matches!(
            action,
            "s3tables:GetTableBucketPolicy"
                | "s3tables:PutTableBucketPolicy"
                | "s3tables:DeleteTableBucketPolicy"
                | "s3tables:GetTablePolicy"
                | "s3tables:PutTablePolicy"
                | "s3tables:DeleteTablePolicy"
        ) && matches!(&self.identity, Identity::Root { account } if account == owner)
        {
            return Ok(());
        }
        self.decide_all(action, arn, Some(owner), resource_policies, &self.context)
    }

    fn decide(
        &self,
        action: &str,
        arn: &str,
        owner: Option<&str>,
        bucket_policy: Option<&Policy>,
        context: &Context,
    ) -> Result<(), S3Error> {
        let resource_policies: Vec<&Policy> = bucket_policy.into_iter().collect();
        self.decide_all(action, arn, owner, &resource_policies, context)
    }

    fn decide_all(
        &self,
        action: &str,
        arn: &str,
        owner: Option<&str>,
        resource_policies: &[&Policy],
        context: &Context,
    ) -> Result<(), S3Error> {
        let mut context = context.clone();
        if let Some(owner) = owner {
            context.set("s3:ResourceAccount", vec![owner.to_string()]);
        }
        let request = truthdb_iam::Request {
            identity: &self.identity,
            action,
            resource: arn,
            context: &context,
        };
        let policies: Vec<&Policy> = self.policies.iter().collect();
        match evaluate_all(&request, &policies, owner, resource_policies) {
            Decision::Allow => Ok(()),
            Decision::ImplicitDeny | Decision::ExplicitDeny => {
                Err(S3Error::access_denied("Access Denied"))
            }
        }
    }

    /// The request's condition keys, for an operation to add to.
    pub fn context(&self) -> &Context {
        &self.context
    }
}
