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
use truthdb_iam::{Context, Decision, Identity, Policy, PolicyKind, evaluate};
use truthdb_object::{BUCKET_POLICY, ObjFile, VersionSel};

use crate::error::S3Error;
use crate::server::{Connection, Principal, S3State};

/// A bucket as authorization sees it.
#[derive(Debug, Clone)]
pub struct BucketGrant {
    /// The owning account; `None` when the bucket does not exist.
    pub owner: Option<String>,
    pub policy: Option<Policy>,
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
}

/// The resource an operation acts on.
#[derive(Debug, Clone, Copy)]
pub enum Resource<'a> {
    /// The service: ListBuckets.
    Service,
    Bucket(&'a str),
    Object(&'a str, &'a str),
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
    principal: &Principal,
    buckets: &[String],
    objects: &[Named],
) -> Result<Loaded, StorageError> {
    let mut texts = Vec::new();
    let (identity, policies) = match &principal.user {
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
                policies.push(
                    Policy::parse(&text, PolicyKind::UserInline).map_err(|err| {
                        corrupt_policy(&format!("user policy {policy_name}"), err)
                    })?,
                );
                texts.push(text);
            }
            for policy_name in file.attached_policies(&principal.account, name)? {
                // A policy detached meanwhile is gone: it grants nothing.
                let Some(managed) = file.managed_policy(&principal.account, &policy_name)? else {
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
    };
    let mut grants = HashMap::new();
    for bucket in buckets {
        let owner = file.bucket(bucket)?.map(|info| info.owner);
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
        grants.insert(bucket.clone(), BucketGrant { owner, policy });
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
    amz_date: Option<&str>,
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
    one("s3:authType", "REST-HEADER".to_string());
    one("s3:signatureversion", "AWS4-HMAC-SHA256".to_string());
    if let Some(signed) = amz_date.and_then(crate::server::parse_amz_date) {
        one(
            "s3:signatureAge",
            now_ms.saturating_sub(signed * 1000).to_string(),
        );
    }
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
    for (key, name) in [
        ("aws:UserAgent", "user-agent"),
        ("aws:Referer", "referer"),
        ("s3:x-amz-acl", "x-amz-acl"),
        ("s3:x-amz-copy-source", "x-amz-copy-source"),
        ("s3:x-amz-metadata-directive", "x-amz-metadata-directive"),
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
        principal: &Principal,
        headers: &HeaderMap,
        raw_query: &str,
        connection: Option<Connection>,
        buckets: Vec<String>,
        objects: Vec<Named>,
    ) -> Result<Authz, S3Error> {
        let who = principal.clone();
        let loaded = state
            .engine
            .run_object(move |file| load(file, &who, &buckets, &objects))
            .await
            .map_err(|err| S3Error::internal(err.to_string()))?;
        let context = context(
            headers,
            raw_query,
            &crate::listing::query_params(raw_query),
            connection,
            headers
                .get("x-amz-date")
                .and_then(|value| value.to_str().ok()),
            crate::objects::now_ms(),
        );
        Ok(Authz {
            identity: loaded.identity,
            policies: loaded.policies,
            buckets: loaded.buckets,
            existing_tags: loaded.existing_tags,
            context,
        })
    }

    /// Checks `action` on `resource` for an operation that stores the tags of
    /// an `x-amz-tagging` header (`tags`: `None` when it stores none): the
    /// tags are its `s3:RequestObjectTag/...`, and storing them also needs
    /// `s3:PutObjectTagging`, as in AWS.
    pub fn check_storing(
        &self,
        action: &str,
        resource: Resource<'_>,
        tags: Option<&[(String, String)]>,
    ) -> Result<(), S3Error> {
        let mut context = self.context.clone();
        if let Some(tags) = tags {
            set_request_tags(&mut context, tags);
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
        let (arn, bucket) = match resource {
            Resource::Service => ("arn:aws:s3:::*".to_string(), None),
            Resource::Bucket(bucket) => (format!("arn:aws:s3:::{bucket}"), Some(bucket)),
            Resource::Object(bucket, key) => (format!("arn:aws:s3:::{bucket}/{key}"), Some(bucket)),
        };
        // Creating a bucket is the caller's own policies' to allow (one that
        // exists already is the operation's conflict to report).
        let grant = bucket
            .filter(|_| action != "s3:CreateBucket")
            .and_then(|bucket| self.buckets.get(bucket));
        let owner = grant.and_then(|grant| grant.owner.as_deref());
        if grant.is_some() && owner.is_none() {
            return Ok(());
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
        self.decide(
            action,
            &arn,
            owner,
            grant.and_then(|grant| grant.policy.as_ref()),
            context,
        )
    }

    /// Checks `action` on the resource `arn` of a bucket `owner` owns, which
    /// has no bucket policy (an S3 Vectors bucket).
    pub fn check_arn(&self, action: &str, arn: &str, owner: &str) -> Result<(), S3Error> {
        self.decide(action, arn, Some(owner), None, &self.context)
    }

    fn decide(
        &self,
        action: &str,
        arn: &str,
        owner: Option<&str>,
        bucket_policy: Option<&Policy>,
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
        match evaluate(&request, &policies, owner, bucket_policy) {
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
