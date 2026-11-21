//! A bucket's replication configuration: PUT, GET and DELETE
//! `?replication`, validated as S3 validates it. The rules' destination is
//! a bucket of the same owner on this server, with versioning enabled (as
//! the source must have it, and keeps while the configuration is set), and
//! with Object Lock when the source has it. Rules filter by key prefix.
//! What later replication steps build is refused rather than accepted and
//! ignored: tag filters, rules without a Filter (whose delete markers
//! replicate), delete-marker replication, several destinations, another
//! account's or another server's bucket, replica ownership, KMS encryption,
//! replication time and metrics, and existing-object replication.
//!
//! The `Role` is kept as given: there are no IAM roles, so replication acts
//! as the source bucket's owner.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::{BUCKET_REPLICATION, StorageClass, Versioning};

use crate::error::{S3Error, escape};
use crate::object_lock::{malformed, read_checked_body};
use crate::server::{Authenticated, Body, S3State, full};
use crate::xml::Element;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
/// A configuration of 1,000 rules fits.
const MAX_BODY: usize = 2 * 1024 * 1024;
const MAX_RULES: usize = 1000;
const MAX_ID: usize = 255;
const BUCKET_ARN: &str = "arn:aws:s3:::";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationConfig {
    pub role: String,
    pub rules: Vec<ReplicationRule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationRule {
    /// Empty when the rule has none.
    pub id: String,
    pub priority: u32,
    pub enabled: bool,
    /// The keys it applies to: those under this prefix (none: every key).
    pub prefix: Option<String>,
    pub destination: Destination,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    pub bucket: String,
    pub account: Option<String>,
    /// The replicas' class (none: the source version's).
    pub storage_class: Option<StorageClass>,
}

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

fn not_yet(what: &str) -> S3Error {
    S3Error::not_implemented(format!("{what} is not implemented"))
}

/// The text of the child named `name`, which holds no elements.
fn text<'a>(element: &'a Element, name: &str) -> Result<Option<&'a str>, S3Error> {
    match element.one(name)? {
        Some(child) if !child.children.is_empty() => Err(malformed()),
        child => Ok(child.map(|child| child.text.as_str())),
    }
}

fn status(element: &Element) -> Result<bool, S3Error> {
    match element.text_of("Status")? {
        Some("Enabled") => Ok(true),
        Some("Disabled") => Ok(false),
        _ => Err(malformed()),
    }
}

fn destination(element: &Element) -> Result<Destination, S3Error> {
    element.only(&[
        "Bucket",
        "Account",
        "StorageClass",
        "AccessControlTranslation",
        "EncryptionConfiguration",
        "ReplicationTime",
        "Metrics",
    ])?;
    let arn = element.text_of("Bucket")?.ok_or_else(malformed)?;
    let bucket = arn
        .strip_prefix(BUCKET_ARN)
        .filter(|name| !name.is_empty() && !name.contains('/'))
        .ok_or_else(|| invalid(format!("Invalid destination bucket ARN: {arn}")))?
        .to_string();
    let account = element.text_of("Account")?.map(str::to_string);
    if account.as_deref() == Some("") {
        return Err(invalid(
            "The <Account> element is empty. It must contain a valid account ID.",
        ));
    }
    if element.one("AccessControlTranslation")?.is_some() {
        if account.is_none() {
            return Err(S3Error::invalid_request(
                "The <Owner> in <AccessControlTranslation> has a value, so the <Account> \
                 element must be specified.",
            ));
        }
        return Err(not_yet("AccessControlTranslation"));
    }
    if element.one("EncryptionConfiguration")?.is_some() {
        return Err(not_yet(
            "Replication with KMS keys (EncryptionConfiguration)",
        ));
    }
    if element.one("ReplicationTime")?.is_some() || element.one("Metrics")?.is_some() {
        return Err(not_yet("Replication Time Control and replication metrics"));
    }
    let storage_class = element
        .text_of("StorageClass")?
        .map(|name| crate::objects::storage_class(Some(name)))
        .transpose()?;
    Ok(Destination {
        bucket,
        account,
        storage_class,
    })
}

fn rule(element: &Element) -> Result<ReplicationRule, S3Error> {
    element.only(&[
        "ID",
        "Priority",
        "Status",
        "Filter",
        "Prefix",
        "DeleteMarkerReplication",
        "Destination",
        "SourceSelectionCriteria",
        "ExistingObjectReplication",
    ])?;
    let id = text(element, "ID")?.unwrap_or("").to_string();
    if id.chars().count() > MAX_ID {
        return Err(invalid("ID length should not exceed allowed limit of 255"));
    }
    if !id.is_ascii() {
        return Err(invalid("Rule ID must not contain non-ASCII characters"));
    }
    let enabled = status(element)?;
    // A rule without a Filter (the earlier schema, with an optional rule
    // Prefix) replicates delete markers.
    let filter = match (element.one("Filter")?, element.one("Prefix")?) {
        (Some(filter), None) => filter,
        (None, _) => return Err(not_yet("A rule without a Filter")),
        (Some(_), Some(_)) => return Err(malformed()),
    };
    filter.only(&["Prefix", "Tag", "And"])?;
    if filter.children.len() > 1 {
        return Err(malformed());
    }
    let prefix = match filter.children.first() {
        None => None,
        Some(only) if only.name == "Prefix" => {
            if !only.children.is_empty() {
                return Err(malformed());
            }
            Some(only.text.clone())
        }
        Some(_) => return Err(not_yet("Filtering replication by tags")),
    };
    let priority = element.text_of("Priority")?.ok_or_else(|| {
        S3Error::invalid_request(
            "Priority must be specified for this version of Cross Region Replication \
             configuration schema.",
        )
    })?;
    if priority.is_empty() || !priority.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed());
    }
    let priority = priority
        .parse()
        .map_err(|_| invalid(format!("Priority {priority} is out of range")))?;
    let markers = element.one("DeleteMarkerReplication")?.ok_or_else(|| {
        S3Error::invalid_request(
            "DeleteMarkerReplication must be specified for this version of Cross Region \
             Replication configuration schema.",
        )
    })?;
    markers.only(&["Status"])?;
    if status(markers)? {
        return Err(not_yet("Delete marker replication"));
    }
    if element.one("SourceSelectionCriteria")?.is_some() {
        return Err(not_yet("SourceSelectionCriteria"));
    }
    if element.one("ExistingObjectReplication")?.is_some() {
        return Err(not_yet("ExistingObjectReplication"));
    }
    let destination = destination(element.one("Destination")?.ok_or_else(malformed)?)?;
    Ok(ReplicationRule {
        id,
        priority,
        enabled,
        prefix,
        destination,
    })
}

/// Parses a PutBucketReplication body.
pub fn parse(body: &[u8]) -> Result<ReplicationConfig, S3Error> {
    let root = crate::xml::parse(body, "ReplicationConfiguration")?;
    root.only(&["Role", "Rule"])?;
    let role = root
        .text_of("Role")?
        .filter(|role| !role.is_empty())
        .ok_or_else(malformed)?
        .to_string();
    let rules = root.all("Rule").map(rule).collect::<Result<Vec<_>, _>>()?;
    if rules.is_empty() || rules.len() > MAX_RULES {
        return Err(malformed());
    }
    let mut ids: Vec<&str> = rules
        .iter()
        .map(|rule| rule.id.as_str())
        .filter(|id| !id.is_empty())
        .collect();
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid(
            "Rule ID must be unique. Found same ID for more than one rule",
        ));
    }
    if rules
        .iter()
        .any(|rule| rule.destination.bucket != rules[0].destination.bucket)
    {
        return Err(not_yet("Replication to more than one destination"));
    }
    Ok(ReplicationConfig { role, rules })
}

/// The configuration as GetBucketReplication answers it (as it is also
/// stored).
pub fn render(config: &ReplicationConfig) -> String {
    let mut out = format!(
        "<ReplicationConfiguration xmlns=\"{XMLNS}\"><Role>{}</Role>",
        escape(&config.role)
    );
    for rule in &config.rules {
        out.push_str("<Rule>");
        if !rule.id.is_empty() {
            out.push_str(&format!("<ID>{}</ID>", escape(&rule.id)));
        }
        out.push_str(&format!("<Priority>{}</Priority>", rule.priority));
        out.push_str(&format!(
            "<Status>{}</Status>",
            if rule.enabled { "Enabled" } else { "Disabled" }
        ));
        match &rule.prefix {
            None => out.push_str("<Filter></Filter>"),
            Some(prefix) => out.push_str(&format!(
                "<Filter><Prefix>{}</Prefix></Filter>",
                escape(prefix)
            )),
        }
        out.push_str(
            "<DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>",
        );
        let destination = &rule.destination;
        out.push_str(&format!(
            "<Destination><Bucket>{BUCKET_ARN}{}</Bucket>",
            escape(&destination.bucket)
        ));
        if let Some(account) = &destination.account {
            out.push_str(&format!("<Account>{}</Account>", escape(account)));
        }
        if let Some(class) = destination.storage_class {
            out.push_str(&format!("<StorageClass>{}</StorageClass>", class.as_str()));
        }
        out.push_str("</Destination></Rule>");
    }
    out.push_str("</ReplicationConfiguration>");
    out
}

/// Whether `bucket`'s replication configuration, if it has one, names a
/// destination without Object Lock or not of `owner` (the bucket's):
/// enabling Object Lock on the bucket is then refused, as the configuration
/// could not be put with it. Another account's bucket blocks whatever its
/// state, which its owner is not told.
pub(crate) fn blocks_object_lock(
    file: &mut truthdb_object::ObjFile<'_>,
    bucket: &str,
    owner: &str,
) -> Result<bool, truthdb_core::storage::StorageError> {
    let Some(document) = file.bucket_config(bucket, BUCKET_REPLICATION, "")? else {
        return Ok(false);
    };
    // A stored configuration is one this module rendered.
    let Ok(config) = parse(&document) else {
        return Ok(true);
    };
    for rule in &config.rules {
        match file.bucket(&rule.destination.bucket)? {
            Some(info) if info.object_lock && info.owner == owner => {}
            _ => return Ok(true),
        }
    }
    Ok(false)
}

/// Why the destination of `config`, for source `source` (of `owner`, with
/// Object Lock or not), cannot be replicated to, if it cannot.
fn refused_destination(
    file: &mut truthdb_object::ObjFile<'_>,
    source: &str,
    owner: &str,
    source_lock: bool,
    config: &ReplicationConfig,
) -> Result<Option<S3Error>, truthdb_core::storage::StorageError> {
    let destination = &config.rules[0].destination;
    if destination.bucket == source {
        return Ok(Some(S3Error::invalid_request(
            "Destination bucket cannot be the same as the source bucket",
        )));
    }
    let Some(info) = file.bucket(&destination.bucket)? else {
        return Ok(Some(S3Error::invalid_request(
            "Destination bucket must exist",
        )));
    };
    // Whose bucket it is, before the Account is compared with its owner:
    // another account's owner is not told.
    if info.owner != owner {
        return Ok(Some(not_yet("Replication to another account's bucket")));
    }
    if let Some(account) = &destination.account
        && *account != info.owner
    {
        return Ok(Some(invalid(
            "The AWS account specified in the <Account> element must match the destination \
             bucket owner.",
        )));
    }
    if info.versioning != Versioning::Enabled {
        return Ok(Some(S3Error::invalid_request(
            "Destination bucket must have versioning enabled.",
        )));
    }
    if source_lock && !info.object_lock {
        return Ok(Some(S3Error::invalid_request(
            "Object Lock is enabled on the source bucket, so it must be enabled on the \
             destination bucket",
        )));
    }
    Ok(None)
}

fn not_found() -> S3Error {
    S3Error::new(
        404,
        "ReplicationConfigurationNotFoundError",
        "There is no replication configuration for this bucket.",
    )
}

/// PUT, GET or DELETE `?replication` on `bucket`.
pub async fn bucket_replication(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    let no_bucket =
        || crate::objects::object_error(truthdb_object::ObjectError::NoSuchBucket, &bucket, "");
    match method {
        Method::GET => {
            let found = crate::scope::object(state, move |file| {
                Ok(match file.bucket(&name)? {
                    None => Err(()),
                    Some(_) => Ok(file.bucket_config(&name, BUCKET_REPLICATION, "")?),
                })
            })
            .await?
            .map_err(|()| no_bucket())?;
            let document = found.ok_or_else(not_found)?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(full(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{}",
                    String::from_utf8_lossy(&document)
                )))
                .expect("a valid response"))
        }
        Method::PUT => {
            let body = read_checked_body(request, auth, MAX_BODY).await?;
            let config = parse(&body)?;
            // The buckets are checked and the configuration set in one
            // transaction: versioning cannot be suspended in between.
            crate::scope::object(state, move |file| {
                let Some(info) = file.bucket(&name)? else {
                    return Ok(Err(None));
                };
                if info.versioning != Versioning::Enabled {
                    return Ok(Err(Some(S3Error::invalid_request(
                        "Versioning must be 'Enabled' on the bucket to apply a replication \
                         configuration",
                    ))));
                }
                if let Some(refused) =
                    refused_destination(file, &name, &info.owner, info.object_lock, &config)?
                {
                    return Ok(Err(Some(refused)));
                }
                file.transaction(|tx| {
                    tx.put_bucket_config(&name, BUCKET_REPLICATION, "", render(&config).as_bytes())
                })?;
                Ok(Ok(()))
            })
            .await?
            .map_err(|refused| refused.unwrap_or_else(no_bucket))?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(full(bytes::Bytes::new()))
                .expect("a valid response"))
        }
        Method::DELETE => {
            crate::scope::object(state, move |file| {
                if file.bucket(&name)?.is_none() {
                    return Ok(Err(()));
                }
                file.transaction(|tx| tx.delete_bucket_config(&name, BUCKET_REPLICATION, ""))?;
                Ok(Ok(()))
            })
            .await?
            .map_err(|()| no_bucket())?;
            Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(full(bytes::Bytes::new()))
                .expect("a valid response"))
        }
        other => Err(S3Error::not_implemented(format!("{other} ?replication"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(rules: &str) -> Result<ReplicationConfig, S3Error> {
        parse(
            format!(
                "<ReplicationConfiguration xmlns=\"{XMLNS}\">\
                 <Role>arn:aws:iam::123456789012:role/replication</Role>{rules}\
                 </ReplicationConfiguration>"
            )
            .as_bytes(),
        )
    }

    fn rule_with(inner: &str) -> String {
        format!(
            "<Rule><Status>Enabled</Status><Priority>1</Priority>{inner}\
             <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
             <Destination><Bucket>arn:aws:s3:::copies</Bucket></Destination></Rule>"
        )
    }

    fn code(rules: &str) -> (u16, String) {
        let error = config(rules).expect_err("refused");
        (error.status, error.code.to_string())
    }

    #[test]
    fn a_configuration_reads_and_renders_back_alike() {
        let rules = "<Rule><ID>logs</ID><Priority>2</Priority><Status>Enabled</Status>\
             <Filter><Prefix>logs/ &amp; more</Prefix></Filter>\
             <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
             <Destination><Bucket>arn:aws:s3:::copies</Bucket><Account>000000000001</Account>\
             <StorageClass>STANDARD_IA</StorageClass></Destination></Rule>\
             <Rule><Priority>1</Priority><Status>Disabled</Status><Filter/>\
             <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
             <Destination><Bucket>arn:aws:s3:::copies</Bucket></Destination></Rule>";
        let parsed = config(rules).unwrap();
        assert_eq!(parsed.role, "arn:aws:iam::123456789012:role/replication");
        assert_eq!(
            parsed.rules[0],
            ReplicationRule {
                id: "logs".into(),
                priority: 2,
                enabled: true,
                prefix: Some("logs/ & more".into()),
                destination: Destination {
                    bucket: "copies".into(),
                    account: Some("000000000001".into()),
                    storage_class: Some(StorageClass::StandardIa),
                },
            }
        );
        assert_eq!(parsed.rules[1].prefix, None, "an empty Filter: every key");
        assert!(!parsed.rules[1].enabled);
        assert_eq!(parse(render(&parsed).as_bytes()), Ok(parsed));
    }

    #[test]
    fn configurations_are_checked_as_s3_checks_them() {
        let malformed = (400, "MalformedXML".to_string());
        let invalid = (400, "InvalidArgument".to_string());
        let request = (400, "InvalidRequest".to_string());
        let unbuilt = (501, "NotImplemented".to_string());
        assert_eq!(code(""), malformed, "no rule");
        let role_less = parse(
            format!(
                "<ReplicationConfiguration xmlns=\"{XMLNS}\">{}</ReplicationConfiguration>",
                rule_with("<Filter/>")
            )
            .as_bytes(),
        );
        assert_eq!(role_less.unwrap_err().code, "MalformedXML", "no Role");
        let many = rule_with("<Filter/>").repeat(MAX_RULES + 1);
        assert_eq!(code(&many), malformed, "more than 1,000 rules");
        assert!(config(&rule_with("<Filter/>").repeat(MAX_RULES)).is_ok());
        for (rules, expected) in [
            (
                "<Rule><Priority>1</Priority><Filter/><Destination><Bucket>arn:aws:s3:::copies\
                 </Bucket></Destination><DeleteMarkerReplication><Status>Disabled</Status>\
                 </DeleteMarkerReplication></Rule>"
                    .to_string(),
                malformed.clone(),
            ),
            (rule_with("<Filter/><Other/>"), malformed.clone()),
            (
                rule_with("<Filter><Prefix>a</Prefix><Prefix>b</Prefix></Filter>"),
                malformed.clone(),
            ),
            (rule_with("<Filter/><Prefix>a</Prefix>"), malformed.clone()),
            // A text element holding elements is not taken for empty text.
            (
                rule_with(
                    "<Filter><Prefix><Tag><Key>a</Key><Value>b</Value></Tag></Prefix></Filter>",
                ),
                malformed.clone(),
            ),
            (rule_with("<ID><x/></ID><Filter/>"), malformed.clone()),
            (
                rule_with(&format!("<ID>{}</ID><Filter/>", "i".repeat(MAX_ID + 1))),
                invalid.clone(),
            ),
            (rule_with("<ID>r\u{e9}gle</ID><Filter/>"), invalid.clone()),
            (
                format!(
                    "{}{}",
                    rule_with("<ID>same</ID><Filter/>"),
                    rule_with("<ID>same</ID><Filter/>")
                ),
                invalid.clone(),
            ),
            (
                "<Rule><Status>Enabled</Status><Filter/><DeleteMarkerReplication><Status>\
                 Disabled</Status></DeleteMarkerReplication><Destination><Bucket>\
                 arn:aws:s3:::copies</Bucket></Destination></Rule>"
                    .to_string(),
                request.clone(),
            ),
            (
                "<Rule><Status>Enabled</Status><Priority>1</Priority><Filter/><Destination>\
                 <Bucket>arn:aws:s3:::copies</Bucket></Destination></Rule>"
                    .to_string(),
                request.clone(),
            ),
            (
                "<Rule><Status>Enabled</Status><Priority>-1</Priority><Filter/>\
                 <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
                 <Destination><Bucket>arn:aws:s3:::copies</Bucket></Destination></Rule>"
                    .to_string(),
                malformed.clone(),
            ),
            // Only rules with a Filter (the earlier schema replicates delete
            // markers).
            (
                "<Rule><Status>Enabled</Status><Prefix>a</Prefix><Destination><Bucket>\
                 arn:aws:s3:::copies</Bucket></Destination></Rule>"
                    .to_string(),
                unbuilt.clone(),
            ),
            (
                rule_with("<Filter><Tag><Key>a</Key><Value>b</Value></Tag></Filter>"),
                unbuilt.clone(),
            ),
            (
                rule_with("<Filter><And><Prefix>a</Prefix></And></Filter>"),
                unbuilt.clone(),
            ),
            (
                rule_with(
                    "<Filter/><SourceSelectionCriteria><ReplicaModifications><Status>Enabled\
                     </Status></ReplicaModifications></SourceSelectionCriteria>",
                ),
                unbuilt.clone(),
            ),
            (
                rule_with(
                    "<Filter/><ExistingObjectReplication><Status>Enabled</Status>\
                     </ExistingObjectReplication>",
                ),
                unbuilt.clone(),
            ),
        ] {
            assert_eq!(code(&rules), expected, "{rules}");
        }
        let with_destination = |destination: &str| {
            format!(
                "<Rule><Status>Enabled</Status><Priority>1</Priority><Filter/>\
                 <DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>\
                 <Destination>{destination}</Destination></Rule>"
            )
        };
        for (destination, expected) in [
            ("", malformed.clone()),
            ("<Bucket>copies</Bucket>", invalid.clone()),
            ("<Bucket>arn:aws:s3:::</Bucket>", invalid.clone()),
            ("<Bucket>arn:aws:s3:::copies/key</Bucket>", invalid.clone()),
            (
                "<Bucket>arn:aws:s3:::copies</Bucket><Account></Account>",
                invalid.clone(),
            ),
            (
                "<Bucket>arn:aws:s3:::copies</Bucket><StorageClass>EXPRESS_ONEZONE\
                 </StorageClass>",
                (400, "InvalidStorageClass".to_string()),
            ),
            (
                "<Bucket>arn:aws:s3:::copies</Bucket><AccessControlTranslation><Owner>\
                 Destination</Owner></AccessControlTranslation>",
                request.clone(),
            ),
            (
                "<Bucket>arn:aws:s3:::copies</Bucket><Account>000000000001</Account>\
                 <AccessControlTranslation><Owner>Destination</Owner>\
                 </AccessControlTranslation>",
                unbuilt.clone(),
            ),
            (
                "<Bucket>arn:aws:s3:::copies</Bucket><EncryptionConfiguration>\
                 <ReplicaKmsKeyID>k</ReplicaKmsKeyID></EncryptionConfiguration>",
                unbuilt.clone(),
            ),
            (
                "<Bucket>arn:aws:s3:::copies</Bucket><Metrics><Status>Enabled</Status>\
                 </Metrics>",
                unbuilt.clone(),
            ),
            (
                "<Bucket>arn:aws:s3:::copies</Bucket><ReplicationTime><Status>Enabled\
                 </Status></ReplicationTime>",
                unbuilt.clone(),
            ),
        ] {
            assert_eq!(
                code(&with_destination(destination)),
                expected,
                "{destination}"
            );
        }
        // Enabled delete-marker replication, and a second destination.
        assert_eq!(
            code(
                "<Rule><Status>Enabled</Status><Priority>1</Priority><Filter/>\
                 <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>\
                 <Destination><Bucket>arn:aws:s3:::copies</Bucket></Destination></Rule>"
            ),
            unbuilt
        );
        assert_eq!(
            code(&format!(
                "{}{}",
                rule_with("<ID>a</ID><Filter/>"),
                with_destination("<Bucket>arn:aws:s3:::elsewhere</Bucket>")
            )),
            unbuilt
        );
    }
}
