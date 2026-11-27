//! S3 Storage Lens configurations (dashboards), through the S3 Control API
//! (under `/v20180820/storagelens`): Put, Get, Delete and List, and their
//! tags (Put, Get, Delete). A configuration is its account's, in this
//! server's Region (its home Region), at most 50 an account, kept under
//! the empty bucket name as kind `STORAGE_LENS` by `<account>/<id>` (its
//! tags as `STORAGE_LENS_TAGS`).
//!
//! Its scope (all buckets, or those it includes or excludes, by bucket or
//! Region), its levels (account, bucket, prefix) and their metrics (free,
//! activity, advanced cost optimization, advanced data protection), its
//! export and its CloudWatch publishing are kept as given. What nothing
//! here computes is refused (501): detailed status code and advanced
//! performance metrics, Storage Lens groups, organizations, table and
//! expanded-prefixes exports, and SSE-KMS exports.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::{STORAGE_LENS, STORAGE_LENS_TAGS};

use crate::access_points::{elements, escape, malformed};
use crate::authz::Authz;
use crate::error::S3Error;
use crate::server::{Authenticated, Body, Connection, S3State, full, read_body};

const XMLNS: &str = crate::batch::XMLNS;
/// The S3 Control path prefix of these calls.
pub(crate) const PREFIX: &str = "/v20180820/storagelens";
/// "up to 50 dashboards per home Region".
const MAX_CONFIGURATIONS: usize = 50;
/// "You can set up to a maximum of 50 tags."
const MAX_TAGS: usize = 50;
const MAX_BODY: usize = 256 * 1024;

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidRequest", message)
}

fn no_such_configuration() -> S3Error {
    S3Error::new(
        404,
        "NoSuchConfiguration",
        "The specified configuration does not exist",
    )
}

/// Whether `id` is a configuration's id: 1 to 64 letters, digits, hyphens,
/// underscores and periods.
fn valid_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn bad_id() -> S3Error {
    invalid("A configuration's id is 1 to 64 letters, digits, hyphens, underscores and periods.")
}

/// The metrics a level (the account's, or its buckets') has on.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Level {
    pub(crate) activity: bool,
    pub(crate) cost_optimization: bool,
    pub(crate) data_protection: bool,
}

/// Prefix-level storage metrics.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PrefixLevel {
    pub(crate) enabled: bool,
    pub(crate) delimiter: Option<String>,
    pub(crate) max_depth: Option<u8>,
    pub(crate) min_storage_percentage: Option<f64>,
}

/// The buckets a configuration covers: all, or those it includes or
/// excludes (by bucket ARN, or by Region).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Scope {
    All,
    Include {
        buckets: Vec<String>,
        regions: Vec<String>,
    },
    Exclude {
        buckets: Vec<String>,
        regions: Vec<String>,
    },
}

/// A metrics export to a bucket.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Export {
    /// `CSV` or `Parquet`.
    pub(crate) format: String,
    pub(crate) account: String,
    /// The destination bucket's ARN.
    pub(crate) bucket_arn: String,
    pub(crate) prefix: Option<String>,
    /// Encrypted with SSE-S3.
    pub(crate) sse_s3: bool,
}

/// A Storage Lens configuration.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LensConfig {
    pub(crate) id: String,
    pub(crate) enabled: bool,
    pub(crate) account_level: Level,
    pub(crate) bucket_level: Level,
    pub(crate) prefix_level: Option<PrefixLevel>,
    pub(crate) scope: Scope,
    pub(crate) export: Option<Export>,
    pub(crate) cloudwatch: bool,
    pub(crate) prefix_delimiter: Option<String>,
}

fn boolean(text: &str) -> Result<bool, S3Error> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(malformed()),
    }
}

/// A bucket ARN's bucket, if `arn` is one (`arn:aws:s3:::<bucket>`, at
/// most 128 characters). The bucket's name is of the characters any bucket's
/// may be (letters, digits, periods, hyphens and underscores, as legacy
/// names are), not held to the rules a new bucket's name is: the bucket may
/// be older than they are.
pub(crate) fn bucket_of_arn(arn: &str) -> Option<&str> {
    if arn.len() > 128 {
        return None;
    }
    arn.strip_prefix("arn:aws:s3:::").filter(|bucket| {
        !bucket.is_empty()
            && bucket
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    })
}

/// Whether `delimiter` is a prefix delimiter: at most one character, one
/// XML can carry (no control character, nor U+FFFE or U+FFFF).
fn valid_delimiter(delimiter: &str) -> bool {
    delimiter.chars().count() <= 1
        && delimiter
            .chars()
            .all(|c| !c.is_control() && !matches!(c, '\u{fffe}' | '\u{ffff}'))
}

/// The configuration in a document of root `root`, `under` it (empty: the
/// root is the configuration itself). `arn` is what its StorageLensArn may
/// be (it is read-only).
fn parse(
    body: &[u8],
    root: &str,
    under: &str,
    arn: &dyn Fn(&str) -> String,
) -> Result<(LensConfig, Vec<(String, String)>), S3Error> {
    let elements = elements(body)?;
    if elements.first().map(|(path, _)| path.as_str()) != Some(root) {
        return Err(malformed());
    }
    let base = match under {
        "" => format!("{root}/"),
        under => format!("{root}/{under}/"),
    };
    let mut id = None;
    let mut enabled = None;
    let (mut account_level, mut bucket_level) = (Level::default(), Level::default());
    let mut prefix_level: Option<PrefixLevel> = None;
    let mut include: Option<(Vec<String>, Vec<String>)> = None;
    let mut exclude: Option<(Vec<String>, Vec<String>)> = None;
    let mut export: Option<Export> = None;
    let mut export_parts = (None, None, None, None, None);
    let mut has_destination = false;
    let mut cloudwatch = None;
    let mut has_cloudwatch = false;
    let mut given_arn = None;
    let mut prefix_delimiter = None;
    let (mut has_account_level, mut has_bucket_level) = (false, false);
    let mut seen = std::collections::HashSet::new();
    let mut tags: Vec<(String, String)> = Vec::new();
    let mut tag: Option<(Option<String>, Option<String>)> = None;
    let not_built = |what: &str| Err(S3Error::not_implemented(what.to_string()));
    let once = |slot: &mut Option<String>, text: &str| -> Result<(), S3Error> {
        match slot {
            Some(_) => Err(malformed()),
            None => {
                *slot = Some(text.to_string());
                Ok(())
            }
        }
    };
    for (path, text) in &elements[1..] {
        let trimmed = text.trim();
        // The request's own tags (PutStorageLensConfiguration's).
        if let Some(rest) = path.strip_prefix(&format!("{root}/Tags")) {
            match rest {
                "" => {
                    if !seen.insert(path.as_str()) {
                        return Err(malformed());
                    }
                }
                "/Tag" => {
                    if let Some(done) = tag.take() {
                        tags.push(finish_tag(done)?);
                    }
                    tag = Some((None, None));
                }
                "/Tag/Key" | "/Tag/Value" => {
                    let current = tag.as_mut().ok_or_else(malformed)?;
                    let slot = if rest == "/Tag/Key" {
                        &mut current.0
                    } else {
                        &mut current.1
                    };
                    once(slot, text)?;
                }
                _ => return Err(malformed()),
            }
            continue;
        }
        if !under.is_empty() && path.as_str() == format!("{root}/{under}") {
            if !seen.insert(path.as_str()) {
                return Err(malformed());
            }
            continue;
        }
        // Each element once, but a list's members.
        let member = path.ends_with("/Buckets/Arn") || path.ends_with("/Regions/Region");
        if !(member || seen.insert(path.as_str())) {
            return Err(malformed());
        }
        let Some(field) = path.strip_prefix(base.as_str()) else {
            return Err(malformed());
        };
        let level_flag = |level: &mut Level, name: &str| -> Result<bool, S3Error> {
            match name {
                "ActivityMetrics"
                | "AdvancedCostOptimizationMetrics"
                | "AdvancedDataProtectionMetrics" => Ok(true),
                "ActivityMetrics/IsEnabled" => {
                    level.activity = boolean(trimmed)?;
                    Ok(true)
                }
                "AdvancedCostOptimizationMetrics/IsEnabled" => {
                    level.cost_optimization = boolean(trimmed)?;
                    Ok(true)
                }
                "AdvancedDataProtectionMetrics/IsEnabled" => {
                    level.data_protection = boolean(trimmed)?;
                    Ok(true)
                }
                "DetailedStatusCodesMetrics" | "AdvancedPerformanceMetrics" => Ok(true),
                "DetailedStatusCodesMetrics/IsEnabled" | "AdvancedPerformanceMetrics/IsEnabled" => {
                    if boolean(trimmed)? {
                        return Err(S3Error::not_implemented(
                            "Detailed status code and advanced performance metrics",
                        ));
                    }
                    Ok(true)
                }
                _ => Ok(false),
            }
        };
        match field {
            "Id" => once(&mut id, trimmed)?,
            "IsEnabled" => {
                if enabled.replace(boolean(trimmed)?).is_some() {
                    return Err(malformed());
                }
            }
            "StorageLensArn" => once(&mut given_arn, trimmed)?,
            "PrefixDelimiter" => {
                if !valid_delimiter(text) {
                    return Err(invalid(
                        "A prefix delimiter is one character, not a control character.",
                    ));
                }
                once(&mut prefix_delimiter, text)?
            }
            "AccountLevel" => has_account_level = true,
            "AccountLevel/BucketLevel" => has_bucket_level = true,
            "AccountLevel/BucketLevel/PrefixLevel"
            | "AccountLevel/BucketLevel/PrefixLevel/StorageMetrics" => {
                prefix_level.get_or_insert(PrefixLevel {
                    enabled: false,
                    delimiter: None,
                    max_depth: None,
                    min_storage_percentage: None,
                });
            }
            "AccountLevel/BucketLevel/PrefixLevel/StorageMetrics/IsEnabled" => {
                prefix_level.as_mut().ok_or_else(malformed)?.enabled = boolean(trimmed)?;
            }
            "AccountLevel/BucketLevel/PrefixLevel/StorageMetrics/SelectionCriteria" => {}
            "AccountLevel/BucketLevel/PrefixLevel/StorageMetrics/SelectionCriteria/Delimiter" => {
                let prefix = prefix_level.as_mut().ok_or_else(malformed)?;
                if !valid_delimiter(text) {
                    return Err(invalid(
                        "A prefix delimiter is one character, not a control character.",
                    ));
                }
                once(&mut prefix.delimiter, text)?;
            }
            "AccountLevel/BucketLevel/PrefixLevel/StorageMetrics/SelectionCriteria/MaxDepth" => {
                let depth = trimmed
                    .parse::<u8>()
                    .ok()
                    .filter(|depth| (1..=10).contains(depth))
                    .ok_or_else(|| invalid("MaxDepth is from 1 to 10."))?;
                prefix_level.as_mut().ok_or_else(malformed)?.max_depth = Some(depth);
            }
            "AccountLevel/BucketLevel/PrefixLevel/StorageMetrics/SelectionCriteria/MinStorageBytesPercentage" =>
            {
                let percentage = trimmed
                    .parse::<f64>()
                    .ok()
                    .filter(|percentage| (1.0..=100.0).contains(percentage))
                    .ok_or_else(|| invalid("MinStorageBytesPercentage is from 1 to 100."))?;
                prefix_level
                    .as_mut()
                    .ok_or_else(malformed)?
                    .min_storage_percentage = Some(percentage);
            }
            other if other.starts_with("AccountLevel/StorageLensGroupLevel") => {
                return not_built("Storage Lens groups");
            }
            other if other.starts_with("AccountLevel/BucketLevel/") => {
                if !level_flag(
                    &mut bucket_level,
                    &other["AccountLevel/BucketLevel/".len()..],
                )? {
                    return Err(malformed());
                }
            }
            other if other.starts_with("AccountLevel/") => {
                if !level_flag(&mut account_level, &other["AccountLevel/".len()..])? {
                    return Err(malformed());
                }
            }
            "Include" | "Exclude" => {
                let slot = if field == "Include" {
                    &mut include
                } else {
                    &mut exclude
                };
                if slot.replace((Vec::new(), Vec::new())).is_some() {
                    return Err(malformed());
                }
            }
            "Include/Buckets" | "Include/Regions" | "Exclude/Buckets" | "Exclude/Regions" => {}
            "Include/Buckets/Arn"
            | "Exclude/Buckets/Arn"
            | "Include/Regions/Region"
            | "Exclude/Regions/Region" => {
                let slot = if field.starts_with("Include") {
                    &mut include
                } else {
                    &mut exclude
                };
                let (buckets, regions) = slot.as_mut().ok_or_else(malformed)?;
                if field.ends_with("/Arn") {
                    if bucket_of_arn(trimmed).is_none() {
                        return Err(invalid(format!("{trimmed} is not a bucket's ARN.")));
                    }
                    buckets.push(trimmed.to_string());
                } else {
                    if !(5..=30).contains(&trimmed.len())
                        || !trimmed.bytes().all(|byte| {
                            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                        })
                    {
                        return Err(invalid(format!("{trimmed} is not a Region.")));
                    }
                    regions.push(trimmed.to_string());
                }
            }
            "DataExport" => {}
            "DataExport/S3BucketDestination" => has_destination = true,
            "DataExport/S3BucketDestination/Format" => once(&mut export_parts.0, trimmed)?,
            "DataExport/S3BucketDestination/OutputSchemaVersion" => {
                once(&mut export_parts.1, trimmed)?
            }
            "DataExport/S3BucketDestination/AccountId" => once(&mut export_parts.2, trimmed)?,
            "DataExport/S3BucketDestination/Arn" => once(&mut export_parts.3, trimmed)?,
            "DataExport/S3BucketDestination/Prefix" => once(&mut export_parts.4, text)?,
            "DataExport/S3BucketDestination/Encryption" => {}
            "DataExport/S3BucketDestination/Encryption/SSE-S3" => {
                export.get_or_insert(Export {
                    format: String::new(),
                    account: String::new(),
                    bucket_arn: String::new(),
                    prefix: None,
                    sse_s3: true,
                });
            }
            other if other.starts_with("DataExport/S3BucketDestination/Encryption/SSE-KMS") => {
                return not_built("Storage Lens exports encrypted with SSE-KMS");
            }
            "DataExport/CloudWatchMetrics" => has_cloudwatch = true,
            "DataExport/CloudWatchMetrics/IsEnabled" => {
                if cloudwatch.replace(boolean(trimmed)?).is_some() {
                    return Err(malformed());
                }
            }
            other if other.starts_with("DataExport/StorageLensTableDestination") => {
                return not_built("Storage Lens exports to S3 Tables");
            }
            other if other.starts_with("ExpandedPrefixesDataExport") => {
                return not_built("Storage Lens expanded prefixes exports");
            }
            other if other.starts_with("AwsOrg") => {
                return not_built("Storage Lens configurations of an organization");
            }
            _ => return Err(malformed()),
        }
    }
    if let Some(done) = tag.take() {
        tags.push(finish_tag(done)?);
    }
    let id = id.ok_or_else(malformed)?;
    let enabled = enabled.ok_or_else(malformed)?;
    if !has_account_level || !has_bucket_level || has_cloudwatch != cloudwatch.is_some() {
        return Err(malformed());
    }
    // "You must enable Storage Lens metrics consistently at both the
    // account level and bucket level, or your request will fail."
    if account_level != bucket_level {
        return Err(invalid(
            "Storage Lens metrics are enabled at both the account and the bucket level, or at neither.",
        ));
    }
    if !valid_id(&id) {
        return Err(bad_id());
    }
    // "When custom delimiters are used, both the prefix delimiter and
    // existing delimiter must specify the same special character."
    if let (Some(prefix_delimiter), Some(delimiter)) = (
        &prefix_delimiter,
        prefix_level
            .as_ref()
            .and_then(|prefix| prefix.delimiter.as_ref()),
    ) && prefix_delimiter != delimiter
    {
        return Err(invalid(
            "The prefix delimiter and the delimiter are the same character.",
        ));
    }
    if let Some(given) = &given_arn
        && *given != arn(&id)
    {
        return Err(invalid("StorageLensArn is read-only."));
    }
    let scope = match (include, exclude) {
        (Some(_), Some(_)) => {
            return Err(invalid(
                "A configuration has an Include or an Exclude, not both.",
            ));
        }
        (Some((buckets, regions)), None) | (None, Some((buckets, regions)))
            if buckets.is_empty() && regions.is_empty() =>
        {
            return Err(invalid("An Include or an Exclude is not empty."));
        }
        (Some((buckets, regions)), None) => Scope::Include { buckets, regions },
        (None, Some((buckets, regions))) => Scope::Exclude { buckets, regions },
        (None, None) => Scope::All,
    };
    let export = match (has_destination, export_parts) {
        (false, (None, None, None, None, None)) if export.is_none() => None,
        (true, (Some(format), Some(version), Some(account), Some(bucket_arn), prefix)) => {
            if !matches!(format.as_str(), "CSV" | "Parquet") {
                return Err(invalid("An export's Format is CSV or Parquet."));
            }
            if version != "V_1" {
                return Err(invalid("An export's OutputSchemaVersion is V_1."));
            }
            if !truthdb_object::valid_account_id(&account) {
                return Err(invalid("An export's AccountId is an account's."));
            }
            if bucket_of_arn(&bucket_arn).is_none() {
                return Err(invalid("An export's Arn is a bucket's."));
            }
            Some(Export {
                format,
                account,
                bucket_arn,
                prefix,
                sse_s3: export.is_some_and(|export| export.sse_s3),
            })
        }
        _ => return Err(malformed()),
    };
    let tags = checked(tags)?;
    Ok((
        LensConfig {
            id,
            enabled,
            account_level,
            bucket_level,
            prefix_level,
            scope,
            export,
            cloudwatch: cloudwatch.unwrap_or(false),
            prefix_delimiter,
        },
        tags,
    ))
}

/// A `<Tag>`'s key and value, checked.
fn finish_tag((key, value): (Option<String>, Option<String>)) -> Result<(String, String), S3Error> {
    let (Some(key), Some(value)) = (key, value) else {
        return Err(malformed());
    };
    // Letters, spaces, digits and `_.:/=+-@`.
    let allowed = |text: &str| {
        text.chars().all(|c| {
            !c.is_control() && (c.is_alphanumeric() || c.is_whitespace() || "_.:/=+-@".contains(c))
        })
    };
    if !(1..=128).contains(&key.chars().count())
        || value.chars().count() > 256
        || !allowed(&key)
        || !allowed(&value)
    {
        return Err(invalid(
            "A tag's key is 1 to 128 characters, its value at most 256, of letters, spaces, digits and _.:/=+-@.",
        ));
    }
    Ok((key, value))
}

/// The tags in a document of root `root` (`<Tags><Tag>` under it).
fn parse_tags(body: &[u8], root: &str) -> Result<Vec<(String, String)>, S3Error> {
    let elements = elements(body)?;
    if elements.first().map(|(path, _)| path.as_str()) != Some(root) {
        return Err(malformed());
    }
    let mut tags = Vec::new();
    let mut tag: Option<(Option<String>, Option<String>)> = None;
    let mut has_tags = false;
    for (path, text) in &elements[1..] {
        match path.strip_prefix(root).unwrap_or("") {
            "/Tags" => {
                if std::mem::replace(&mut has_tags, true) {
                    return Err(malformed());
                }
            }
            "/Tags/Tag" => {
                if let Some(done) = tag.take() {
                    tags.push(finish_tag(done)?);
                }
                tag = Some((None, None));
            }
            rest @ ("/Tags/Tag/Key" | "/Tags/Tag/Value") => {
                let current = tag.as_mut().ok_or_else(malformed)?;
                let slot = if rest.ends_with("Key") {
                    &mut current.0
                } else {
                    &mut current.1
                };
                if slot.replace(text.clone()).is_some() {
                    return Err(malformed());
                }
            }
            _ => return Err(malformed()),
        }
    }
    if let Some(done) = tag.take() {
        tags.push(finish_tag(done)?);
    }
    if !has_tags {
        return Err(malformed());
    }
    checked(tags)
}

/// `tags`, if there are at most [`MAX_TAGS`] of them, each key once.
fn checked(tags: Vec<(String, String)>) -> Result<Vec<(String, String)>, S3Error> {
    if tags.len() > MAX_TAGS {
        return Err(invalid(format!(
            "A configuration has at most {MAX_TAGS} tags."
        )));
    }
    let mut keys: Vec<&str> = tags.iter().map(|(key, _)| key.as_str()).collect();
    keys.sort_unstable();
    keys.dedup();
    if keys.len() != tags.len() {
        return Err(invalid("A tag's key is given once."));
    }
    Ok(tags)
}

fn render_level(level: &Level) -> String {
    format!(
        "<ActivityMetrics><IsEnabled>{}</IsEnabled></ActivityMetrics><AdvancedCostOptimizationMetrics><IsEnabled>{}</IsEnabled></AdvancedCostOptimizationMetrics><AdvancedDataProtectionMetrics><IsEnabled>{}</IsEnabled></AdvancedDataProtectionMetrics>",
        level.activity, level.cost_optimization, level.data_protection
    )
}

fn render_scope(name: &str, buckets: &[String], regions: &[String]) -> String {
    let mut inner = String::new();
    if !buckets.is_empty() {
        let arns: String = buckets
            .iter()
            .map(|arn| format!("<Arn>{}</Arn>", escape(arn)))
            .collect();
        inner.push_str(&format!("<Buckets>{arns}</Buckets>"));
    }
    if !regions.is_empty() {
        let names: String = regions
            .iter()
            .map(|region| format!("<Region>{}</Region>", escape(region)))
            .collect();
        inner.push_str(&format!("<Regions>{names}</Regions>"));
    }
    format!("<{name}>{inner}</{name}>")
}

impl LensConfig {
    /// Its elements (inside `<StorageLensConfiguration>`), without its ARN:
    /// that is the server's Region's when it is answered.
    fn render(&self) -> String {
        let prefix = self
            .prefix_level
            .as_ref()
            .map(|prefix| {
                let mut criteria = String::new();
                if let Some(delimiter) = &prefix.delimiter {
                    criteria.push_str(&format!("<Delimiter>{}</Delimiter>", escape(delimiter)));
                }
                if let Some(depth) = prefix.max_depth {
                    criteria.push_str(&format!("<MaxDepth>{depth}</MaxDepth>"));
                }
                if let Some(percentage) = prefix.min_storage_percentage {
                    criteria.push_str(&format!(
                        "<MinStorageBytesPercentage>{percentage}</MinStorageBytesPercentage>"
                    ));
                }
                format!(
                    "<PrefixLevel><StorageMetrics><IsEnabled>{}</IsEnabled><SelectionCriteria>{criteria}</SelectionCriteria></StorageMetrics></PrefixLevel>",
                    prefix.enabled
                )
            })
            .unwrap_or_default();
        let scope = match &self.scope {
            Scope::All => String::new(),
            Scope::Include { buckets, regions } => render_scope("Include", buckets, regions),
            Scope::Exclude { buckets, regions } => render_scope("Exclude", buckets, regions),
        };
        let mut export = String::new();
        if let Some(destination) = &self.export {
            let prefix = destination
                .prefix
                .as_ref()
                .map(|prefix| format!("<Prefix>{}</Prefix>", escape(prefix)))
                .unwrap_or_default();
            let encryption = if destination.sse_s3 {
                "<Encryption><SSE-S3></SSE-S3></Encryption>"
            } else {
                ""
            };
            export.push_str(&format!(
                "<S3BucketDestination><Format>{}</Format><OutputSchemaVersion>V_1</OutputSchemaVersion><AccountId>{}</AccountId><Arn>{}</Arn>{prefix}{encryption}</S3BucketDestination>",
                escape(&destination.format),
                escape(&destination.account),
                escape(&destination.bucket_arn)
            ));
        }
        if self.export.is_some() || self.cloudwatch {
            export = format!(
                "<DataExport>{export}<CloudWatchMetrics><IsEnabled>{}</IsEnabled></CloudWatchMetrics></DataExport>",
                self.cloudwatch
            );
        }
        let delimiter = self
            .prefix_delimiter
            .as_ref()
            .map(|delimiter| format!("<PrefixDelimiter>{}</PrefixDelimiter>", escape(delimiter)))
            .unwrap_or_default();
        format!(
            "<Id>{}</Id><AccountLevel>{}<BucketLevel>{}{prefix}</BucketLevel></AccountLevel>{scope}{export}<IsEnabled>{}</IsEnabled>{delimiter}",
            escape(&self.id),
            render_level(&self.account_level),
            render_level(&self.bucket_level),
            self.enabled,
        )
    }
}

fn render_tags(tags: &[(String, String)]) -> String {
    tags.iter()
        .map(|(key, value)| {
            format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                escape(key),
                escape(value)
            )
        })
        .collect()
}

/// A configuration's key: `<account>/<id>`.
fn key(account: &str, id: &str) -> String {
    format!("{account}/{id}")
}

/// The configuration's ARN.
pub(crate) fn arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:s3:{region}:{account}:storage-lens/{id}")
}

/// `account`'s configuration `id`, as stored (its elements, rendered).
async fn stored(state: &S3State, account: &str, id: &str) -> Result<Option<String>, S3Error> {
    let name = key(account, id);
    crate::scope::object(state, move |file| {
        file.bucket_config("", STORAGE_LENS, &name)
    })
    .await
    .map(|found| found.map(|document| String::from_utf8_lossy(&document).into_owned()))
}

/// Every stored configuration of `account`, by id: each one's elements.
pub(crate) fn configurations_of(
    file: &mut truthdb_object::ObjFile<'_>,
    account: &str,
) -> Result<Vec<(String, String)>, truthdb_core::storage::StorageError> {
    let prefix = format!("{account}/");
    let mut out = Vec::new();
    for id in file.bucket_config_ids_with("", STORAGE_LENS, &prefix)? {
        if let Some(document) = file.bucket_config("", STORAGE_LENS, &id)? {
            out.push((
                id[prefix.len()..].to_string(),
                String::from_utf8_lossy(&document).into_owned(),
            ));
        }
    }
    Ok(out)
}

/// A stored configuration, parsed again.
pub(crate) fn read(document: &str, region: &str, account: &str) -> Result<LensConfig, S3Error> {
    let wrapped = format!("<StorageLensConfiguration>{document}</StorageLensConfiguration>");
    parse(wrapped.as_bytes(), "StorageLensConfiguration", "", &|id| {
        arn(region, account, id)
    })
    .map(|(config, _)| config)
    .map_err(|err| {
        S3Error::internal(format!(
            "stored Storage Lens configuration: {}",
            err.message
        ))
    })
}

fn xml(body: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response")
}

fn empty() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .body(full(Vec::new()))
        .expect("a valid response")
}

/// Handles a request under [`PREFIX`].
pub async fn handle(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let Some(principal) = auth.principal.clone() else {
        return Err(S3Error::access_denied("Access Denied"));
    };
    let header_account = request
        .headers()
        .get("x-amz-account-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    // Not cross-account: a configuration is its account's.
    if header_account != principal.account {
        return Err(S3Error::access_denied("Access Denied"));
    }
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let rest = path.strip_prefix(PREFIX).unwrap_or("");
    let segments: Vec<&str> = rest.split('/').skip(1).collect();
    let (action, id, tagging) = match (&method, segments.as_slice()) {
        (&Method::GET, []) => ("s3:ListStorageLensConfigurations", None, false),
        (&Method::PUT, [id]) => ("s3:PutStorageLensConfiguration", Some(*id), false),
        (&Method::GET, [id]) => ("s3:GetStorageLensConfiguration", Some(*id), false),
        (&Method::DELETE, [id]) => ("s3:DeleteStorageLensConfiguration", Some(*id), false),
        (&Method::PUT, [id, "tagging"]) => {
            ("s3:PutStorageLensConfigurationTagging", Some(*id), true)
        }
        (&Method::GET, [id, "tagging"]) => {
            ("s3:GetStorageLensConfigurationTagging", Some(*id), true)
        }
        (&Method::DELETE, [id, "tagging"]) => {
            ("s3:DeleteStorageLensConfigurationTagging", Some(*id), true)
        }
        _ => return Err(S3Error::not_implemented(format!("{method} {path}"))),
    };
    let id = id
        .map(|id| {
            percent_encoding::percent_decode_str(id)
                .decode_utf8()
                .map(|id| id.into_owned())
                .map_err(|_| invalid("The configuration's id is not UTF-8."))
        })
        .transpose()?;
    // The path's id is an id before anything is looked up or authorized by
    // it: a key holding a NUL would reach another configuration's stored
    // chunks, and its ARN would not be the one a policy names.
    if id.as_deref().is_some_and(|id| !valid_id(id)) {
        return Err(bad_id());
    }
    let authz = Authz::load(
        state,
        Some(&principal),
        auth.signed,
        request.headers(),
        &query,
        request.extensions().get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    let account = principal.account.clone();
    match &id {
        Some(id) => authz.check(
            action,
            crate::authz::Resource::Arn(&arn(&state.region, &account, id)),
        )?,
        None => authz.check(action, crate::authz::Resource::Service)?,
    }
    let scope = crate::scope::Scope {
        key: Some((principal.access_key.clone(), principal.account.clone())),
        buckets: Vec::new(),
        origin: None,
    };
    crate::scope::within(scope, async move {
        match (id, tagging, method) {
            (None, _, _) => list(state, &account).await,
            (Some(id), false, Method::PUT) => put(state, request, auth, &account, &id).await,
            (Some(id), false, Method::GET) => {
                let document = stored(state, &account, &id)
                    .await?
                    .ok_or_else(no_such_configuration)?;
                let arn = escape(&arn(&state.region, &account, &id));
                Ok(xml(format!(
                    "<StorageLensConfiguration xmlns=\"{XMLNS}\">{document}<StorageLensArn>{arn}</StorageLensArn></StorageLensConfiguration>"
                )))
            }
            (Some(id), false, _) => {
                let name = key(&account, &id);
                let deleted = crate::scope::object(state, move |file| {
                    file.transaction(|tx| {
                        let deleted = tx.delete_bucket_config("", STORAGE_LENS, &name)?;
                        tx.delete_bucket_config("", STORAGE_LENS_TAGS, &name)?;
                        // Its reports go with it.
                        tx.forget_lens(&name)?;
                        Ok(deleted)
                    })
                })
                .await?;
                if !deleted {
                    return Err(no_such_configuration());
                }
                Ok(empty())
            }
            (Some(id), true, method) => tagging_call(state, request, auth, &account, &id, method).await,
        }
    })
    .await
}

async fn put(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    account: &str,
    id: &str,
) -> Result<Response<Body>, S3Error> {
    let body = read_body(request, auth, MAX_BODY).await?;
    let region = state.region.clone();
    let of = account.to_string();
    let (config, tags) = parse(
        &body,
        "PutStorageLensConfigurationRequest",
        "StorageLensConfiguration",
        &|id| arn(&region, &of, id),
    )?;
    if config.id != id {
        return Err(invalid("The configuration's Id is the one its path names."));
    }
    // Kept by the parser, but nothing exports or publishes metrics yet.
    if config.export.is_some() {
        return Err(S3Error::not_implemented("Storage Lens metrics exports"));
    }
    if config.cloudwatch {
        return Err(S3Error::not_implemented(
            "Storage Lens metrics published to CloudWatch",
        ));
    }
    let document = config.render();
    let (of, name) = (account.to_string(), key(account, id));
    let made = crate::scope::object(state, move |file| {
        let existing = file.bucket_config("", STORAGE_LENS, &name)?.is_some();
        if !existing && configurations_of(file, &of)?.len() >= MAX_CONFIGURATIONS {
            return Ok(false);
        }
        file.transaction(|tx| {
            tx.put_bucket_config("", STORAGE_LENS, &name, document.as_bytes())?;
            if tags.is_empty() {
                tx.delete_bucket_config("", STORAGE_LENS_TAGS, &name)?;
            } else {
                tx.put_bucket_config("", STORAGE_LENS_TAGS, &name, render_tags(&tags).as_bytes())?;
            }
            Ok(true)
        })
    })
    .await?;
    if !made {
        return Err(invalid(format!(
            "An account has at most {MAX_CONFIGURATIONS} Storage Lens configurations."
        )));
    }
    // Its first report is made at once.
    crate::storage_lens_run::wake();
    Ok(empty())
}

async fn list(state: &S3State, account: &str) -> Result<Response<Body>, S3Error> {
    let of = account.to_string();
    let all = crate::scope::object(state, move |file| configurations_of(file, &of)).await?;
    let mut entries = String::new();
    for (id, document) in all {
        let config = read(&document, &state.region, account)?;
        entries.push_str(&format!(
            "<StorageLensConfiguration><Id>{}</Id><StorageLensArn>{}</StorageLensArn><HomeRegion>{}</HomeRegion><IsEnabled>{}</IsEnabled></StorageLensConfiguration>",
            escape(&id),
            escape(&arn(&state.region, account, &id)),
            escape(&state.region),
            config.enabled
        ));
    }
    Ok(xml(format!(
        "<ListStorageLensConfigurationResult xmlns=\"{XMLNS}\">{entries}</ListStorageLensConfigurationResult>"
    )))
}

async fn tagging_call(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    account: &str,
    id: &str,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let name = key(account, id);
    // Its tags: `None` for none to put (a read or a delete).
    let tags = match method {
        Method::PUT => {
            let body = read_body(request, auth, MAX_BODY).await?;
            Some(parse_tags(
                &body,
                "PutStorageLensConfigurationTaggingRequest",
            )?)
        }
        _ => None,
    };
    let read = method == Method::GET;
    // The configuration is looked for in the same hold as its tags are
    // read or changed, so a delete between them leaves no tags behind.
    let found = crate::scope::object(state, move |file| {
        if file.bucket_config("", STORAGE_LENS, &name)?.is_none() {
            return Ok(None);
        }
        if read {
            return file.bucket_config("", STORAGE_LENS_TAGS, &name).map(Some);
        }
        file.transaction(|tx| match &tags {
            Some(tags) => {
                tx.put_bucket_config("", STORAGE_LENS_TAGS, &name, render_tags(tags).as_bytes())
            }
            None => tx
                .delete_bucket_config("", STORAGE_LENS_TAGS, &name)
                .map(|_| ()),
        })?;
        Ok(Some(None))
    })
    .await?
    .ok_or_else(no_such_configuration)?;
    if !read {
        return Ok(empty());
    }
    let tags = found
        .map(|document| String::from_utf8_lossy(&document).into_owned())
        .unwrap_or_default();
    Ok(xml(format!(
        "<GetStorageLensConfigurationTaggingResult xmlns=\"{XMLNS}\"><Tags>{tags}</Tags></GetStorageLensConfigurationTaggingResult>"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(inner: &str) -> String {
        format!(
            "<PutStorageLensConfigurationRequest><StorageLensConfiguration>{inner}</StorageLensConfiguration></PutStorageLensConfigurationRequest>"
        )
    }

    fn parsed(inner: &str) -> Result<LensConfig, S3Error> {
        parse(
            request(inner).as_bytes(),
            "PutStorageLensConfigurationRequest",
            "StorageLensConfiguration",
            &|id| arn("us-east-1", "111111111111", id),
        )
        .map(|(config, _)| config)
    }

    const LEVELS: &str = "<AccountLevel><ActivityMetrics><IsEnabled>true</IsEnabled></ActivityMetrics><AdvancedCostOptimizationMetrics><IsEnabled>true</IsEnabled></AdvancedCostOptimizationMetrics><BucketLevel><ActivityMetrics><IsEnabled>true</IsEnabled></ActivityMetrics><AdvancedCostOptimizationMetrics><IsEnabled>true</IsEnabled></AdvancedCostOptimizationMetrics><PrefixLevel><StorageMetrics><IsEnabled>true</IsEnabled><SelectionCriteria><Delimiter>/</Delimiter><MaxDepth>3</MaxDepth><MinStorageBytesPercentage>2.5</MinStorageBytesPercentage></SelectionCriteria></StorageMetrics></PrefixLevel></BucketLevel></AccountLevel>";

    #[test]
    fn a_configuration_reads_back_from_its_rendering() {
        let config = parsed(&format!(
            "<Id>lens.1</Id>{LEVELS}<Include><Buckets><Arn>arn:aws:s3:::data</Arn></Buckets><Regions><Region>us-east-1</Region></Regions></Include><DataExport><S3BucketDestination><Format>CSV</Format><OutputSchemaVersion>V_1</OutputSchemaVersion><AccountId>111111111111</AccountId><Arn>arn:aws:s3:::reports</Arn><Prefix>lens</Prefix><Encryption><SSE-S3/></Encryption></S3BucketDestination><CloudWatchMetrics><IsEnabled>true</IsEnabled></CloudWatchMetrics></DataExport><IsEnabled>true</IsEnabled>"
        ))
        .unwrap();
        assert_eq!(config.id, "lens.1");
        assert!(config.account_level.activity && config.bucket_level.cost_optimization);
        assert_eq!(
            config.scope,
            Scope::Include {
                buckets: vec!["arn:aws:s3:::data".into()],
                regions: vec!["us-east-1".into()]
            }
        );
        assert!(
            config
                .export
                .as_ref()
                .is_some_and(|export| export.sse_s3 && export.format == "CSV")
        );
        assert!(config.cloudwatch);
        let rendered = config.render();
        assert_eq!(
            read(&rendered, "us-east-1", "111111111111").unwrap(),
            config
        );
        // Its ARN is not kept: it reads back in another Region.
        assert!(!rendered.contains("StorageLensArn"));
        assert_eq!(
            read(&rendered, "eu-west-1", "111111111111").unwrap(),
            config
        );
    }

    #[test]
    fn configurations_are_refused_for_what_they_lack_or_do_not_build() {
        let levels = "<AccountLevel><BucketLevel/></AccountLevel>";
        for (inner, why) in [
            (format!("{levels}<IsEnabled>true</IsEnabled>"), "no id"),
            (format!("<Id>a</Id>{levels}"), "no IsEnabled"),
            (format!("<Id>a b</Id>{levels}<IsEnabled>true</IsEnabled>"), "an id not allowed"),
            (format!("<Id>a</Id>{levels}<Include><Regions><Region>us-east-1</Region></Regions></Include><Exclude><Regions><Region>us-west-2</Region></Regions></Exclude><IsEnabled>true</IsEnabled>"), "both scopes"),
            (format!("<Id>a</Id>{levels}<Include/><IsEnabled>true</IsEnabled>"), "an empty scope"),
            (format!("<Id>a</Id>{levels}<Exclude><Buckets><Arn>data</Arn></Buckets></Exclude><IsEnabled>true</IsEnabled>"), "no bucket ARN"),
            (format!("<Id>a</Id>{levels}<DataExport><S3BucketDestination><Format>JSON</Format><OutputSchemaVersion>V_1</OutputSchemaVersion><AccountId>111111111111</AccountId><Arn>arn:aws:s3:::r</Arn></S3BucketDestination></DataExport><IsEnabled>true</IsEnabled>"), "a format not known"),
            (format!("<Id>a</Id>{levels}<DataExport><S3BucketDestination><Format>CSV</Format><AccountId>111111111111</AccountId><Arn>arn:aws:s3:::r</Arn></S3BucketDestination></DataExport><IsEnabled>true</IsEnabled>"), "no schema version"),
            (format!("<Id>a</Id>{levels}<StorageLensArn>arn:aws:s3:us-east-1:111111111111:storage-lens/b</StorageLensArn><IsEnabled>true</IsEnabled>"), "another ARN"),
            (format!("<Id>a</Id><AccountLevel><BucketLevel><PrefixLevel><StorageMetrics><SelectionCriteria><MaxDepth>11</MaxDepth></SelectionCriteria></StorageMetrics></PrefixLevel></BucketLevel></AccountLevel><IsEnabled>true</IsEnabled>"), "too deep"),
            (format!("<Id>a</Id>{levels}<Unknown/><IsEnabled>true</IsEnabled>"), "an unknown element"),
            ("<Id>a</Id><IsEnabled>true</IsEnabled>".to_string(), "no account level"),
            ("<Id>a</Id><AccountLevel/><IsEnabled>true</IsEnabled>".to_string(), "no bucket level"),
            ("<Id>a</Id><AccountLevel><ActivityMetrics><IsEnabled>true</IsEnabled></ActivityMetrics><BucketLevel/></AccountLevel><IsEnabled>true</IsEnabled>".to_string(), "levels not consistent"),
            (format!("<Id>a</Id>{levels}<DataExport><CloudWatchMetrics/></DataExport><IsEnabled>true</IsEnabled>"), "CloudWatch without IsEnabled"),
            (format!("<Id>a</Id>{levels}<PrefixDelimiter>//</PrefixDelimiter><IsEnabled>true</IsEnabled>"), "a long delimiter"),
            (format!("<Id>a</Id>{levels}<PrefixDelimiter>&#1;</PrefixDelimiter><IsEnabled>true</IsEnabled>"), "a control character delimiter"),
            (format!("<Id>a</Id>{levels}<PrefixDelimiter>&#xFFFE;</PrefixDelimiter><IsEnabled>true</IsEnabled>"), "a delimiter XML has not"),
            ("<Id>a</Id><AccountLevel><BucketLevel><PrefixLevel><StorageMetrics><SelectionCriteria><Delimiter>-</Delimiter></SelectionCriteria></StorageMetrics></PrefixLevel></BucketLevel></AccountLevel><PrefixDelimiter>/</PrefixDelimiter><IsEnabled>true</IsEnabled>".to_string(), "two delimiters"),
            ("<Id>a</Id><AccountLevel><BucketLevel><PrefixLevel><StorageMetrics><SelectionCriteria><Delimiter>&#xFFFF;</Delimiter></SelectionCriteria></StorageMetrics></PrefixLevel></BucketLevel></AccountLevel><IsEnabled>true</IsEnabled>".to_string(), "a prefix delimiter XML has not"),
            ("<Id>a</Id><AccountLevel><BucketLevel><PrefixLevel><StorageMetrics><SelectionCriteria><Delimiter>&#1;</Delimiter></SelectionCriteria></StorageMetrics></PrefixLevel></BucketLevel></AccountLevel><IsEnabled>true</IsEnabled>".to_string(), "a control character prefix delimiter"),
            (format!("<Id>a</Id>{levels}<Include><Buckets><Arn>arn:aws:s3:::a&#1;b</Arn></Buckets></Include><IsEnabled>true</IsEnabled>"), "a bucket ARN of no bucket name"),
            (format!("<Id>a</Id>{levels}<Include><Buckets><Arn>arn:aws:s3:::A b</Arn></Buckets></Include><IsEnabled>true</IsEnabled>"), "a bucket ARN with a space"),
            (format!("<Id>a</Id>{levels}<IsEnabled>true</IsEnabled></StorageLensConfiguration><StorageLensConfiguration><PrefixDelimiter>/</PrefixDelimiter>"), "the configuration twice"),
            ("<Id>a</Id><AccountLevel><BucketLevel><PrefixLevel><StorageMetrics><SelectionCriteria><MaxDepth>3</MaxDepth><MaxDepth>7</MaxDepth></SelectionCriteria></StorageMetrics></PrefixLevel></BucketLevel></AccountLevel><IsEnabled>true</IsEnabled>".to_string(), "MaxDepth twice"),
            ("<Id>a</Id><AccountLevel><BucketLevel><ActivityMetrics><IsEnabled>true</IsEnabled></ActivityMetrics><ActivityMetrics><IsEnabled>false</IsEnabled></ActivityMetrics></BucketLevel></AccountLevel><IsEnabled>true</IsEnabled>".to_string(), "a level's metrics twice"),
            (format!("<Id>a</Id>{levels}<Include><Buckets><Arn>arn:aws:s3:::{}</Arn></Buckets></Include><IsEnabled>true</IsEnabled>", "x".repeat(116)), "a bucket ARN too long"),
        ] {
            let err = parsed(&inner).expect_err(why);
            assert!(err.status == 400, "{why}: {}", err.code);
        }
        for (inner, why) in [
            (
                "<Id>a</Id><AccountLevel><DetailedStatusCodesMetrics><IsEnabled>true</IsEnabled></DetailedStatusCodesMetrics><BucketLevel/></AccountLevel><IsEnabled>true</IsEnabled>",
                "status codes",
            ),
            (
                "<Id>a</Id><AccountLevel><BucketLevel><AdvancedPerformanceMetrics><IsEnabled>true</IsEnabled></AdvancedPerformanceMetrics></BucketLevel></AccountLevel><IsEnabled>true</IsEnabled>",
                "performance",
            ),
            (
                "<Id>a</Id><AccountLevel><BucketLevel/><StorageLensGroupLevel/></AccountLevel><IsEnabled>true</IsEnabled>",
                "groups",
            ),
            (
                "<Id>a</Id><AccountLevel><BucketLevel/></AccountLevel><AwsOrg><Arn>x</Arn></AwsOrg><IsEnabled>true</IsEnabled>",
                "organizations",
            ),
            (
                "<Id>a</Id><AccountLevel><BucketLevel/></AccountLevel><DataExport><StorageLensTableDestination><IsEnabled>true</IsEnabled></StorageLensTableDestination></DataExport><IsEnabled>true</IsEnabled>",
                "tables",
            ),
        ] {
            assert_eq!(parsed(inner).expect_err(why).status, 501, "{why}");
        }
        // A bucket older than today's naming rules is named in a scope.
        for bucket in [
            "logs.s3-accelerate",
            "My_Legacy_Bucket",
            "fast--use1-az4--x-s3",
        ] {
            let scoped = parsed(&format!(
                "<Id>a</Id><AccountLevel><BucketLevel/></AccountLevel><Include><Buckets><Arn>arn:aws:s3:::{bucket}</Arn></Buckets></Include><IsEnabled>true</IsEnabled>"
            ));
            assert!(scoped.is_ok(), "{bucket}");
        }
        // Turned off, they are taken.
        assert!(parsed("<Id>a</Id><AccountLevel><DetailedStatusCodesMetrics><IsEnabled>false</IsEnabled></DetailedStatusCodesMetrics><BucketLevel/></AccountLevel><IsEnabled>true</IsEnabled>").is_ok());
    }

    #[test]
    fn tags_are_read_once_each() {
        // A Put's tags: in one <Tags>.
        let put = |tags: &str| {
            parse(
                format!("<PutStorageLensConfigurationRequest><StorageLensConfiguration><Id>a</Id><AccountLevel><BucketLevel/></AccountLevel><IsEnabled>true</IsEnabled></StorageLensConfiguration>{tags}</PutStorageLensConfigurationRequest>").as_bytes(),
                "PutStorageLensConfigurationRequest",
                "StorageLensConfiguration",
                &|id| arn("us-east-1", "111111111111", id),
            )
            .map(|(_, tags)| tags)
        };
        assert_eq!(
            put("<Tags><Tag><Key>a</Key><Value>1</Value></Tag></Tags>").unwrap(),
            vec![("a".into(), "1".into())]
        );
        assert!(put("<Tags><Tag><Key>a</Key><Value>1</Value></Tag></Tags><Tags><Tag><Key>b</Key><Value>2</Value></Tag></Tags>").is_err());
        let body = |tags: &str| {
            format!(
                "<PutStorageLensConfigurationTaggingRequest><Tags>{tags}</Tags></PutStorageLensConfigurationTaggingRequest>"
            )
        };
        let tags = parse_tags(
            body("<Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>b</Key><Value></Value></Tag>")
                .as_bytes(),
            "PutStorageLensConfigurationTaggingRequest",
        )
        .unwrap();
        assert_eq!(
            tags,
            vec![("a".into(), "1".into()), ("b".into(), String::new())]
        );
        for bad in [
            body("<Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>a</Key><Value>2</Value></Tag>"),
            body("<Tag><Value>1</Value></Tag>"),
            body(&"<Tag><Key>k</Key><Value>v</Value></Tag>".replace("k", &"k".repeat(129))),
            body("<Tag><Key>a*</Key><Value>1</Value></Tag>"),
            body("</Tags><Tags><Tag><Key>a</Key><Value>1</Value></Tag>"),
            "<PutStorageLensConfigurationTaggingRequest/>".to_string(),
            body("<Tag><Key>a\nb</Key><Value>1</Value></Tag>"),
            body("<Tag><Key>a</Key><Value>x\ty</Value></Tag>"),
        ] {
            assert!(
                parse_tags(bad.as_bytes(), "PutStorageLensConfigurationTaggingRequest").is_err(),
                "{bad}"
            );
        }
    }
}
