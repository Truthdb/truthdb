//! A bucket's S3 Inventory configurations (`?inventory&id=`): up to 1000 of
//! them, each naming what its report lists (every version or the current
//! ones, under a prefix, with optional fields), where it goes (a bucket of
//! the same owner, CSV or Parquet; ORC and SSE-KMS are not supported) and
//! how often (daily or weekly). The reports themselves are made by the
//! object jobs.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::{BUCKET_INVENTORY, InventoryConfig, InventoryFormat, InventoryFrequency};

use crate::error::{S3Error, escape};
use crate::object_lock::malformed;
use crate::server::{Authenticated, Body, S3State, full};
use crate::xml;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const MAX_BODY: usize = 64 * 1024;
/// The configurations a bucket holds at most, and a listing's page.
const MAX_CONFIGURATIONS: usize = 1000;
const PAGE: usize = 100;
/// The optional fields an inventory list may hold (S3's).
const OPTIONAL_FIELDS: [&str; 19] = [
    "Size",
    "LastModifiedDate",
    "StorageClass",
    "ETag",
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

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

/// Whether `id` is a configuration id: 1 to 64 letters, digits, periods,
/// dashes and underscores.
fn valid_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

/// Parses and checks an InventoryConfiguration, which must be the one `id`
/// names.
pub fn parse(body: &[u8], id: &str) -> Result<InventoryConfig, S3Error> {
    let root = xml::parse(body, "InventoryConfiguration")?;
    root.only(&[
        "Destination",
        "IsEnabled",
        "Filter",
        "Id",
        "IncludedObjectVersions",
        "OptionalFields",
        "Schedule",
    ])?;
    let named = root.text_of("Id")?.ok_or_else(malformed)?;
    if !valid_id(named) {
        return Err(invalid(
            "An inventory configuration's Id is 1 to 64 letters, numbers, periods, dashes and underscores",
        ));
    }
    if named != id {
        return Err(invalid(
            "The Id in the inventory configuration is not the id the request names",
        ));
    }
    let enabled = match root.text_of("IsEnabled")? {
        Some("true") => true,
        Some("false") => false,
        _ => return Err(malformed()),
    };
    let prefix = match root.one("Filter")? {
        None => None,
        Some(filter) => {
            filter.only(&["Prefix"])?;
            Some(filter.text_of("Prefix")?.ok_or_else(malformed)?.to_string())
        }
    };
    let destination = root.one("Destination")?.ok_or_else(malformed)?;
    destination.only(&["S3BucketDestination"])?;
    let target = destination
        .one("S3BucketDestination")?
        .ok_or_else(malformed)?;
    target.only(&["AccountId", "Bucket", "Encryption", "Format", "Prefix"])?;
    let bucket = target.text_of("Bucket")?.ok_or_else(malformed)?;
    let destination_bucket = bucket
        .strip_prefix("arn:aws:s3:::")
        .filter(|name| !name.is_empty() && !name.contains('/'))
        .ok_or_else(|| invalid("The destination Bucket is a bucket ARN (arn:aws:s3:::bucket)"))?
        .to_string();
    let format = match target.text_of("Format")? {
        Some("CSV") => InventoryFormat::Csv,
        Some("Parquet") => InventoryFormat::Parquet,
        Some("ORC") => return Err(invalid("The ORC inventory format is not supported")),
        _ => return Err(malformed()),
    };
    let sse_s3 = match target.one("Encryption")? {
        None => false,
        Some(encryption) => {
            encryption.only(&["SSE-S3", "SSE-KMS"])?;
            if encryption.one("SSE-KMS")?.is_some() {
                return Err(invalid(
                    "SSE-KMS encryption of inventory lists is not supported",
                ));
            }
            encryption.one("SSE-S3")?.is_some()
        }
    };
    let all_versions = match root.text_of("IncludedObjectVersions")? {
        Some("All") => true,
        Some("Current") => false,
        _ => return Err(malformed()),
    };
    let mut optional_fields = Vec::new();
    if let Some(fields) = root.one("OptionalFields")? {
        fields.only(&["Field"])?;
        for field in fields.all("Field") {
            if !OPTIONAL_FIELDS.contains(&field.text.as_str()) {
                return Err(malformed());
            }
            if !optional_fields.contains(&field.text) {
                optional_fields.push(field.text.clone());
            }
        }
    }
    let schedule = root.one("Schedule")?.ok_or_else(malformed)?;
    schedule.only(&["Frequency"])?;
    let frequency = match schedule.text_of("Frequency")? {
        Some("Daily") => InventoryFrequency::Daily,
        Some("Weekly") => InventoryFrequency::Weekly,
        _ => return Err(malformed()),
    };
    Ok(InventoryConfig {
        id: id.to_string(),
        enabled,
        prefix,
        destination: destination_bucket,
        destination_account: target.text_of("AccountId")?.map(str::to_string),
        destination_prefix: target.text_of("Prefix")?.map(str::to_string),
        format,
        sse_s3,
        all_versions,
        optional_fields,
        frequency,
    })
}

/// The configuration as XML, under `element`.
fn render_as(config: &InventoryConfig, element: &str, xmlns: bool) -> String {
    let mut out = match xmlns {
        true => format!("<{element} xmlns=\"{XMLNS}\">"),
        false => format!("<{element}>"),
    };
    out.push_str("<Destination><S3BucketDestination>");
    if let Some(account) = &config.destination_account {
        out.push_str(&format!("<AccountId>{}</AccountId>", escape(account)));
    }
    out.push_str(&format!(
        "<Bucket>arn:aws:s3:::{}</Bucket>",
        escape(&config.destination)
    ));
    out.push_str(&format!(
        "<Format>{}</Format>",
        match config.format {
            InventoryFormat::Csv => "CSV",
            InventoryFormat::Parquet => "Parquet",
        }
    ));
    if let Some(prefix) = &config.destination_prefix {
        out.push_str(&format!("<Prefix>{}</Prefix>", escape(prefix)));
    }
    if config.sse_s3 {
        out.push_str("<Encryption><SSE-S3></SSE-S3></Encryption>");
    }
    out.push_str("</S3BucketDestination></Destination>");
    out.push_str(&format!("<IsEnabled>{}</IsEnabled>", config.enabled));
    if let Some(prefix) = &config.prefix {
        out.push_str(&format!(
            "<Filter><Prefix>{}</Prefix></Filter>",
            escape(prefix)
        ));
    }
    out.push_str(&format!("<Id>{}</Id>", escape(&config.id)));
    out.push_str(&format!(
        "<IncludedObjectVersions>{}</IncludedObjectVersions>",
        if config.all_versions {
            "All"
        } else {
            "Current"
        }
    ));
    if !config.optional_fields.is_empty() {
        out.push_str("<OptionalFields>");
        for field in &config.optional_fields {
            out.push_str(&format!("<Field>{field}</Field>"));
        }
        out.push_str("</OptionalFields>");
    }
    out.push_str(&format!(
        "<Schedule><Frequency>{}</Frequency></Schedule>",
        match config.frequency {
            InventoryFrequency::Daily => "Daily",
            InventoryFrequency::Weekly => "Weekly",
        }
    ));
    out.push_str(&format!("</{element}>"));
    out
}

pub fn render(config: &InventoryConfig) -> String {
    render_as(config, "InventoryConfiguration", true)
}

/// The query's `name` parameter, decoded.
fn query_value(request: &Request<Incoming>, name: &str) -> Result<Option<String>, S3Error> {
    request
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| {
            percent_encoding::percent_decode_str(value)
                .decode_utf8()
                .map(|value| value.into_owned())
                .map_err(|_| invalid(format!("The {name} is not UTF-8")))
        })
        .transpose()
}

fn no_configuration() -> S3Error {
    S3Error::new(
        404,
        "NoSuchConfiguration",
        "The specified configuration does not exist.",
    )
}

fn empty(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(full(bytes::Bytes::new()))
        .expect("a valid response")
}

fn xml_answer(document: String) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{document}"
        )))
        .expect("a valid response")
}

/// PUT, GET or DELETE `?inventory&id=`, or GET `?inventory` (a listing).
pub async fn bucket_inventory(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    let Some(owner) = crate::scope::object(state, move |file| {
        Ok(file.bucket(&name)?.map(|info| info.owner))
    })
    .await?
    else {
        return Err(S3Error::new(
            404,
            "NoSuchBucket",
            format!("The bucket {bucket} does not exist"),
        ));
    };
    let id = query_value(&request, "id")?;
    if method == Method::GET && id.is_none() {
        return list(state, &request, bucket).await;
    }
    let Some(id) = id else {
        return Err(invalid("The request names no id"));
    };
    if !valid_id(&id) {
        return Err(invalid(
            "An inventory configuration's id is 1 to 64 letters, numbers, periods, dashes and underscores",
        ));
    }
    match method {
        Method::PUT => {
            // No checksum needed (the SDKs send none), but one sent is checked.
            let body = if crate::object_lock::has_integrity_check(request.headers()) {
                crate::object_lock::read_checked_body(request, auth, MAX_BODY).await?
            } else {
                crate::server::read_body(request, auth, MAX_BODY).await?
            };
            let config = parse(&body, &id)?;
            if config
                .destination_account
                .as_ref()
                .is_some_and(|account| *account != owner)
            {
                return Err(invalid(
                    "The destination account is not the source bucket owner",
                ));
            }
            let document = config.encode();
            // The lists go to a bucket of the same owner (checked again
            // when one is written).
            let refused = crate::scope::object(state, move |file| {
                match file.bucket(&config.destination)? {
                    None => return Ok(Some("The destination bucket does not exist")),
                    Some(target) if target.owner != owner => {
                        return Ok(Some(
                            "The destination bucket is not the source bucket owner's",
                        ));
                    }
                    Some(_) => {}
                }
                file.transaction(|tx| {
                    let ids = tx.bucket_config_ids(&bucket, BUCKET_INVENTORY)?;
                    if !ids.contains(&id) && ids.len() >= MAX_CONFIGURATIONS {
                        return Ok(Some("TooManyConfigurations"));
                    }
                    tx.put_bucket_config(&bucket, BUCKET_INVENTORY, &id, &document)?;
                    Ok(None)
                })
            })
            .await?;
            match refused {
                None => Ok(empty(StatusCode::OK)),
                Some("TooManyConfigurations") => Err(S3Error::new(
                    400,
                    "TooManyConfigurations",
                    "You are attempting to create a new configuration but have already reached the 1,000-configuration limit.",
                )),
                Some(message) => Err(invalid(message)),
            }
        }
        Method::GET => {
            let stored = crate::scope::object(state, move |file| {
                file.bucket_config(&bucket, BUCKET_INVENTORY, &id)
            })
            .await?
            .and_then(|document| InventoryConfig::decode(&document))
            .ok_or_else(no_configuration)?;
            Ok(xml_answer(render(&stored)))
        }
        Method::DELETE => {
            let deleted = crate::scope::object(state, move |file| {
                file.transaction(|tx| tx.delete_bucket_config(&bucket, BUCKET_INVENTORY, &id))
            })
            .await?;
            if !deleted {
                return Err(no_configuration());
            }
            Ok(empty(StatusCode::NO_CONTENT))
        }
        other => Err(S3Error::not_implemented(format!("{other} ?inventory"))),
    }
}

/// ListBucketInventoryConfigurations: 100 a page, in id order, continued
/// after the id a token names.
async fn list(
    state: &S3State,
    request: &Request<Incoming>,
    bucket: String,
) -> Result<Response<Body>, S3Error> {
    let token = query_value(request, "continuation-token")?;
    let after = token.clone();
    let (page, truncated) = crate::scope::object(state, move |file| {
        let ids = file.bucket_config_ids(&bucket, BUCKET_INVENTORY)?;
        let mut page = Vec::new();
        let mut remaining = ids
            .into_iter()
            .filter(|id| after.as_ref().is_none_or(|after| id > after));
        for id in remaining.by_ref().take(PAGE) {
            if let Some(config) = file
                .bucket_config(&bucket, BUCKET_INVENTORY, &id)?
                .and_then(|document| InventoryConfig::decode(&document))
            {
                page.push(config);
            }
        }
        Ok((page, remaining.next().is_some()))
    })
    .await?;
    let mut out = format!("<ListInventoryConfigurationsResult xmlns=\"{XMLNS}\">");
    out.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
    if let Some(token) = &token {
        out.push_str(&format!(
            "<ContinuationToken>{}</ContinuationToken>",
            escape(token)
        ));
    }
    if truncated && let Some(last) = page.last() {
        out.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape(&last.id)
        ));
    }
    for config in &page {
        out.push_str(&render_as(config, "InventoryConfiguration", false));
    }
    out.push_str("</ListInventoryConfigurationsResult>");
    Ok(xml_answer(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(id: &str, inner: &str) -> Result<InventoryConfig, S3Error> {
        parse(
            format!(
                "<InventoryConfiguration xmlns=\"{XMLNS}\"><Id>{id}</Id>{inner}</InventoryConfiguration>"
            )
            .as_bytes(),
            id,
        )
    }

    const DESTINATION: &str = "<Destination><S3BucketDestination><Bucket>arn:aws:s3:::lists</Bucket>\
        <Format>CSV</Format></S3BucketDestination></Destination>";
    const REST: &str = "<IsEnabled>true</IsEnabled><IncludedObjectVersions>Current</IncludedObjectVersions>\
        <Schedule><Frequency>Daily</Frequency></Schedule>";

    #[test]
    fn configurations_parse_render_and_refuse_as_s3_does() {
        let plain = config("report1", &format!("{DESTINATION}{REST}")).unwrap();
        assert_eq!(plain.destination, "lists");
        assert_eq!(plain.format, InventoryFormat::Csv);
        assert!(plain.enabled && !plain.all_versions && plain.prefix.is_none());
        let full = config(
            "r.2",
            "<Destination><S3BucketDestination><AccountId>111122223333</AccountId>\
             <Bucket>arn:aws:s3:::lists</Bucket><Format>Parquet</Format><Prefix>inv</Prefix>\
             <Encryption><SSE-S3/></Encryption></S3BucketDestination></Destination>\
             <IsEnabled>false</IsEnabled><Filter><Prefix>docs/</Prefix></Filter>\
             <IncludedObjectVersions>All</IncludedObjectVersions>\
             <OptionalFields><Field>Size</Field><Field>ETag</Field><Field>Size</Field></OptionalFields>\
             <Schedule><Frequency>Weekly</Frequency></Schedule>",
        )
        .unwrap();
        assert_eq!(full.optional_fields, ["Size", "ETag"]);
        assert!(full.sse_s3 && full.all_versions && !full.enabled);
        assert_eq!(full.frequency, InventoryFrequency::Weekly);
        for parsed in [plain, full] {
            assert_eq!(
                parse(render(&parsed).as_bytes(), &parsed.id).unwrap(),
                parsed
            );
        }
        let code = |result: Result<InventoryConfig, S3Error>| result.expect_err("refused").code;
        assert_eq!(
            code(config("bad id", &format!("{DESTINATION}{REST}"))),
            "InvalidArgument"
        );
        let orc = DESTINATION.replace("CSV", "ORC");
        assert_eq!(
            code(config("a", &format!("{orc}{REST}"))),
            "InvalidArgument"
        );
        let kms = DESTINATION.replace(
            "<Format>",
            "<Encryption><SSE-KMS><KeyId>k</KeyId></SSE-KMS></Encryption><Format>",
        );
        assert_eq!(
            code(config("a", &format!("{kms}{REST}"))),
            "InvalidArgument"
        );
        let not_arn = DESTINATION.replace("arn:aws:s3:::lists", "lists");
        assert_eq!(
            code(config("a", &format!("{not_arn}{REST}"))),
            "InvalidArgument"
        );
        for broken in [
            format!(
                "{DESTINATION}<IsEnabled>yes</IsEnabled><IncludedObjectVersions>All</IncludedObjectVersions>\
                <Schedule><Frequency>Daily</Frequency></Schedule>"
            ),
            format!(
                "{DESTINATION}<IsEnabled>true</IsEnabled><IncludedObjectVersions>Some</IncludedObjectVersions>\
                <Schedule><Frequency>Daily</Frequency></Schedule>"
            ),
            format!(
                "{DESTINATION}<IsEnabled>true</IsEnabled><IncludedObjectVersions>All</IncludedObjectVersions>\
                <Schedule><Frequency>Hourly</Frequency></Schedule>"
            ),
            format!("{DESTINATION}{REST}<OptionalFields><Field>Color</Field></OptionalFields>"),
            REST.to_string(),
            format!("{DESTINATION}{REST}<Other/>"),
        ] {
            assert_eq!(code(config("a", &broken)), "MalformedXML", "{broken}");
        }
    }
}
