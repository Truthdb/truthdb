//! Bucket lifecycle configuration: PUT, GET and DELETE `?lifecycle`
//! (validated as S3 does) and the `x-amz-expiration` header of the objects
//! it expires. The object engine keeps the rules and applies them in a daily
//! pass (see `truthdb_object::LifecycleConfig`).

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::{
    Expiration, Filter, LifecycleConfig, MinTransitionSize, NoncurrentExpiration,
    NoncurrentTransition, Rule, StorageClass, Transition, When, transition_allowed,
};

use crate::error::{S3Error, escape};
use crate::object_lock::malformed;
use crate::server::{Authenticated, Body, S3State, full};
use crate::xml::Element;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// The header naming which small objects may transition.
pub const MIN_SIZE_HEADER: &str = "x-amz-transition-default-minimum-object-size";

/// A configuration of 1,000 rules fits.
const MAX_BODY: usize = 2 * 1024 * 1024;
const MAX_RULES: usize = 1000;
const MAX_ID: usize = 255;
const DAY_MS: u64 = 86_400_000;

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

/// `x-amz-transition-default-minimum-object-size`: none means the default
/// (no object under 128 KiB transitions).
fn min_size(value: Option<&str>) -> Result<MinTransitionSize, S3Error> {
    match value {
        None | Some("all_storage_classes_128K") => Ok(MinTransitionSize::AllClasses128K),
        Some("varies_by_storage_class") => Ok(MinTransitionSize::VariesByClass),
        Some(other) => Err(invalid(format!(
            "Invalid value {other:?} for {MIN_SIZE_HEADER}"
        ))),
    }
}

fn min_size_name(min_size: MinTransitionSize) -> &'static str {
    match min_size {
        MinTransitionSize::AllClasses128K => "all_storage_classes_128K",
        MinTransitionSize::VariesByClass => "varies_by_storage_class",
    }
}

fn number<T: std::str::FromStr>(text: &str) -> Result<T, S3Error> {
    // Plain digits only: no sign, no spaces.
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed());
    }
    text.parse()
        .map_err(|_| invalid(format!("{text} is out of range")))
}

/// A `Date`: ISO 8601, at midnight UTC.
fn date(text: &str) -> Result<u64, S3Error> {
    let ms = text
        .contains('T')
        .then(|| truthdb_iam::parse_date_ms(text))
        .flatten()
        .and_then(|ms| u64::try_from(ms).ok())
        .ok_or_else(|| invalid(format!("{text:?} is not a valid ISO 8601 date")))?;
    if ms % DAY_MS != 0 {
        return Err(invalid("'Date' must be at midnight GMT"));
    }
    Ok(ms)
}

/// A transition's class: a colder class S3 transitions to.
fn transition_class(text: &str) -> Result<StorageClass, S3Error> {
    match StorageClass::parse(text) {
        Some(class) if transition_allowed(StorageClass::Standard, class) => Ok(class),
        _ => Err(invalid(format!(
            "{text:?} is not a valid transition storage class"
        ))),
    }
}

/// How long a class must be kept before a lifecycle rule moves it on.
fn minimum_days(class: StorageClass) -> u32 {
    match class {
        StorageClass::StandardIa | StorageClass::OnezoneIa => 30,
        StorageClass::GlacierIr | StorageClass::Glacier => 90,
        StorageClass::DeepArchive => 180,
        _ => 0,
    }
}

fn tag(element: &Element) -> Result<(String, String), S3Error> {
    element.only(&["Key", "Value"])?;
    let key = element.text_of("Key")?.ok_or_else(malformed)?;
    let value = element.text_of("Value")?.ok_or_else(malformed)?;
    if key.is_empty() || key.chars().count() > 128 || value.chars().count() > 256 {
        return Err(invalid(
            "A tag's key is 1 to 128 characters, its value at most 256",
        ));
    }
    Ok((key.to_string(), value.to_string()))
}

fn filter(element: &Element) -> Result<Filter, S3Error> {
    element.only(&[
        "Prefix",
        "Tag",
        "ObjectSizeGreaterThan",
        "ObjectSizeLessThan",
        "And",
    ])?;
    if element.children.len() > 1 {
        return Err(malformed());
    }
    let Some(only) = element.children.first() else {
        return Ok(Filter::All);
    };
    Ok(match only.name.as_str() {
        "Prefix" => Filter::Prefix(only.text.clone()),
        "Tag" => {
            let (key, value) = tag(only)?;
            Filter::Tag(key, value)
        }
        "ObjectSizeGreaterThan" => Filter::SizeGreaterThan(number(&only.text)?),
        "ObjectSizeLessThan" => Filter::SizeLessThan(number(&only.text)?),
        _ => {
            only.only(&[
                "Prefix",
                "Tag",
                "ObjectSizeGreaterThan",
                "ObjectSizeLessThan",
            ])?;
            let tags = only.all("Tag").map(tag).collect::<Result<Vec<_>, _>>()?;
            let mut keys: Vec<&str> = tags.iter().map(|(key, _)| key.as_str()).collect();
            keys.sort_unstable();
            if keys.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(invalid("Duplicate Tag Keys are not allowed."));
            }
            let size_greater_than = only
                .text_of("ObjectSizeGreaterThan")?
                .map(number)
                .transpose()?;
            let size_less_than = only
                .text_of("ObjectSizeLessThan")?
                .map(number)
                .transpose()?;
            if let (Some(greater), Some(less)) = (size_greater_than, size_less_than)
                && greater >= less
            {
                return Err(invalid(
                    "ObjectSizeGreaterThan must be less than ObjectSizeLessThan",
                ));
            }
            Filter::And {
                prefix: only.text_of("Prefix")?.map(str::to_string),
                tags,
                size_greater_than,
                size_less_than,
            }
        }
    })
}

/// `Days` or `Date`, exactly one; `Days` from `min_days` up.
fn when(element: &Element, min_days: u32) -> Result<When, S3Error> {
    match (element.text_of("Days")?, element.text_of("Date")?) {
        (Some(days), None) => {
            let days: u32 = number(days)?;
            if days < min_days {
                return Err(invalid(format!("'Days' must be at least {min_days}")));
            }
            Ok(When::Days(days))
        }
        (None, Some(text)) => Ok(When::Date(date(text)?)),
        _ => Err(malformed()),
    }
}

/// `NewerNoncurrentVersions`: 1 to 100, and only in a rule with a Filter.
fn newer_versions(element: &Element, has_filter: bool) -> Result<Option<u32>, S3Error> {
    let Some(text) = element.text_of("NewerNoncurrentVersions")? else {
        return Ok(None);
    };
    if !has_filter {
        return Err(S3Error::invalid_request(
            "NewerNoncurrentVersions needs a Filter in its rule",
        ));
    }
    let newer: u32 = number(text)?;
    if !(1..=100).contains(&newer) {
        return Err(invalid("'NewerNoncurrentVersions' must be 1 to 100"));
    }
    Ok(Some(newer))
}

fn rule(element: &Element) -> Result<Rule, S3Error> {
    element.only(&[
        "ID",
        "Status",
        "Filter",
        "Prefix",
        "Expiration",
        "Transition",
        "NoncurrentVersionExpiration",
        "NoncurrentVersionTransition",
        "AbortIncompleteMultipartUpload",
    ])?;
    let id = element.text_of("ID")?.unwrap_or("").to_string();
    if id.chars().count() > MAX_ID {
        return Err(invalid("ID length should not exceed allowed limit of 255"));
    }
    let enabled = match element.text_of("Status")? {
        Some("Enabled") => true,
        Some("Disabled") => false,
        _ => return Err(malformed()),
    };
    let filter = match (element.one("Filter")?, element.text_of("Prefix")?) {
        (Some(given), None) => filter(given)?,
        (None, Some(prefix)) => Filter::Legacy(prefix.to_string()),
        _ => return Err(malformed()),
    };
    let has_filter = element.one("Filter")?.is_some();
    let expiration = match element.one("Expiration")? {
        None => None,
        Some(given) => {
            given.only(&["Days", "Date", "ExpiredObjectDeleteMarker"])?;
            match given.text_of("ExpiredObjectDeleteMarker")? {
                Some(_) if given.children.len() > 1 => {
                    return Err(invalid(
                        "ExpiredObjectDeleteMarker cannot be specified with Days or Date",
                    ));
                }
                Some(text) => Some(Expiration::ExpiredObjectDeleteMarker(match text {
                    "true" => true,
                    "false" => false,
                    _ => return Err(malformed()),
                })),
                None => Some(Expiration::At(when(given, 1)?)),
            }
        }
    };
    if matches!(expiration, Some(Expiration::ExpiredObjectDeleteMarker(_))) && filter.uses_tags() {
        return Err(invalid(
            "ExpiredObjectDeleteMarker cannot be specified with object tags",
        ));
    }
    let transitions = element
        .all("Transition")
        .map(|given| {
            given.only(&["Days", "Date", "StorageClass"])?;
            Ok(Transition {
                when: when(given, 0)?,
                class: transition_class(given.text_of("StorageClass")?.ok_or_else(malformed)?)?,
            })
        })
        .collect::<Result<Vec<_>, S3Error>>()?;
    check_transitions(&transitions)?;
    let noncurrent_expiration = element
        .one("NoncurrentVersionExpiration")?
        .map(|given| {
            given.only(&["NoncurrentDays", "NewerNoncurrentVersions"])?;
            let days: u32 = number(given.text_of("NoncurrentDays")?.ok_or_else(malformed)?)?;
            if days == 0 {
                return Err(invalid("'NoncurrentDays' must be greater than 0"));
            }
            Ok(NoncurrentExpiration {
                days,
                newer_versions: newer_versions(given, has_filter)?,
            })
        })
        .transpose()?;
    let noncurrent_transitions = element
        .all("NoncurrentVersionTransition")
        .map(|given| {
            given.only(&["NoncurrentDays", "NewerNoncurrentVersions", "StorageClass"])?;
            Ok(NoncurrentTransition {
                days: number(given.text_of("NoncurrentDays")?.ok_or_else(malformed)?)?,
                newer_versions: newer_versions(given, has_filter)?,
                class: transition_class(given.text_of("StorageClass")?.ok_or_else(malformed)?)?,
            })
        })
        .collect::<Result<Vec<_>, S3Error>>()?;
    check_transitions(
        &noncurrent_transitions
            .iter()
            .map(|transition| Transition {
                when: When::Days(transition.days),
                class: transition.class,
            })
            .collect::<Vec<_>>(),
    )?;
    let abort_upload_days = element
        .one("AbortIncompleteMultipartUpload")?
        .map(|given| {
            given.only(&["DaysAfterInitiation"])?;
            number(
                given
                    .text_of("DaysAfterInitiation")?
                    .ok_or_else(malformed)?,
            )
        })
        .transpose()?;
    if abort_upload_days.is_some() && (filter.uses_tags() || filter.uses_size()) {
        return Err(invalid(
            "AbortIncompleteMultipartUpload cannot be specified with object tags or sizes",
        ));
    }
    if expiration.is_none()
        && transitions.is_empty()
        && noncurrent_expiration.is_none()
        && noncurrent_transitions.is_empty()
        && abort_upload_days.is_none()
    {
        return Err(invalid(
            "At least one action needs to be specified in a rule",
        ));
    }
    Ok(Rule {
        id,
        enabled,
        filter,
        expiration,
        transitions,
        noncurrent_expiration,
        noncurrent_transitions,
        abort_upload_days,
    })
}

/// A rule's transitions are all by days or all by date, go colder each
/// time, and keep each class its minimum time before moving it on.
fn check_transitions(transitions: &[Transition]) -> Result<(), S3Error> {
    let mut sorted: Vec<(u64, StorageClass, bool)> = transitions
        .iter()
        .map(|transition| match transition.when {
            When::Days(days) => (u64::from(days) * DAY_MS, transition.class, true),
            When::Date(date_ms) => (date_ms, transition.class, false),
        })
        .collect();
    if sorted.windows(2).any(|pair| pair[0].2 != pair[1].2) {
        return Err(invalid(
            "Found mixed 'Date' and 'Days' based Transition actions in a lifecycle rule",
        ));
    }
    sorted.sort_by_key(|(at, _, _)| *at);
    for pair in sorted.windows(2) {
        let ((first_at, first, _), (then_at, then, _)) = (pair[0], pair[1]);
        if !transition_allowed(first, then) {
            return Err(invalid(format!(
                "A rule cannot transition from {} to {}",
                first.as_str(),
                then.as_str()
            )));
        }
        if then_at < first_at + u64::from(minimum_days(first)) * DAY_MS {
            return Err(invalid(format!(
                "Objects must be kept in {} for at least {} days before they transition again",
                first.as_str(),
                minimum_days(first)
            )));
        }
    }
    Ok(())
}

/// A PutBucketLifecycleConfiguration body and its minimum-size header.
pub fn parse(body: &[u8], min_size_header: Option<&str>) -> Result<LifecycleConfig, S3Error> {
    let root = crate::xml::parse(body, "LifecycleConfiguration")?;
    root.only(&["Rule"])?;
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
    Ok(LifecycleConfig {
        min_size: min_size(min_size_header)?,
        rules,
    })
}

fn render_when(out: &mut String, when: When) {
    match when {
        When::Days(days) => out.push_str(&format!("<Days>{days}</Days>")),
        When::Date(date_ms) => {
            out.push_str(&format!("<Date>{}</Date>", crate::time::iso8601(date_ms)))
        }
    }
}

fn render_tag(out: &mut String, (key, value): &(String, String)) {
    out.push_str(&format!(
        "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
        escape(key),
        escape(value)
    ));
}

/// The configuration as GetBucketLifecycleConfiguration returns it.
pub fn render(config: &LifecycleConfig) -> String {
    let mut out = format!("<LifecycleConfiguration xmlns=\"{XMLNS}\">");
    for rule in &config.rules {
        out.push_str("<Rule>");
        if !rule.id.is_empty() {
            out.push_str(&format!("<ID>{}</ID>", escape(&rule.id)));
        }
        match &rule.filter {
            Filter::Legacy(prefix) => out.push_str(&format!("<Prefix>{}</Prefix>", escape(prefix))),
            Filter::All => out.push_str("<Filter></Filter>"),
            Filter::Prefix(prefix) => out.push_str(&format!(
                "<Filter><Prefix>{}</Prefix></Filter>",
                escape(prefix)
            )),
            Filter::Tag(key, value) => {
                out.push_str("<Filter>");
                render_tag(&mut out, &(key.clone(), value.clone()));
                out.push_str("</Filter>");
            }
            Filter::SizeGreaterThan(bound) => out.push_str(&format!(
                "<Filter><ObjectSizeGreaterThan>{bound}</ObjectSizeGreaterThan></Filter>"
            )),
            Filter::SizeLessThan(bound) => out.push_str(&format!(
                "<Filter><ObjectSizeLessThan>{bound}</ObjectSizeLessThan></Filter>"
            )),
            Filter::And {
                prefix,
                tags,
                size_greater_than,
                size_less_than,
            } => {
                out.push_str("<Filter><And>");
                if let Some(prefix) = prefix {
                    out.push_str(&format!("<Prefix>{}</Prefix>", escape(prefix)));
                }
                for tag in tags {
                    render_tag(&mut out, tag);
                }
                if let Some(bound) = size_greater_than {
                    out.push_str(&format!(
                        "<ObjectSizeGreaterThan>{bound}</ObjectSizeGreaterThan>"
                    ));
                }
                if let Some(bound) = size_less_than {
                    out.push_str(&format!("<ObjectSizeLessThan>{bound}</ObjectSizeLessThan>"));
                }
                out.push_str("</And></Filter>");
            }
        }
        out.push_str(if rule.enabled {
            "<Status>Enabled</Status>"
        } else {
            "<Status>Disabled</Status>"
        });
        for transition in &rule.transitions {
            out.push_str("<Transition>");
            render_when(&mut out, transition.when);
            out.push_str(&format!(
                "<StorageClass>{}</StorageClass></Transition>",
                transition.class.as_str()
            ));
        }
        match rule.expiration {
            Some(Expiration::At(when)) => {
                out.push_str("<Expiration>");
                render_when(&mut out, when);
                out.push_str("</Expiration>");
            }
            Some(Expiration::ExpiredObjectDeleteMarker(on)) => out.push_str(&format!(
                "<Expiration><ExpiredObjectDeleteMarker>{on}</ExpiredObjectDeleteMarker></Expiration>"
            )),
            None => {}
        }
        let newer = |newer: Option<u32>| {
            newer
                .map(|count| format!("<NewerNoncurrentVersions>{count}</NewerNoncurrentVersions>"))
                .unwrap_or_default()
        };
        for transition in &rule.noncurrent_transitions {
            out.push_str(&format!(
                "<NoncurrentVersionTransition><NoncurrentDays>{}</NoncurrentDays>{}\
                 <StorageClass>{}</StorageClass></NoncurrentVersionTransition>",
                transition.days,
                newer(transition.newer_versions),
                transition.class.as_str()
            ));
        }
        if let Some(expiration) = rule.noncurrent_expiration {
            out.push_str(&format!(
                "<NoncurrentVersionExpiration><NoncurrentDays>{}</NoncurrentDays>{}\
                 </NoncurrentVersionExpiration>",
                expiration.days,
                newer(expiration.newer_versions)
            ));
        }
        if let Some(days) = rule.abort_upload_days {
            out.push_str(&format!(
                "<AbortIncompleteMultipartUpload><DaysAfterInitiation>{days}\
                 </DaysAfterInitiation></AbortIncompleteMultipartUpload>"
            ));
        }
        out.push_str("</Rule>");
    }
    out.push_str("</LifecycleConfiguration>");
    out
}

/// The `x-amz-expiration` value for the current version of `key` (with
/// this size, tags and write time), when a rule expires it.
pub fn expiration_header(
    config: Option<&LifecycleConfig>,
    key: &str,
    size: u64,
    tags: &[(String, String)],
    mtime_ms: u64,
) -> Option<String> {
    let (at, rule_id) = config?.expiration_of(key, size, tags, mtime_ms)?;
    Some(format!(
        "expiry-date=\"{}\", rule-id=\"{}\"",
        crate::objects::http_date(at),
        crate::sigv4::encode_key(rule_id).replace('/', "%2F")
    ))
}

/// PUT, GET or DELETE `?lifecycle` on `bucket`.
pub async fn bucket_lifecycle(
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
                    Some(_) => Ok(file.lifecycle(&name)?),
                })
            })
            .await?
            .map_err(|()| no_bucket())?;
            let config = found.ok_or_else(|| {
                S3Error::new(
                    404,
                    "NoSuchLifecycleConfiguration",
                    "The lifecycle configuration does not exist",
                )
            })?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .header(MIN_SIZE_HEADER, min_size_name(config.min_size))
                .body(full(bytes::Bytes::from(render(&config))))
                .expect("a valid response"))
        }
        Method::PUT => {
            let header = request
                .headers()
                .get(MIN_SIZE_HEADER)
                .map(|value| value.to_str().map(str::to_string))
                .transpose()
                .map_err(|_| invalid(format!("Invalid value for {MIN_SIZE_HEADER}")))?;
            let body = crate::server::read_body(request, auth, MAX_BODY).await?;
            let config = parse(&body, header.as_deref())?;
            let min = config.min_size;
            crate::scope::object(state, move |file| {
                if file.bucket(&name)?.is_none() {
                    return Ok(Err(()));
                }
                file.transaction(|tx| tx.put_lifecycle(&name, &config, crate::objects::now_ms()))?;
                Ok(Ok(()))
            })
            .await?
            .map_err(|()| no_bucket())?;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(MIN_SIZE_HEADER, min_size_name(min))
                .body(full(bytes::Bytes::new()))
                .expect("a valid response"))
        }
        _ => {
            crate::scope::object(state, move |file| {
                if file.bucket(&name)?.is_none() {
                    return Ok(Err(()));
                }
                file.transaction(|tx| tx.delete_lifecycle(&name))?;
                Ok(Ok(()))
            })
            .await?
            .map_err(|()| no_bucket())?;
            Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(full(bytes::Bytes::new()))
                .expect("a valid response"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(xml: &str) -> Result<LifecycleConfig, S3Error> {
        parse(
            format!("<LifecycleConfiguration xmlns=\"{XMLNS}\">{xml}</LifecycleConfiguration>")
                .as_bytes(),
            None,
        )
    }

    fn code(xml: &str) -> String {
        rules(xml).unwrap_err().code.to_string()
    }

    #[test]
    fn a_configuration_reads_and_renders_back_alike() {
        let xml = format!(
            "<LifecycleConfiguration xmlns=\"{XMLNS}\">\
             <Rule><ID>logs/cold</ID><Filter><And><Prefix>logs/</Prefix>\
             <Tag><Key>a</Key><Value>b &amp; c</Value></Tag>\
             <ObjectSizeGreaterThan>500</ObjectSizeGreaterThan>\
             <ObjectSizeLessThan>64000</ObjectSizeLessThan></And></Filter>\
             <Status>Enabled</Status>\
             <Transition><Days>30</Days><StorageClass>STANDARD_IA</StorageClass></Transition>\
             <Transition><Days>120</Days><StorageClass>GLACIER</StorageClass></Transition>\
             <Expiration><Days>365</Days></Expiration>\
             <NoncurrentVersionTransition><NoncurrentDays>0</NoncurrentDays>\
             <NewerNoncurrentVersions>3</NewerNoncurrentVersions>\
             <StorageClass>DEEP_ARCHIVE</StorageClass></NoncurrentVersionTransition>\
             <NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays>\
             </NoncurrentVersionExpiration></Rule>\
             <Rule><Prefix>old/</Prefix><Status>Disabled</Status>\
             <Expiration><Date>2030-01-01T00:00:00.000Z</Date></Expiration>\
             <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation>\
             </AbortIncompleteMultipartUpload></Rule>\
             <Rule><Filter></Filter><Status>Enabled</Status>\
             <Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>\
             </Expiration></Rule>\
             <Rule><Filter><Tag><Key>k</Key><Value></Value></Tag></Filter>\
             <Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>\
             <Rule><Filter><ObjectSizeLessThan>9</ObjectSizeLessThan></Filter>\
             <Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>\
             </LifecycleConfiguration>"
        );
        let config = parse(xml.as_bytes(), Some("varies_by_storage_class")).unwrap();
        assert_eq!(config.min_size, MinTransitionSize::VariesByClass);
        assert_eq!(config.rules.len(), 5);
        assert_eq!(config.rules[1].filter, Filter::Legacy("old/".into()));
        assert_eq!(config.rules[2].filter, Filter::All);
        assert_eq!(
            config.rules[1].expiration,
            Some(Expiration::At(When::Date(1_893_456_000_000)))
        );
        let rendered = render(&config);
        assert_eq!(
            parse(rendered.as_bytes(), Some("varies_by_storage_class")).unwrap(),
            config
        );
        assert!(rendered.contains("<ID>logs/cold</ID>"));
        assert!(rendered.contains("<Value>b &amp; c</Value>"));
    }

    #[test]
    fn invalid_configurations_are_refused_as_s3_refuses_them() {
        let ok = "<Status>Enabled</Status><Expiration><Days>1</Days></Expiration>";
        assert!(rules(&format!("<Rule><Filter/>{ok}</Rule>")).is_ok());
        for (xml, wanted) in [
            // No rule, no status, an unknown status, an unknown element.
            ("", "MalformedXML"),
            (
                "<Rule><Filter/><Expiration><Days>1</Days></Expiration></Rule>",
                "MalformedXML",
            ),
            (
                "<Rule><Filter/><Status>On</Status><Expiration><Days>1</Days></Expiration></Rule>",
                "MalformedXML",
            ),
            (
                &format!("<Rule><Filter/>{ok}<Bogus/></Rule>"),
                "MalformedXML",
            ),
            // Filter and Prefix, or neither.
            (
                &format!("<Rule><Filter/><Prefix>a</Prefix>{ok}</Rule>"),
                "MalformedXML",
            ),
            (&format!("<Rule>{ok}</Rule>"), "MalformedXML"),
            // Two filter conditions outside And.
            (
                &format!(
                    "<Rule><Filter><Prefix>a</Prefix><ObjectSizeLessThan>5</ObjectSizeLessThan></Filter>{ok}</Rule>"
                ),
                "MalformedXML",
            ),
            // No action.
            (
                "<Rule><Filter/><Status>Enabled</Status></Rule>",
                "InvalidArgument",
            ),
            // Expiration: zero days, Days with Date, a date not at midnight,
            // a delete-marker cleanup with days or with tags.
            (
                "<Rule><Filter/><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><Filter/><Status>Enabled</Status><Expiration><Days>1</Days><Date>2030-01-01T00:00:00Z</Date></Expiration></Rule>",
                "MalformedXML",
            ),
            (
                "<Rule><Filter/><Status>Enabled</Status><Expiration><Date>2030-01-01T10:00:00Z</Date></Expiration></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><Filter/><Status>Enabled</Status><Expiration><Days>1</Days><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><Filter><Tag><Key>a</Key><Value>b</Value></Tag></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>",
                "InvalidArgument",
            ),
            // Transitions: an unknown or warmer class, a class kept too
            // short, a way S3 does not go, days and dates mixed.
            (
                "<Rule><Filter/><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>STANDARD</StorageClass></Transition></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><Filter/><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>STANDARD_IA</StorageClass></Transition><Transition><Days>20</Days><StorageClass>GLACIER</StorageClass></Transition></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><Filter/><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>ONEZONE_IA</StorageClass></Transition><Transition><Days>90</Days><StorageClass>GLACIER_IR</StorageClass></Transition></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><Filter/><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>STANDARD_IA</StorageClass></Transition><Transition><Date>2030-01-01T00:00:00Z</Date><StorageClass>GLACIER</StorageClass></Transition></Rule>",
                "InvalidArgument",
            ),
            // Noncurrent: zero days; newer versions out of range or without
            // a Filter.
            (
                "<Rule><Filter/><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>0</NoncurrentDays></NoncurrentVersionExpiration></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><Filter/><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>101</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule>",
                "InvalidArgument",
            ),
            (
                "<Rule><Prefix/><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule>",
                "InvalidRequest",
            ),
            // Aborting uploads by tag; tags given twice; sizes crossed.
            (
                "<Rule><Filter><Tag><Key>a</Key><Value>b</Value></Tag></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>",
                "InvalidArgument",
            ),
            (
                &format!(
                    "<Rule><Filter><And><Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>a</Key><Value>2</Value></Tag></And></Filter>{ok}</Rule>"
                ),
                "InvalidArgument",
            ),
            (
                &format!(
                    "<Rule><Filter><And><ObjectSizeGreaterThan>9</ObjectSizeGreaterThan><ObjectSizeLessThan>9</ObjectSizeLessThan></And></Filter>{ok}</Rule>"
                ),
                "InvalidArgument",
            ),
            // A negative or signed number.
            (
                "<Rule><Filter/><Status>Enabled</Status><Expiration><Days>-1</Days></Expiration></Rule>",
                "MalformedXML",
            ),
            // Two rules with one ID; an ID too long.
            (
                &format!(
                    "<Rule><ID>x</ID><Filter/>{ok}</Rule><Rule><ID>x</ID><Filter/>{ok}</Rule>"
                ),
                "InvalidArgument",
            ),
            (
                &format!("<Rule><ID>{}</ID><Filter/>{ok}</Rule>", "i".repeat(256)),
                "InvalidArgument",
            ),
        ] {
            assert_eq!(code(xml), wanted, "{xml}");
        }
        let too_many = format!("<Rule><Filter/>{ok}</Rule>").repeat(1001);
        assert_eq!(code(&too_many), "MalformedXML");
        assert_eq!(
            parse(
                format!(
                    "<LifecycleConfiguration><Rule><Filter/>{ok}</Rule></LifecycleConfiguration>"
                )
                .as_bytes(),
                Some("tiny")
            )
            .unwrap_err()
            .code,
            "InvalidArgument"
        );
    }

    #[test]
    fn the_expiration_header_names_its_date_and_rule() {
        let config = rules(
            "<Rule><ID>a rule/1</ID><Filter/><Status>Enabled</Status>\
             <Expiration><Days>1</Days></Expiration></Rule>",
        )
        .unwrap();
        assert_eq!(
            expiration_header(Some(&config), "k", 1, &[], 1_356_000_000_000).unwrap(),
            "expiry-date=\"Sat, 22 Dec 2012 00:00:00 GMT\", rule-id=\"a%20rule%2F1\""
        );
        assert_eq!(expiration_header(None, "k", 1, &[], 0), None);
    }
}
