//! Bucket event notifications: PUT and GET `?notification` (validated as S3
//! does). A topic configuration's events go to the stream topic its SNS
//! topic ARN names (`arn:aws:sns:<region>:<account>:<topic>`): the account
//! must be the bucket owner's, and the topic one created for that account
//! (`stream create topic <topic> account <account>`); an event to any other
//! topic is dropped. The topic must exist unless
//! `x-amz-skip-destination-validation` is true; each topic a configuration
//! names gets S3's test event. Queue and Lambda destinations
//! and EventBridge are not offered. The object engine keeps the
//! configuration and sends the events (see
//! `truthdb_object::NotificationConfig`).

use std::collections::BTreeSet;

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_object::{Destination, EVENT_TYPES, NotificationConfig, NotificationRule};
use truthdb_streams::NewRecord;

use crate::error::{S3Error, escape};
use crate::object_lock::malformed;
use crate::server::{Authenticated, Body, RequestIds, S3State, full};
use crate::xml::Element;

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const SKIP_VALIDATION_HEADER: &str = "x-amz-skip-destination-validation";
const MAX_BODY: usize = 1024 * 1024;
/// A filter rule's value, and a configuration's id, at most.
const MAX_VALUE: usize = 1024;

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidArgument", message)
}

/// A random configuration id, as S3 assigns one.
fn new_id() -> Result<String, S3Error> {
    use base64::Engine as _;
    use ring::rand::SecureRandom;
    let mut bytes = [0u8; 36];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| S3Error::internal("the system random generator failed"))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// `arn:aws:sns:<region>:<account>:<topic>`, in this server's region.
fn topic_arn(arn: &str, region: &str) -> Result<Destination, S3Error> {
    let fields: Vec<&str> = arn.split(':').collect();
    let refused = || invalid(format!("The ARN is not well formed: {arn}"));
    let ["arn", "aws", "sns", arn_region, account, topic] = fields.as_slice() else {
        return Err(refused());
    };
    if account.len() != 12 || !account.bytes().all(|b| b.is_ascii_digit()) {
        return Err(refused());
    }
    if *arn_region != region {
        return Err(invalid(format!(
            "The notification destination service region is not valid for the bucket location constraint: {arn}"
        )));
    }
    truthdb_streams::valid_topic_name(topic).map_err(|_| refused())?;
    Ok(Destination::Topic(arn.to_string()))
}

fn rule(element: &Element, region: &str) -> Result<NotificationRule, S3Error> {
    element.only(&["Id", "Topic", "Event", "Filter"])?;
    let id = match element.text_of("Id")? {
        Some(id) if id.len() > MAX_VALUE => {
            return Err(invalid("A configuration id is at most 1024 characters"));
        }
        Some(id) if !id.is_empty() => id.to_string(),
        _ => new_id()?,
    };
    let destination = topic_arn(element.text_of("Topic")?.ok_or_else(malformed)?, region)?;
    let mut events = Vec::new();
    for event in element.all("Event") {
        if !EVENT_TYPES.contains(&event.text.as_str()) {
            return Err(invalid(format!(
                "The event is not supported for notifications: {}",
                event.text
            )));
        }
        if !events.contains(&event.text) {
            events.push(event.text.clone());
        }
    }
    if events.is_empty() {
        return Err(malformed());
    }
    let (mut prefix, mut suffix) = (None, None);
    if let Some(filter) = element.one("Filter")? {
        filter.only(&["S3Key"])?;
        if let Some(key) = filter.one("S3Key")? {
            key.only(&["FilterRule"])?;
            for rule in key.all("FilterRule") {
                rule.only(&["Name", "Value"])?;
                let name = rule.text_of("Name")?.ok_or_else(malformed)?;
                let value = rule.text_of("Value")?.unwrap_or_default();
                if value.len() > MAX_VALUE {
                    return Err(invalid("A filter rule value is at most 1024 characters"));
                }
                let slot = match name.to_ascii_lowercase().as_str() {
                    "prefix" => &mut prefix,
                    "suffix" => &mut suffix,
                    _ => {
                        return Err(invalid(format!(
                            "filter rule name must be either prefix or suffix: {name}"
                        )));
                    }
                };
                if slot.is_some() {
                    return Err(invalid(format!(
                        "Cannot specify more than one {} rule in a filter.",
                        name.to_ascii_lowercase()
                    )));
                }
                *slot = Some(value.to_string());
            }
        }
    }
    Ok(NotificationRule {
        id,
        events,
        prefix,
        suffix,
        destination,
    })
}

/// Reads a `NotificationConfiguration` (topic configurations only).
pub fn parse(body: &[u8], region: &str) -> Result<NotificationConfig, S3Error> {
    let root = crate::xml::parse(body, "NotificationConfiguration")?;
    root.only(&[
        "TopicConfiguration",
        "QueueConfiguration",
        "CloudFunctionConfiguration",
        "EventBridgeConfiguration",
    ])?;
    for (name, what) in [
        ("QueueConfiguration", "queue destinations"),
        ("CloudFunctionConfiguration", "Lambda function destinations"),
        ("EventBridgeConfiguration", "EventBridge"),
    ] {
        if root.all(name).next().is_some() {
            return Err(S3Error::not_implemented(format!(
                "Event notifications to {what} are not supported"
            )));
        }
    }
    let rules = root
        .all("TopicConfiguration")
        .map(|element| rule(element, region))
        .collect::<Result<Vec<_>, _>>()?;
    let mut ids = BTreeSet::new();
    if let Some(repeated) = rules.iter().find(|rule| !ids.insert(&rule.id)) {
        return Err(invalid(format!(
            "Configuration id is not unique: {}",
            repeated.id
        )));
    }
    let config = NotificationConfig {
        region: region.to_string(),
        rules,
    };
    if config.overlapping().is_some() {
        return Err(invalid(
            "Configurations overlap. Configurations on the same bucket cannot share a common event type.",
        ));
    }
    Ok(config)
}

pub fn render(config: Option<&NotificationConfig>) -> String {
    let mut out = format!("<NotificationConfiguration xmlns=\"{XMLNS}\">");
    for rule in config.map_or(&[][..], |config| &config.rules) {
        out.push_str("<TopicConfiguration>");
        out.push_str(&format!("<Id>{}</Id>", escape(&rule.id)));
        out.push_str(&format!(
            "<Topic>{}</Topic>",
            escape(rule.destination.arn())
        ));
        for event in &rule.events {
            out.push_str(&format!("<Event>{}</Event>", escape(event)));
        }
        if rule.prefix.is_some() || rule.suffix.is_some() {
            out.push_str("<Filter><S3Key>");
            for (name, value) in [("prefix", &rule.prefix), ("suffix", &rule.suffix)] {
                if let Some(value) = value {
                    out.push_str(&format!(
                        "<FilterRule><Name>{name}</Name><Value>{}</Value></FilterRule>",
                        escape(value)
                    ));
                }
            }
            out.push_str("</S3Key></Filter>");
        }
        out.push_str("</TopicConfiguration>");
    }
    out.push_str("</NotificationConfiguration>");
    out
}

/// PUT or GET `?notification` on `bucket`.
pub async fn bucket_notification(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    method: Method,
) -> Result<Response<Body>, S3Error> {
    let name = bucket.clone();
    let no_bucket =
        || crate::objects::object_error(truthdb_object::ObjectError::NoSuchBucket, &bucket, "");
    if method == Method::GET {
        let config = crate::scope::object(state, move |file| {
            Ok(match file.bucket(&name)? {
                None => Err(()),
                Some(_) => Ok(file.notification(&name)?),
            })
        })
        .await?
        .map_err(|()| no_bucket())?;
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/xml")
            .body(full(bytes::Bytes::from(render(config.as_ref()))))
            .expect("a valid response"));
    }
    let skip_validation = match request.headers().get(SKIP_VALIDATION_HEADER) {
        None => false,
        Some(value) => match value.to_str().map(str::to_ascii_lowercase).as_deref() {
            Ok("true") => true,
            Ok("false") => false,
            _ => {
                return Err(invalid(format!(
                    "Invalid value for {SKIP_VALIDATION_HEADER}"
                )));
            }
        },
    };
    let ids = request
        .extensions()
        .get::<RequestIds>()
        .cloned()
        .ok_or_else(|| S3Error::internal("a request without its ids"))?;
    let body = crate::server::read_body(request, auth, MAX_BODY).await?;
    let config = parse(&body, &state.region)?;
    let refused = || invalid("Unable to validate the following destination configurations");
    // A bucket's events go only to topics its owner owns, named by ARNs of
    // its owner's account.
    let lookup = name.clone();
    let owner = crate::scope::object(state, move |file| {
        Ok(file.bucket(&lookup)?.map(|info| info.owner))
    })
    .await?
    .ok_or_else(no_bucket)?;
    if config
        .rules
        .iter()
        .any(|rule| rule.destination.arn().split(':').nth(4) != Some(owner.as_str()))
    {
        return Err(refused());
    }
    let topics: BTreeSet<String> = config
        .rules
        .iter()
        .map(|rule| rule.destination.topic().to_string())
        .collect();
    if !skip_validation && !topics.is_empty() {
        let (wanted, account) = (topics.clone(), owner.clone());
        let usable = state
            .engine
            .run_engine(move |engine| {
                let existing = engine.streams(|file| file.topics())?;
                Ok(wanted.iter().all(|topic| {
                    existing.iter().any(|(t, _)| {
                        &t.name == topic && t.account.as_deref() == Some(account.as_str())
                    })
                }))
            })
            .await
            .map_err(|err| S3Error::internal(err.to_string()))?;
        if !usable {
            return Err(refused());
        }
    }
    crate::scope::object(state, move |file| {
        if file.bucket(&name)?.is_none() {
            return Ok(Err(()));
        }
        file.transaction(|tx| tx.put_notification(&name, &config))?;
        Ok(Ok(()))
    })
    .await?
    .map_err(|()| no_bucket())?;
    // S3's test event, to each topic named (one gone, or no longer the
    // owner's, gets none).
    if !skip_validation && !topics.is_empty() {
        let message = truthdb_object::test_message(
            &bucket,
            &ids.request_id,
            &ids.host_id,
            crate::objects::now_ms(),
        );
        state
            .engine
            .run_engine(move |engine| {
                engine.streams(|file| {
                    let owned: Vec<String> = file
                        .topics()?
                        .into_iter()
                        .filter(|(topic, _)| {
                            topics.contains(&topic.name)
                                && topic.account.as_deref() == Some(owner.as_str())
                        })
                        .map(|(topic, _)| topic.name)
                        .collect();
                    file.transaction(|tx| {
                        for topic in &owned {
                            let record = NewRecord {
                                key: None,
                                value: message.clone().into_bytes(),
                            };
                            let _ =
                                tx.append(topic, 0, &[record], None, crate::objects::now_ms())?;
                        }
                        Ok(())
                    })
                })
            })
            .await
            .map_err(|err| S3Error::internal(err.to_string()))?;
    }
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(full(bytes::Bytes::new()))
        .expect("a valid response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REGION: &str = "us-east-1";
    const ARN: &str = "arn:aws:sns:us-east-1:123456789012:events";

    fn config(xml: &str) -> Result<NotificationConfig, S3Error> {
        parse(
            format!(
                "<NotificationConfiguration xmlns=\"{XMLNS}\">{xml}</NotificationConfiguration>"
            )
            .as_bytes(),
            REGION,
        )
    }

    fn topic(id: &str, events: &[&str], filter: &str) -> String {
        let events: String = events
            .iter()
            .map(|e| format!("<Event>{e}</Event>"))
            .collect();
        format!(
            "<TopicConfiguration><Id>{id}</Id><Topic>{ARN}</Topic>{events}{filter}</TopicConfiguration>"
        )
    }

    fn code(xml: &str) -> String {
        config(xml).unwrap_err().code.to_string()
    }

    #[test]
    fn a_configuration_reads_and_renders_back_alike() {
        let xml = topic(
            "images",
            &["s3:ObjectCreated:*", "s3:ObjectRemoved:Delete"],
            "<Filter><S3Key><FilterRule><Name>Prefix</Name><Value>images/</Value></FilterRule>\
             <FilterRule><Name>suffix</Name><Value>.jpg &amp; more</Value></FilterRule></S3Key></Filter>",
        ) + &topic(
            "logs",
            &["s3:ObjectCreated:Put"],
            "<Filter><S3Key><FilterRule><Name>prefix</Name><Value>logs/</Value></FilterRule></S3Key></Filter>",
        );
        let parsed = config(&xml).unwrap();
        assert_eq!(parsed.rules.len(), 2);
        assert_eq!(parsed.rules[0].prefix.as_deref(), Some("images/"));
        assert_eq!(parsed.rules[0].suffix.as_deref(), Some(".jpg & more"));
        assert_eq!(parsed.rules[0].destination.topic(), "events");
        let rendered = render(Some(&parsed));
        assert_eq!(
            parse(rendered.as_bytes(), REGION).unwrap(),
            parsed,
            "{rendered}"
        );
        assert_eq!(config("").unwrap().rules, []);
        assert_eq!(
            render(None),
            format!("<NotificationConfiguration xmlns=\"{XMLNS}\"></NotificationConfiguration>")
        );
        // An id is assigned when none is given.
        let assigned = config(&format!(
            "<TopicConfiguration><Topic>{ARN}</Topic><Event>s3:ObjectCreated:*</Event></TopicConfiguration>"
        ))
        .unwrap();
        assert!(assigned.rules[0].id.len() >= 40);
    }

    #[test]
    fn configurations_are_refused_as_s3_refuses_them() {
        let created = &["s3:ObjectCreated:*"];
        for (xml, expected) in [
            (
                topic("a", &["s3:ObjectCreated:Nope"], ""),
                "InvalidArgument",
            ),
            (topic("a", &[], ""), "MalformedXML"),
            (
                topic("a", created, "") + &topic("a", &["s3:ObjectRemoved:*"], ""),
                "InvalidArgument",
            ),
            (
                topic("a", created, "") + &topic("b", &["s3:ObjectCreated:Put"], ""),
                "InvalidArgument",
            ),
            (
                topic(
                    "a",
                    created,
                    "<Filter><S3Key><FilterRule><Name>middle</Name><Value>x</Value></FilterRule></S3Key></Filter>",
                ),
                "InvalidArgument",
            ),
            (
                topic(
                    "a",
                    created,
                    "<Filter><S3Key><FilterRule><Name>prefix</Name><Value>x</Value></FilterRule>\
                     <FilterRule><Name>prefix</Name><Value>y</Value></FilterRule></S3Key></Filter>",
                ),
                "InvalidArgument",
            ),
            (
                format!(
                    "<TopicConfiguration><Topic>arn:aws:sqs:us-east-1:123456789012:q</Topic>\
                     <Event>s3:ObjectCreated:*</Event></TopicConfiguration>"
                ),
                "InvalidArgument",
            ),
            (
                format!(
                    "<TopicConfiguration><Topic>arn:aws:sns:eu-west-1:123456789012:t</Topic>\
                     <Event>s3:ObjectCreated:*</Event></TopicConfiguration>"
                ),
                "InvalidArgument",
            ),
            (
                format!(
                    "<TopicConfiguration><Topic>arn:aws:sns:us-east-1:123456789012:a/b</Topic>\
                     <Event>s3:ObjectCreated:*</Event></TopicConfiguration>"
                ),
                "InvalidArgument",
            ),
            (
                "<QueueConfiguration><Queue>arn:aws:sqs:us-east-1:123456789012:q</Queue>\
                 <Event>s3:ObjectCreated:*</Event></QueueConfiguration>"
                    .to_string(),
                "NotImplemented",
            ),
            ("<EventBridgeConfiguration/>".to_string(), "NotImplemented"),
            ("<Other/>".to_string(), "MalformedXML"),
        ] {
            assert_eq!(code(&xml), expected, "{xml}");
        }
        // Different suffixes do not overlap.
        let filter = |suffix: &str| {
            format!(
                "<Filter><S3Key><FilterRule><Name>suffix</Name><Value>{suffix}</Value></FilterRule></S3Key></Filter>"
            )
        };
        assert!(
            config(&(topic("a", created, &filter(".png")) + &topic("b", created, &filter(".jpg"))))
                .is_ok()
        );
    }
}
