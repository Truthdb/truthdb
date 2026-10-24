//! A trail's event selectors (PutEventSelectors): which events it logs.
//! Without any, a trail logs every management event and no data event.
//! Basic selectors (at most 5, 250 data resources in all) choose by
//! read/write type, management events or not (and their sources to leave
//! out), and S3 objects by ARN prefix (`arn:aws:s3` for all; data resources
//! of other types are refused). Advanced selectors choose by fields
//! (`eventCategory`, which each must name, `eventSource`, `eventName`,
//! `readOnly`, `resources.type`, `resources.ARN`) with `Equals`,
//! `StartsWith`, `EndsWith` and their negations, 500 values in all. An event
//! is logged if it matches any selector.

use serde_json::{Value, json};

use crate::cloudtrail::CtError;

const MAX_SELECTORS: usize = 5;
const MAX_DATA_RESOURCES: usize = 250;
const MAX_ADVANCED_VALUES: usize = 500;
const S3_OBJECT: &str = "AWS::S3::Object";
const FIELDS: [&str; 6] = [
    "eventCategory",
    "eventSource",
    "eventName",
    "readOnly",
    "resources.type",
    "resources.ARN",
];
const CONDITIONS: [&str; 6] = [
    "Equals",
    "StartsWith",
    "EndsWith",
    "NotEquals",
    "NotStartsWith",
    "NotEndsWith",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadWrite {
    All,
    ReadOnly,
    WriteOnly,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Basic {
    read_write: ReadWrite,
    management: bool,
    excluded_sources: Vec<String>,
    /// S3 object ARN prefixes (`arn:aws:s3` for all).
    objects: Vec<String>,
}

/// One field selector: the field and its conditions (name, values).
#[derive(Debug, Clone, PartialEq)]
struct Field {
    field: String,
    conditions: Vec<(String, Vec<String>)>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Advanced {
    name: Option<String>,
    fields: Vec<Field>,
}

/// A trail's selectors.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Selectors {
    Basic(Vec<Basic>),
    Advanced(Vec<Advanced>),
}

fn invalid(message: impl Into<String>) -> CtError {
    CtError::new(400, "InvalidEventSelectorsException", message)
}

fn strings(value: &Value, what: &str) -> Result<Vec<String>, CtError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| invalid(format!("{what} holds strings.")))
            })
            .collect(),
        _ => Err(invalid(format!("{what} is a list."))),
    }
}

impl Selectors {
    /// The selectors a PutEventSelectors call sets (one kind or the other).
    pub(crate) fn parse(
        basic: Option<&Value>,
        advanced: Option<&Value>,
    ) -> Result<Selectors, CtError> {
        match (
            basic.filter(|v| !v.is_null()),
            advanced.filter(|v| !v.is_null()),
        ) {
            (Some(_), Some(_)) => Err(CtError::new(
                400,
                "InvalidParameterCombinationException",
                "EventSelectors and AdvancedEventSelectors cannot both be given.",
            )),
            (None, None) => Err(invalid(
                "Either EventSelectors or AdvancedEventSelectors is given.",
            )),
            (Some(basic), None) => Selectors::parse_basic(basic),
            (None, Some(advanced)) => Selectors::parse_advanced(advanced),
        }
    }

    fn parse_basic(value: &Value) -> Result<Selectors, CtError> {
        let selectors = value
            .as_array()
            .ok_or_else(|| invalid("EventSelectors is a list."))?;
        if selectors.is_empty() || selectors.len() > MAX_SELECTORS {
            return Err(invalid("A trail has 1 to 5 event selectors."));
        }
        let mut parsed = Vec::new();
        let mut resources = 0;
        for selector in selectors {
            let read_write = match selector["ReadWriteType"].as_str() {
                None | Some("All") => ReadWrite::All,
                Some("ReadOnly") => ReadWrite::ReadOnly,
                Some("WriteOnly") => ReadWrite::WriteOnly,
                Some(other) => return Err(invalid(format!("{other} is not a ReadWriteType."))),
            };
            let mut objects = Vec::new();
            for resource in selector["DataResources"].as_array().into_iter().flatten() {
                if resource["Type"].as_str() != Some(S3_OBJECT) {
                    return Err(invalid(format!(
                        "Data resources of type {} are not supported.",
                        resource["Type"]
                    )));
                }
                for value in strings(&resource["Values"], "Values")? {
                    let ok = value == "arn:aws:s3"
                        || value
                            .strip_prefix("arn:aws:s3:::")
                            .and_then(|rest| rest.split_once('/'))
                            .is_some_and(|(bucket, _)| !bucket.is_empty());
                    if !ok {
                        return Err(invalid(format!("{value} is not an S3 object ARN prefix.")));
                    }
                    objects.push(value);
                    resources += 1;
                }
            }
            parsed.push(Basic {
                read_write,
                management: selector["IncludeManagementEvents"]
                    .as_bool()
                    .unwrap_or(true),
                excluded_sources: strings(
                    &selector["ExcludeManagementEventSources"],
                    "ExcludeManagementEventSources",
                )?,
                objects,
            });
        }
        if resources > MAX_DATA_RESOURCES {
            return Err(invalid("A trail has at most 250 data resources."));
        }
        Ok(Selectors::Basic(parsed))
    }

    fn parse_advanced(value: &Value) -> Result<Selectors, CtError> {
        let selectors = value
            .as_array()
            .ok_or_else(|| invalid("AdvancedEventSelectors is a list."))?;
        if selectors.is_empty() {
            return Err(invalid(
                "AdvancedEventSelectors holds at least one selector.",
            ));
        }
        let mut parsed = Vec::new();
        let mut values = 0;
        for selector in selectors {
            let mut fields: Vec<Field> = Vec::new();
            for field in selector["FieldSelectors"].as_array().into_iter().flatten() {
                let name = field["Field"].as_str().unwrap_or_default();
                if !FIELDS.contains(&name) {
                    return Err(invalid(format!("{name} is not a supported field.")));
                }
                if fields.iter().any(|known| known.field == name) {
                    return Err(invalid(format!("The field {name} is given twice.")));
                }
                if let Some(object) = field.as_object()
                    && let Some(unknown) = object
                        .keys()
                        .find(|key| *key != "Field" && !CONDITIONS.contains(&key.as_str()))
                {
                    return Err(invalid(format!("{unknown} is not a condition.")));
                }
                let mut conditions = Vec::new();
                for condition in CONDITIONS {
                    let given = strings(&field[condition], condition)?;
                    if !given.is_empty() {
                        values += given.len();
                        conditions.push((condition.to_string(), given));
                    }
                }
                if conditions.is_empty() {
                    return Err(invalid(format!("The field {name} has no condition.")));
                }
                // These take Equals only (as CloudTrail).
                if matches!(name, "eventCategory" | "readOnly" | "resources.type")
                    && conditions
                        .iter()
                        .any(|(condition, _)| condition != "Equals")
                {
                    return Err(invalid(format!("The field {name} takes Equals only.")));
                }
                fields.push(Field {
                    field: name.to_string(),
                    conditions,
                });
            }
            if !fields.iter().any(|field| field.field == "eventCategory") {
                return Err(invalid("Each advanced event selector names eventCategory."));
            }
            let data = fields.iter().any(|field| {
                field.field == "eventCategory"
                    && field
                        .conditions
                        .iter()
                        .any(|(_, values)| values.iter().any(|value| value == "Data"))
            });
            if data && !fields.iter().any(|field| field.field == "resources.type") {
                return Err(invalid("A data event selector names resources.type."));
            }
            // Management or Data events; a management selector chooses by
            // eventCategory, eventSource and readOnly only.
            for field in fields.iter().filter(|field| field.field == "eventCategory") {
                for (_, values) in &field.conditions {
                    if let Some(other) = values
                        .iter()
                        .find(|value| !matches!(value.as_str(), "Management" | "Data"))
                    {
                        return Err(invalid(format!("{other} is not an event category.")));
                    }
                }
            }
            if !data
                && let Some(field) = fields.iter().find(|field| {
                    !matches!(
                        field.field.as_str(),
                        "eventCategory" | "eventSource" | "readOnly"
                    )
                })
            {
                return Err(invalid(format!(
                    "A management event selector does not choose by {}.",
                    field.field
                )));
            }
            parsed.push(Advanced {
                name: selector["Name"].as_str().map(str::to_string),
                fields,
            });
        }
        if values > MAX_ADVANCED_VALUES {
            return Err(invalid(
                "A trail's advanced event selectors hold at most 500 values.",
            ));
        }
        Ok(Selectors::Advanced(parsed))
    }

    /// The selectors as GetEventSelectors returns them (and as they are
    /// kept): `(EventSelectors, AdvancedEventSelectors)`.
    pub(crate) fn render(&self) -> (Value, Value) {
        match self {
            Selectors::Basic(selectors) => (
                json!(
                    selectors
                        .iter()
                        .map(|selector| {
                            let mut out = json!({
                                "ReadWriteType": match selector.read_write {
                                    ReadWrite::All => "All",
                                    ReadWrite::ReadOnly => "ReadOnly",
                                    ReadWrite::WriteOnly => "WriteOnly",
                                },
                                "IncludeManagementEvents": selector.management,
                                "DataResources": if selector.objects.is_empty() {
                                    json!([])
                                } else {
                                    json!([{ "Type": S3_OBJECT, "Values": selector.objects }])
                                },
                            });
                            if !selector.excluded_sources.is_empty() {
                                out["ExcludeManagementEventSources"] =
                                    json!(selector.excluded_sources);
                            }
                            out
                        })
                        .collect::<Vec<_>>()
                ),
                Value::Null,
            ),
            Selectors::Advanced(selectors) => (
                Value::Null,
                json!(
                    selectors
                        .iter()
                        .map(|selector| {
                            let mut out = json!({
                                "FieldSelectors": selector.fields.iter().map(|field| {
                                    let mut out = json!({ "Field": field.field });
                                    for (condition, values) in &field.conditions {
                                        out[condition] = json!(values);
                                    }
                                    out
                                }).collect::<Vec<_>>(),
                            });
                            if let Some(name) = &selector.name {
                                out["Name"] = json!(name);
                            }
                            out
                        })
                        .collect::<Vec<_>>()
                ),
            ),
        }
    }

    /// The selectors kept as `(EventSelectors, AdvancedEventSelectors)`.
    pub(crate) fn from_rendered(basic: &Value, advanced: &Value) -> Option<Selectors> {
        Selectors::parse(Some(basic), Some(advanced)).ok()
    }

    /// Whether any selector may choose data events (a trail without them
    /// gets none built).
    pub(crate) fn wants_data(&self) -> bool {
        match self {
            Selectors::Basic(selectors) => selectors
                .iter()
                .any(|selector| !selector.objects.is_empty()),
            Selectors::Advanced(selectors) => selectors.iter().any(|selector| {
                selector.fields.iter().any(|field| {
                    field.field == "eventCategory"
                        && field
                            .conditions
                            .iter()
                            .any(|(_, values)| values.iter().any(|value| value == "Data"))
                })
            }),
        }
    }
}

/// Whether a trail with `selectors` (none: the default) logs `event` (a
/// CloudTrail record).
pub(crate) fn logs(selectors: Option<&Selectors>, event: &Value) -> bool {
    let category = event["eventCategory"].as_str().unwrap_or("");
    let read_only = event["readOnly"].as_bool().unwrap_or(false);
    let Some(selectors) = selectors else {
        return category == "Management";
    };
    match selectors {
        Selectors::Basic(selectors) => selectors.iter().any(|selector| {
            let kind = match selector.read_write {
                ReadWrite::All => true,
                ReadWrite::ReadOnly => read_only,
                ReadWrite::WriteOnly => !read_only,
            };
            kind && match category {
                "Management" => {
                    selector.management
                        && !event["eventSource"].as_str().is_some_and(|source| {
                            selector
                                .excluded_sources
                                .iter()
                                .any(|excluded| excluded == source)
                        })
                }
                "Data" => object_arns(event).iter().any(|(arn, of_prefix)| {
                    selector.objects.iter().any(|value| {
                        value == "arn:aws:s3"
                            || arn.starts_with(value.as_str())
                            // An event on a bucket's objects (a listing) for
                            // a value naming some of them.
                            || (*of_prefix && value.starts_with(arn))
                    })
                }),
                _ => false,
            }
        }),
        Selectors::Advanced(selectors) => selectors.iter().any(|selector| {
            selector
                .fields
                .iter()
                .all(|field| field_matches(field, event))
        }),
    }
}

/// The ARNs of an event's S3 objects, each with whether it is a prefix of
/// the objects (`ARNPrefix`, an event on a bucket's objects).
fn object_arns(event: &Value) -> Vec<(&str, bool)> {
    event["resources"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|resource| resource["type"] == S3_OBJECT)
        .filter_map(|resource| match resource["ARN"].as_str() {
            Some(arn) => Some((arn, false)),
            None => resource["ARNPrefix"].as_str().map(|prefix| (prefix, true)),
        })
        .collect()
}

/// Whether `field`'s conditions all hold for `event`: a positive condition
/// for some value of the field, a negative one for none.
fn field_matches(field: &Field, event: &Value) -> bool {
    let values: Vec<String> = match field.field.as_str() {
        "readOnly" => vec![event["readOnly"].as_bool().unwrap_or(false).to_string()],
        "resources.type" | "resources.ARN" => {
            let types = field.field == "resources.type";
            event["resources"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|resource| {
                    match types {
                        true => resource["type"].as_str(),
                        false => resource["ARN"].as_str().or(resource["ARNPrefix"].as_str()),
                    }
                    .map(str::to_string)
                })
                .collect()
        }
        name => event[name]
            .as_str()
            .map(str::to_string)
            .into_iter()
            .collect(),
    };
    field.conditions.iter().all(|(condition, wanted)| {
        let test = |value: &String, want: &String| match condition.trim_start_matches("Not") {
            "Equals" => value == want,
            "StartsWith" => value.starts_with(want.as_str()),
            _ => value.ends_with(want.as_str()),
        };
        let any = values
            .iter()
            .any(|value| wanted.iter().any(|want| test(value, want)));
        if condition.starts_with("Not") {
            !any
        } else {
            any
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(category: &str, name: &str, read_only: bool, key: Option<&str>) -> Value {
        let mut event = json!({
            "eventCategory": category,
            "eventSource": "s3.amazonaws.com",
            "eventName": name,
            "readOnly": read_only,
            "resources": [{ "type": "AWS::S3::Bucket", "ARN": "arn:aws:s3:::logs" }],
        });
        if let Some(key) = key {
            event["resources"].as_array_mut().unwrap().insert(
                0,
                json!({ "type": S3_OBJECT, "ARN": format!("arn:aws:s3:::logs/{key}") }),
            );
        }
        event
    }

    #[test]
    fn selectors_choose_events_as_cloudtrail_does() {
        let get = event("Data", "GetObject", true, Some("docs/a"));
        let put = event("Data", "PutObject", false, Some("img/b"));
        let manage = event("Management", "PutBucketPolicy", false, None);
        // The default: management events only.
        assert!(logs(None, &manage) && !logs(None, &get));
        let basic = |value: Value| Selectors::parse(Some(&value), None).unwrap();
        let docs = basic(
            json!([{ "ReadWriteType": "All", "IncludeManagementEvents": false,
            "DataResources": [{ "Type": S3_OBJECT, "Values": ["arn:aws:s3:::logs/docs/"] }] }]),
        );
        assert!(logs(Some(&docs), &get) && !logs(Some(&docs), &put) && !logs(Some(&docs), &manage));
        assert!(docs.wants_data());
        let writes = basic(json!([{ "ReadWriteType": "WriteOnly",
            "DataResources": [{ "Type": S3_OBJECT, "Values": ["arn:aws:s3"] }] }]));
        assert!(
            logs(Some(&writes), &put) && logs(Some(&writes), &manage) && !logs(Some(&writes), &get)
        );
        let advanced = Selectors::parse(
            None,
            Some(&json!([{ "Name": "puts", "FieldSelectors": [
                { "Field": "eventCategory", "Equals": ["Data"] },
                { "Field": "resources.type", "Equals": ["AWS::S3::Object"] },
                { "Field": "eventName", "Equals": ["PutObject", "DeleteObject"] },
                { "Field": "resources.ARN", "NotStartsWith": ["arn:aws:s3:::logs/docs/"] },
            ]}])),
        )
        .unwrap();
        assert!(logs(Some(&advanced), &put) && !logs(Some(&advanced), &get));
        assert!(!logs(
            Some(&advanced),
            &event("Data", "PutObject", false, Some("docs/c"))
        ));
        assert!(advanced.wants_data() && !basic(json!([{ "ReadWriteType": "All" }])).wants_data());
        // A listing of a bucket names its objects by prefix: a value for some
        // of them, or for all of S3, takes it; a value for another bucket does
        // not; nor does a selector of keys outside the bucket's prefix.
        let mut listing = event("Data", "ListObjects", true, None);
        listing["resources"].as_array_mut().unwrap().insert(
            0,
            json!({ "type": S3_OBJECT, "ARNPrefix": "arn:aws:s3:::logs/" }),
        );
        assert!(logs(Some(&docs), &listing));
        let elsewhere = basic(json!([{ "DataResources": [{ "Type": S3_OBJECT,
            "Values": ["arn:aws:s3:::other/"] }] }]));
        assert!(!logs(Some(&elsewhere), &listing));
        let typed = Selectors::parse(
            None,
            Some(&json!([{ "FieldSelectors": [
                { "Field": "eventCategory", "Equals": ["Data"] },
                { "Field": "resources.type", "Equals": ["AWS::S3::Object"] },
                { "Field": "resources.ARN", "StartsWith": ["arn:aws:s3:::logs/"] },
            ]}])),
        )
        .unwrap();
        assert!(logs(Some(&typed), &listing) && logs(Some(&typed), &get));
        // Kept as rendered, read back the same.
        for selectors in [&docs, &writes, &advanced] {
            let (b, a) = selectors.render();
            assert_eq!(Selectors::from_rendered(&b, &a).as_ref(), Some(selectors));
        }
    }

    #[test]
    fn selectors_are_checked_as_cloudtrail_checks_them() {
        let code = |basic: Option<Value>, advanced: Option<Value>| {
            Selectors::parse(basic.as_ref(), advanced.as_ref())
                .unwrap_err()
                .code
        };
        assert_eq!(
            code(Some(json!([])), Some(json!([]))),
            "InvalidParameterCombinationException"
        );
        assert_eq!(code(None, None), "InvalidEventSelectorsException");
        assert_eq!(
            code(Some(json!([])), None),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            code(Some(json!([{}, {}, {}, {}, {}, {}])), None),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            code(Some(json!([{ "ReadWriteType": "read-only" }])), None),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            code(
                Some(
                    json!([{ "DataResources": [{ "Type": "AWS::Lambda::Function",
            "Values": ["arn:aws:lambda"] }] }])
                ),
                None
            ),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            code(
                Some(json!([{ "DataResources": [{ "Type": S3_OBJECT,
            "Values": ["arn:aws:s3:::nobucketslash"] }] }])),
                None
            ),
            "InvalidEventSelectorsException"
        );
        let many: Vec<String> = (0..251).map(|n| format!("arn:aws:s3:::b/{n}")).collect();
        assert_eq!(
            code(
                Some(json!([{ "DataResources": [{ "Type": S3_OBJECT, "Values": many }] }])),
                None
            ),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            code(
                None,
                Some(json!([{ "FieldSelectors": [{ "Field": "eventName", "Equals": ["x"] }] }]))
            ),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            code(
                None,
                Some(
                    json!([{ "FieldSelectors": [{ "Field": "eventCategory", "Equals": ["Data"] },
            { "Field": "userIdentity.arn", "Equals": ["x"] }] }])
                )
            ),
            "InvalidEventSelectorsException"
        );
        let many: Vec<String> = (0..501).map(|n| n.to_string()).collect();
        assert_eq!(
            code(
                None,
                Some(
                    json!([{ "FieldSelectors": [{ "Field": "eventCategory", "Equals": ["Data"] },
            { "Field": "eventName", "Equals": many }] }])
                )
            ),
            "InvalidEventSelectorsException"
        );
        let refused = |selectors: Value| code(None, Some(selectors));
        // Only Management and Data; a management selector by eventCategory,
        // eventSource and readOnly only.
        assert_eq!(
            refused(
                json!([{ "FieldSelectors": [{ "Field": "eventCategory", "Equals": ["Bogus"] }] }])
            ),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            refused(
                json!([{ "FieldSelectors": [{ "Field": "eventCategory", "Equals": ["Management"] },
            { "Field": "eventName", "Equals": ["PutBucketPolicy"] }] }])
            ),
            "InvalidEventSelectorsException"
        );
        assert!(
            Selectors::parse(
                None,
                Some(&json!([{ "FieldSelectors": [
            { "Field": "eventCategory", "Equals": ["Management"] },
            { "Field": "readOnly", "Equals": ["false"] },
            { "Field": "eventSource", "NotEquals": ["kms.amazonaws.com"] }] }]))
            )
            .is_ok()
        );
        // Equals only for these; no field twice; known conditions only; a data
        // selector names resources.type.
        assert_eq!(
            refused(
                json!([{ "FieldSelectors": [{ "Field": "eventCategory", "StartsWith": ["Da"] }] }])
            ),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            refused(
                json!([{ "FieldSelectors": [{ "Field": "eventCategory", "Equals": ["Management"] },
            { "Field": "readOnly", "NotEquals": ["true"] }] }])
            ),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            refused(
                json!([{ "FieldSelectors": [{ "Field": "eventCategory", "Equals": ["Management"] },
            { "Field": "eventCategory", "Equals": ["Data"] }] }])
            ),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            refused(
                json!([{ "FieldSelectors": [{ "Field": "eventCategory", "Equals": ["Management"],
            "Matches": ["x"] }] }])
            ),
            "InvalidEventSelectorsException"
        );
        assert_eq!(
            refused(
                json!([{ "FieldSelectors": [{ "Field": "eventCategory", "Equals": ["Data"] }] }])
            ),
            "InvalidEventSelectorsException"
        );
    }
}
