//! Parsing and validating a policy document.

use serde::de::{Deserialize, Deserializer, Error as _, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};

use super::cond::{Condition, check_variables};

/// What a policy document is, which sets what it may hold and its size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyKind {
    /// A bucket policy: every statement names its principals. At most 20 KB.
    Bucket,
    /// A policy embedded in a user: no principals. At most 2,048 characters,
    /// not counting whitespace.
    UserInline,
    /// A managed policy, attached to users: no principals. At most 6,144
    /// characters, not counting whitespace.
    Managed,
}

/// Why a policy document was refused (S3's `MalformedPolicy`, IAM's
/// `MalformedPolicyDocument`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MalformedPolicy(pub String);

fn malformed(message: impl Into<String>) -> MalformedPolicy {
    MalformedPolicy(message.into())
}

/// A parsed policy.
#[derive(Debug, Clone)]
pub struct Policy {
    /// Version 2012-10-17: `${...}` are policy variables (otherwise literal).
    pub(crate) variables: bool,
    pub(crate) statements: Vec<Statement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Effect {
    Allow,
    Deny,
}

/// A statement's principals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Principal {
    /// `*` or `{"AWS": "*"}`: everyone, anonymous callers included.
    Anyone,
    /// An account: its root and its users.
    Account(String),
    User {
        account: String,
        name: String,
    },
}

/// A list of patterns an element matches, or (for `NotAction` and
/// `NotResource`) everything they do not.
#[derive(Debug, Clone)]
pub(crate) struct Patterns {
    pub negated: bool,
    pub values: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct Statement {
    pub effect: Effect,
    /// The statement's principals (bucket policies only).
    pub principals: Vec<Principal>,
    pub actions: Patterns,
    pub resources: Patterns,
    pub conditions: Vec<Condition>,
}

impl Policy {
    pub fn parse(document: &str, kind: PolicyKind) -> Result<Policy, MalformedPolicy> {
        let size_ok = match kind {
            PolicyKind::Bucket => document.len() <= 20 * 1024,
            PolicyKind::UserInline | PolicyKind::Managed => {
                let limit = if kind == PolicyKind::Managed {
                    6144
                } else {
                    2048
                };
                document.chars().filter(|c| !c.is_whitespace()).count() <= limit
            }
        };
        if !size_ok {
            return Err(malformed("The policy is larger than its size limit."));
        }
        let root = match serde_json::from_str::<Unique>(document) {
            Ok(Unique(Value::Object(root))) => root,
            Ok(_) => return Err(malformed("The policy is not a JSON object.")),
            Err(err) => return Err(malformed(format!("The policy is not valid JSON: {err}"))),
        };
        for name in root.keys() {
            if !["Version", "Id", "Statement"].contains(&name.as_str()) {
                return Err(malformed(format!("Unknown policy element {name}.")));
            }
        }
        let variables = match root.get("Version") {
            None => false,
            Some(Value::String(version)) if version == "2012-10-17" => true,
            Some(Value::String(version)) if version == "2008-10-17" => false,
            Some(_) => return Err(malformed("Version must be 2012-10-17 or 2008-10-17.")),
        };
        match (kind, root.get("Id")) {
            (_, None) => {}
            (PolicyKind::Bucket, Some(Value::String(_))) => {}
            (PolicyKind::Bucket, Some(_)) => return Err(malformed("Id must be a string.")),
            (_, Some(_)) => return Err(malformed("An identity policy has no Id.")),
        }
        let statements: Vec<&Value> = match root.get("Statement") {
            Some(Value::Array(items)) if !items.is_empty() => items.iter().collect(),
            Some(item @ Value::Object(_)) => vec![item],
            _ => return Err(malformed("Statement must be one or more statements.")),
        };
        let statements = statements
            .into_iter()
            .map(|statement| match statement {
                Value::Object(statement) => parse_statement(statement, kind, variables),
                _ => Err(malformed("A statement must be a JSON object.")),
            })
            .collect::<Result<_, _>>()?;
        Ok(Policy {
            variables,
            statements,
        })
    }
}

/// A string or a list of strings (`"x"` or `["x", "y"]`), non-empty.
fn strings(value: &Value, element: &str) -> Result<Vec<String>, MalformedPolicy> {
    let wrong = || malformed(format!("{element} must be a string or a list of strings."));
    let out: Vec<String> = match value {
        Value::String(text) => vec![text.clone()],
        Value::Array(items) => items
            .iter()
            .map(|item| item.as_str().map(str::to_string).ok_or_else(wrong))
            .collect::<Result<_, _>>()?,
        _ => return Err(wrong()),
    };
    if out.is_empty() {
        return Err(wrong());
    }
    Ok(out)
}

/// `Action` or `NotAction` (or `Resource` or `NotResource`): exactly one.
fn patterns(statement: &Map<String, Value>, element: &str) -> Result<Patterns, MalformedPolicy> {
    let not = format!("Not{element}");
    match (statement.get(element), statement.get(&not)) {
        (Some(value), None) => Ok(Patterns {
            negated: false,
            values: strings(value, element)?,
        }),
        (None, Some(value)) => Ok(Patterns {
            negated: true,
            values: strings(value, &not)?,
        }),
        _ => Err(malformed(format!(
            "A statement needs exactly one of {element} and {not}."
        ))),
    }
}

fn parse_statement(
    statement: &Map<String, Value>,
    kind: PolicyKind,
    variables: bool,
) -> Result<Statement, MalformedPolicy> {
    for name in statement.keys() {
        if ![
            "Sid",
            "Effect",
            "Principal",
            "NotPrincipal",
            "Action",
            "NotAction",
            "Resource",
            "NotResource",
            "Condition",
        ]
        .contains(&name.as_str())
        {
            return Err(malformed(format!("Unknown statement element {name}.")));
        }
    }
    if statement.get("Sid").is_some_and(|sid| !sid.is_string()) {
        return Err(malformed("Sid must be a string."));
    }
    let effect = match statement.get("Effect").and_then(Value::as_str) {
        Some("Allow") => Effect::Allow,
        Some("Deny") => Effect::Deny,
        _ => return Err(malformed("Effect must be Allow or Deny.")),
    };
    if statement.contains_key("NotPrincipal") {
        return Err(malformed("NotPrincipal is not supported."));
    }
    let principals = match (kind, statement.get("Principal")) {
        (PolicyKind::Bucket, Some(principal)) => parse_principals(principal)?,
        (PolicyKind::Bucket, None) => {
            return Err(malformed("A bucket policy statement needs a Principal."));
        }
        (_, Some(_)) => {
            return Err(malformed("An identity policy statement has no Principal."));
        }
        (_, None) => Vec::new(),
    };
    let actions = patterns(statement, "Action")?;
    for action in &actions.values {
        if action != "*"
            && !action
                .split_once(':')
                .is_some_and(|(service, name)| !service.is_empty() && !name.is_empty())
        {
            return Err(malformed(format!("Action {action} is not service:action.")));
        }
    }
    let mut resources = patterns(statement, "Resource")?;
    for resource in &mut resources.values {
        if resource == "*" {
            continue;
        }
        let parts = resource.split(':').count();
        if !resource.starts_with("arn:") || parts < 2 {
            return Err(malformed(format!("Resource {resource} is not an ARN.")));
        }
        // An identity policy may leave an ARN's last parts out: they are
        // wildcards (`arn:aws:s3` is `arn:aws:s3:*:*:*`).
        if parts < 6 {
            if kind == PolicyKind::Bucket {
                return Err(malformed(format!("Resource {resource} is not an ARN.")));
            }
            resource.push_str(&":*".repeat(6 - parts));
        }
        if variables {
            // Variables only replace the resource part, after the fifth colon.
            let parts: Vec<&str> = resource.splitn(6, ':').collect();
            if parts[..5].iter().any(|part| part.contains("${")) {
                return Err(malformed(format!(
                    "Resource {resource}: a policy variable may only replace the resource part."
                )));
            }
            check_variables(parts[5]).map_err(malformed)?;
        }
    }
    let conditions = match statement.get("Condition") {
        None => Vec::new(),
        Some(Value::Object(blocks)) => {
            let mut conditions = Vec::new();
            for (operator, block) in blocks {
                let entries = match block {
                    Value::Object(entries) if !entries.is_empty() => entries,
                    _ => {
                        return Err(malformed(format!(
                            "Condition {operator} must map one or more keys to values."
                        )));
                    }
                };
                for (key, values) in entries {
                    conditions.push(Condition::parse(operator, key, values, variables)?);
                }
            }
            conditions
        }
        Some(_) => return Err(malformed("Condition must be a JSON object.")),
    };
    Ok(Statement {
        effect,
        principals,
        actions,
        resources,
        conditions,
    })
}

/// `"*"`, or `{"AWS": ...}` naming `*`, account ids, account root ARNs and
/// user ARNs.
fn parse_principals(value: &Value) -> Result<Vec<Principal>, MalformedPolicy> {
    let aws = match value {
        Value::String(any) if any == "*" => return Ok(vec![Principal::Anyone]),
        Value::Object(map) => {
            if let Some(other) = map.keys().find(|name| *name != "AWS") {
                return Err(malformed(format!(
                    "Principal type {other} is not supported."
                )));
            }
            map.get("AWS")
                .ok_or_else(|| malformed("A Principal must name AWS principals."))?
        }
        _ => return Err(malformed("Principal must be \"*\" or {\"AWS\": ...}.")),
    };
    strings(aws, "Principal")?
        .into_iter()
        .map(|principal| {
            if principal == "*" {
                return Ok(Principal::Anyone);
            }
            if truthdb_object::valid_account_id(&principal) {
                return Ok(Principal::Account(principal));
            }
            let invalid = || malformed(format!("Invalid principal {principal}."));
            let rest = principal
                .strip_prefix("arn:aws:iam::")
                .ok_or_else(invalid)?;
            let (account, resource) = rest.split_once(':').ok_or_else(invalid)?;
            if !truthdb_object::valid_account_id(account) {
                return Err(invalid());
            }
            match resource {
                "root" => Ok(Principal::Account(account.to_string())),
                user => match user.strip_prefix("user/") {
                    Some(name) if !name.is_empty() && !name.contains(['*', '?']) => {
                        Ok(Principal::User {
                            account: account.to_string(),
                            name: name.to_string(),
                        })
                    }
                    _ => Err(invalid()),
                },
            }
        })
        .collect()
}

/// A JSON value whose objects hold no key twice: AWS refuses a policy that
/// repeats one (`serde_json::Value` would keep the last).
struct Unique(Value);

impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(UniqueVisitor)
    }
}

struct UniqueVisitor;

impl<'de> Visitor<'de> for UniqueVisitor {
    type Value = Unique;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Unique, E> {
        Ok(Unique(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Unique, E> {
        Ok(Unique(Value::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Unique, E> {
        Ok(Unique(Value::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Unique, E> {
        Ok(Unique(Value::from(value)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Unique, E> {
        Ok(Unique(Value::String(value.to_string())))
    }

    fn visit_unit<E>(self) -> Result<Unique, E> {
        Ok(Unique(Value::Null))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Unique, A::Error> {
        let mut out = Vec::new();
        while let Some(Unique(item)) = seq.next_element()? {
            out.push(item);
        }
        Ok(Unique(Value::Array(out)))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Unique, A::Error> {
        let mut out = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if out.contains_key(&key) {
                return Err(A::Error::custom(format!("the key {key} appears twice")));
            }
            let Unique(value) = map.next_value()?;
            out.insert(key, value);
        }
        Ok(Unique(Value::Object(out)))
    }
}
