//! Vector metadata and query filters, as S3 Vectors has them.
//!
//! Metadata is a JSON object of at most [`MAX_METADATA_KEYS`] keys whose
//! values are strings, numbers, booleans, or lists of those; it takes at
//! most [`MAX_METADATA_BYTES`] as JSON, and its filterable part (every key
//! the index does not name non-filterable) at most [`MAX_FILTERABLE_BYTES`].
//!
//! A filter is a JSON object: `{"field": value}` (equality),
//! `{"field": {"$op": operand, ...}}` (each operator must hold),
//! `{"$and": [filters]}`, `{"$or": [filters]}`, several entries meaning all
//! of them. Operators: `$eq` and `$ne` (string, number, boolean; against a
//! list, `$eq` holds when an element equals), `$gt`, `$gte`, `$lt`, `$lte`
//! (numbers), `$startsWith` (strings), `$in` and `$nin` (non-empty lists of
//! those), `$exists` (boolean). A filter sees only filterable metadata.

use serde_json::{Map, Value};

use crate::names::{MAX_FILTERABLE_BYTES, MAX_METADATA_BYTES, MAX_METADATA_KEYS};

/// The most values one filter may compare against.
pub const MAX_FILTER_CONSTRAINTS: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataError(pub String);

/// A vector's metadata, checked, as stored: all of it, and its filterable
/// part.
#[derive(Debug, Clone, PartialEq)]
pub struct Metadata {
    pub whole: Map<String, Value>,
    pub filterable: Map<String, Value>,
}

fn primitive(value: &Value) -> bool {
    matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_))
}

impl Metadata {
    /// Checks `value` (absent or null for none) as metadata of an index
    /// whose `non_filterable` keys are not filterable.
    pub fn new(
        value: Option<&Value>,
        non_filterable: &[String],
    ) -> Result<Metadata, MetadataError> {
        let whole = match value {
            None | Some(Value::Null) => Map::new(),
            Some(Value::Object(map)) => map.clone(),
            Some(_) => return Err(MetadataError("Metadata must be a JSON object.".into())),
        };
        if whole.len() > MAX_METADATA_KEYS {
            return Err(MetadataError(format!(
                "Metadata may have at most {MAX_METADATA_KEYS} keys."
            )));
        }
        for (key, value) in &whole {
            let valid = match value {
                Value::Array(items) => items.iter().all(primitive),
                other => primitive(other),
            };
            if !valid {
                return Err(MetadataError(format!(
                    "Metadata value of '{key}' must be a string, number, boolean or list of those."
                )));
            }
        }
        if Value::Object(whole.clone()).to_string().len() > MAX_METADATA_BYTES {
            return Err(MetadataError(format!(
                "Metadata may take at most {MAX_METADATA_BYTES} bytes."
            )));
        }
        let filterable: Map<String, Value> = whole
            .iter()
            .filter(|(key, _)| !non_filterable.contains(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if Value::Object(filterable.clone()).to_string().len() > MAX_FILTERABLE_BYTES {
            return Err(MetadataError(format!(
                "Filterable metadata may take at most {MAX_FILTERABLE_BYTES} bytes."
            )));
        }
        Ok(Metadata { whole, filterable })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterError(pub String);

/// A primitive a filter compares against.
#[derive(Debug, Clone, PartialEq)]
enum Primitive {
    Str(String),
    Num(f64),
    Bool(bool),
}

impl Primitive {
    fn from(value: &Value) -> Option<Primitive> {
        Some(match value {
            Value::String(text) => Primitive::Str(text.clone()),
            Value::Number(number) => Primitive::Num(number.as_f64()?),
            Value::Bool(flag) => Primitive::Bool(*flag),
            _ => return None,
        })
    }

    fn equals(&self, value: &Value) -> bool {
        match (self, value) {
            (Primitive::Str(a), Value::String(b)) => a == b,
            (Primitive::Num(a), Value::Number(b)) => b.as_f64() == Some(*a),
            (Primitive::Bool(a), Value::Bool(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Condition {
    Eq(Primitive),
    Ne(Primitive),
    Gt(f64),
    Gte(f64),
    Lt(f64),
    Lte(f64),
    StartsWith(String),
    In(Vec<Primitive>),
    Nin(Vec<Primitive>),
    Exists(bool),
}

/// A parsed metadata filter.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter(Node);

#[derive(Debug, Clone, PartialEq)]
enum Node {
    And(Vec<Node>),
    Or(Vec<Node>),
    Field(String, Vec<Condition>),
}

/// The values of `field` in `metadata`: a list's elements, or the value.
fn values<'a>(metadata: &'a Map<String, Value>, field: &str) -> Vec<&'a Value> {
    match metadata.get(field) {
        None => Vec::new(),
        Some(Value::Array(items)) => items.iter().collect(),
        Some(value) => vec![value],
    }
}

impl Condition {
    fn holds(&self, metadata: &Map<String, Value>, field: &str) -> bool {
        let values = values(metadata, field);
        let number = |test: &dyn Fn(f64) -> bool| {
            values.iter().any(|value| value.as_f64().is_some_and(test))
        };
        match self {
            Condition::Eq(p) => values.iter().any(|value| p.equals(value)),
            Condition::Ne(p) => !values.iter().any(|value| p.equals(value)),
            Condition::Gt(n) => number(&|v| v > *n),
            Condition::Gte(n) => number(&|v| v >= *n),
            Condition::Lt(n) => number(&|v| v < *n),
            Condition::Lte(n) => number(&|v| v <= *n),
            Condition::StartsWith(prefix) => values.iter().any(|value| {
                value
                    .as_str()
                    .is_some_and(|text| text.starts_with(prefix.as_str()))
            }),
            Condition::In(list) => values
                .iter()
                .any(|value| list.iter().any(|p| p.equals(value))),
            Condition::Nin(list) => !values
                .iter()
                .any(|value| list.iter().any(|p| p.equals(value))),
            Condition::Exists(present) => metadata.contains_key(field) == *present,
        }
    }
}

fn bad(message: impl Into<String>) -> FilterError {
    FilterError(message.into())
}

impl Filter {
    /// Parses a filter, counting its constraints (at most
    /// [`MAX_FILTER_CONSTRAINTS`]).
    pub fn parse(value: &Value) -> Result<Filter, FilterError> {
        let mut constraints = 0;
        let filter = Filter(Node::parse_object(value, &mut constraints)?);
        if constraints > MAX_FILTER_CONSTRAINTS {
            return Err(bad(format!(
                "A filter may have at most {MAX_FILTER_CONSTRAINTS} constraints."
            )));
        }
        Ok(filter)
    }

    /// Whether `filterable` metadata satisfies the filter.
    pub fn holds(&self, filterable: &Map<String, Value>) -> bool {
        self.0.holds(filterable)
    }
}

impl Node {
    fn parse_object(value: &Value, constraints: &mut usize) -> Result<Node, FilterError> {
        let Value::Object(entries) = value else {
            return Err(bad("A filter must be a JSON object."));
        };
        if entries.is_empty() {
            return Err(bad("A filter must not be empty."));
        }
        let mut all = Vec::new();
        for (name, operand) in entries {
            all.push(match name.as_str() {
                "$and" | "$or" => {
                    let Some(items) = operand.as_array().filter(|items| !items.is_empty()) else {
                        return Err(bad(format!("{name} takes a non-empty list of filters.")));
                    };
                    let parts = items
                        .iter()
                        .map(|item| Node::parse_object(item, constraints))
                        .collect::<Result<Vec<_>, _>>()?;
                    if name == "$and" {
                        Node::And(parts)
                    } else {
                        Node::Or(parts)
                    }
                }
                field if field.starts_with('$') => {
                    return Err(bad(format!("Unknown filter operator {field}.")));
                }
                field => Node::Field(field.to_string(), Node::conditions(operand, constraints)?),
            });
        }
        Ok(if all.len() == 1 {
            all.pop().expect("one")
        } else {
            Node::And(all)
        })
    }

    fn conditions(operand: &Value, constraints: &mut usize) -> Result<Vec<Condition>, FilterError> {
        let Value::Object(operators) = operand else {
            *constraints += 1;
            return Primitive::from(operand)
                .map(|p| vec![Condition::Eq(p)])
                .ok_or_else(|| bad("A field filter compares with a string, number or boolean."));
        };
        if operators.is_empty() {
            return Err(bad("A field filter must name an operator."));
        }
        let mut out = Vec::new();
        for (operator, value) in operators {
            let number = || {
                value
                    .as_f64()
                    .ok_or_else(|| bad(format!("{operator} takes a number.")))
            };
            let primitive = || {
                Primitive::from(value)
                    .ok_or_else(|| bad(format!("{operator} takes a string, number or boolean.")))
            };
            let list = || -> Result<Vec<Primitive>, FilterError> {
                let items = value
                    .as_array()
                    .filter(|items| !items.is_empty())
                    .ok_or_else(|| bad(format!("{operator} takes a non-empty list.")))?;
                items
                    .iter()
                    .map(|item| {
                        Primitive::from(item).ok_or_else(|| {
                            bad(format!("{operator} takes strings, numbers or booleans."))
                        })
                    })
                    .collect()
            };
            let condition = match operator.as_str() {
                "$eq" => Condition::Eq(primitive()?),
                "$ne" => Condition::Ne(primitive()?),
                "$gt" => Condition::Gt(number()?),
                "$gte" => Condition::Gte(number()?),
                "$lt" => Condition::Lt(number()?),
                "$lte" => Condition::Lte(number()?),
                "$startsWith" => Condition::StartsWith(
                    value
                        .as_str()
                        .ok_or_else(|| bad("$startsWith takes a string."))?
                        .to_string(),
                ),
                "$in" => Condition::In(list()?),
                "$nin" => Condition::Nin(list()?),
                "$exists" => Condition::Exists(
                    value
                        .as_bool()
                        .ok_or_else(|| bad("$exists takes a boolean."))?,
                ),
                other => return Err(bad(format!("Unknown filter operator {other}."))),
            };
            *constraints += match &condition {
                Condition::In(items) | Condition::Nin(items) => items.len(),
                _ => 1,
            };
            out.push(condition);
        }
        Ok(out)
    }

    fn holds(&self, filterable: &Map<String, Value>) -> bool {
        match self {
            Node::And(parts) => parts.iter().all(|part| part.holds(filterable)),
            Node::Or(parts) => parts.iter().any(|part| part.holds(filterable)),
            Node::Field(field, conditions) => conditions
                .iter()
                .all(|condition| condition.holds(filterable, field)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn holds(filter: Value, metadata: Value) -> bool {
        let Value::Object(metadata) = metadata else {
            unreachable!()
        };
        Filter::parse(&filter).unwrap().holds(&metadata)
    }

    #[test]
    fn every_operator_matches_as_s3_vectors_documents() {
        let movie = json!({
            "genre": ["documentary", "romance"],
            "year": 2021,
            "title": "Deep sea",
            "path": "/marketing/a",
            "free": true
        });
        let cases = [
            (json!({"genre": "documentary"}), true),
            (json!({"genre": {"$eq": "drama"}}), false),
            (json!({"genre": {"$ne": "drama"}}), true),
            (json!({"genre": {"$ne": "romance"}}), false),
            (json!({"year": {"$gt": 2019}}), true),
            (json!({"year": {"$gte": 2021, "$lte": 2021}}), true),
            (json!({"year": {"$lt": 2021}}), false),
            (json!({"path": {"$startsWith": "/marketing/"}}), true),
            (json!({"genre": {"$in": ["comedy", "romance"]}}), true),
            (json!({"genre": {"$nin": ["comedy", "romance"]}}), false),
            (json!({"missing": {"$nin": ["x"]}}), true),
            (json!({"missing": {"$exists": false}}), true),
            (json!({"title": {"$exists": true}}), true),
            (json!({"free": true, "year": 2021}), true),
            (json!({"$and": [{"genre": "drama"}, {"year": 2021}]}), false),
            (json!({"$or": [{"genre": "drama"}, {"year": 2021}]}), true),
            (json!({"year": "2021"}), false),
        ];
        for (filter, expected) in cases {
            assert_eq!(holds(filter.clone(), movie.clone()), expected, "{filter}");
        }
    }

    #[test]
    fn malformed_filters_and_metadata_are_refused() {
        for filter in [
            json!([]),
            json!({}),
            json!({"$and": []}),
            json!({"$not": [{"a": 1}]}),
            json!({"a": {"$gt": "x"}}),
            json!({"a": {"$in": []}}),
            json!({"a": {"$regex": "x"}}),
            json!({"a": {}}),
            json!({"a": null}),
            json!({"a": {"$in": (0..101).collect::<Vec<_>>()}}),
        ] {
            assert!(Filter::parse(&filter).is_err(), "{filter}");
        }
        let none: &[String] = &[];
        assert!(Metadata::new(Some(&json!({"a": {"b": 1}})), none).is_err());
        assert!(Metadata::new(Some(&json!({"a": null})), none).is_err());
        assert!(Metadata::new(Some(&json!([1])), none).is_err());
        let many: Map<String, Value> = (0..51).map(|n| (format!("k{n}"), json!(n))).collect();
        assert!(Metadata::new(Some(&Value::Object(many)), none).is_err());
        let text = json!({"text": "x".repeat(3000)});
        assert!(
            Metadata::new(Some(&text), none).is_err(),
            "filterable past 2 KB"
        );
        let held = Metadata::new(Some(&text), &["text".to_string()]).unwrap();
        assert!(held.filterable.is_empty());
        assert!(
            Metadata::new(
                Some(&json!({"t": "x".repeat(41 * 1024)})),
                &["t".to_string()]
            )
            .is_err()
        );
        assert_eq!(Metadata::new(None, none).unwrap().whole, Map::new());
    }
}
