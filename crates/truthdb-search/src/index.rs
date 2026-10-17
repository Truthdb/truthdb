use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{CommandError, SearchError};

pub type Document = Map<String, Value>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EngineState {
    pub indices: BTreeMap<String, IndexState>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexState {
    mappings: BTreeMap<String, FieldType>,
    documents: BTreeMap<String, Document>,
    #[serde(skip)]
    text_postings: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    #[serde(skip)]
    exact_postings: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
}

impl IndexState {
    pub fn new(mappings: BTreeMap<String, FieldType>) -> Self {
        IndexState {
            mappings,
            documents: BTreeMap::new(),
            text_postings: BTreeMap::new(),
            exact_postings: BTreeMap::new(),
        }
    }

    pub fn rebuild_postings(&mut self) -> Result<(), SearchError> {
        self.text_postings.clear();
        self.exact_postings.clear();
        let doc_ids: Vec<String> = self.documents.keys().cloned().collect();
        for doc_id in doc_ids {
            let document = self.documents.get(&doc_id).unwrap().clone();
            self.index_document(&doc_id, &document)?;
        }
        Ok(())
    }

    pub fn validate_document(&self, document: &Document) -> Result<(), SearchError> {
        for (field, value) in document {
            let field_type = self
                .mappings
                .get(field)
                .ok_or_else(|| CommandError::UnknownField(field.clone()))?;
            field_type.validate_value(field, value)?;
        }
        Ok(())
    }

    pub fn insert_document(&mut self, id: &str, document: &Document) -> Result<(), SearchError> {
        if self.documents.contains_key(id) {
            return Err(SearchError::Replay(format!(
                "wal attempted to insert duplicate document id '{id}'"
            )));
        }
        self.validate_document(document)?;
        self.index_document(id, document)?;
        self.documents.insert(id.to_string(), document.clone());
        Ok(())
    }

    fn index_document(&mut self, id: &str, document: &Document) -> Result<(), SearchError> {
        for (field, value) in document {
            let Some(field_type) = self.mappings.get(field) else {
                continue;
            };
            match field_type {
                FieldType::Text => {
                    let Some(text) = value.as_str() else {
                        return Err(CommandError::InvalidFieldType {
                            field: field.clone(),
                            expected: "text string".to_string(),
                            actual: value_type_name(value).to_string(),
                        }
                        .into());
                    };
                    let postings = self.text_postings.entry(field.clone()).or_default();
                    for term in tokenize(text) {
                        postings.entry(term).or_default().insert(id.to_string());
                    }
                }
                FieldType::Keyword | FieldType::Float => {
                    let term = exact_term_key(field, *field_type, value)?;
                    self.exact_postings
                        .entry(field.clone())
                        .or_default()
                        .entry(term)
                        .or_default()
                        .insert(id.to_string());
                }
            }
        }
        Ok(())
    }

    pub fn search(&self, query: &SearchQuery) -> Result<Vec<SearchHit>, SearchError> {
        let scores = self.evaluate_query(query)?;
        let mut hits = scores
            .into_iter()
            .filter_map(|(id, score)| {
                self.documents.get(&id).map(|document| SearchHit {
                    id,
                    score,
                    source: Value::Object(document.clone()),
                })
            })
            .collect::<Vec<_>>();

        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(hits)
    }

    fn evaluate_query(&self, query: &SearchQuery) -> Result<BTreeMap<String, f64>, SearchError> {
        match query {
            SearchQuery::Match { field, query } => self.evaluate_match_query(field, query),
            SearchQuery::Term { field, value } => self.evaluate_term_query(field, value),
            SearchQuery::Bool { must, filter } => self.evaluate_bool_query(must, filter),
        }
    }

    fn evaluate_match_query(
        &self,
        field: &str,
        query: &str,
    ) -> Result<BTreeMap<String, f64>, SearchError> {
        self.require_field_type(field, FieldType::Text)?;
        let Some(postings) = self.text_postings.get(field) else {
            return Ok(BTreeMap::new());
        };

        let mut scores = BTreeMap::new();
        for term in tokenize(query) {
            if let Some(doc_ids) = postings.get(&term) {
                for doc_id in doc_ids {
                    *scores.entry(doc_id.clone()).or_insert(0.0) += 1.0;
                }
            }
        }
        Ok(scores)
    }

    fn evaluate_term_query(
        &self,
        field: &str,
        value: &Value,
    ) -> Result<BTreeMap<String, f64>, SearchError> {
        let field_type = self.field_type(field)?;
        let term = exact_term_key(field, field_type, value)?;
        let Some(postings) = self.exact_postings.get(field) else {
            return Ok(BTreeMap::new());
        };
        let Some(doc_ids) = postings.get(&term) else {
            return Ok(BTreeMap::new());
        };

        let mut scores = BTreeMap::new();
        for doc_id in doc_ids {
            scores.insert(doc_id.clone(), 1.0);
        }
        Ok(scores)
    }

    fn evaluate_bool_query(
        &self,
        must: &[SearchQuery],
        filter: &[SearchQuery],
    ) -> Result<BTreeMap<String, f64>, SearchError> {
        if must.is_empty() && filter.is_empty() {
            return Err(CommandError::InvalidCommand(
                "bool query must contain at least one must or filter clause".to_string(),
            )
            .into());
        }

        let mut scores = if must.is_empty() {
            self.documents
                .keys()
                .map(|id| (id.clone(), 0.0))
                .collect::<BTreeMap<_, _>>()
        } else {
            let mut iter = must.iter();
            let first = iter.next().ok_or_else(|| {
                CommandError::InvalidCommand("missing bool.must clause".to_string())
            })?;
            let mut scores = self.evaluate_query(first)?;
            for clause in iter {
                let clause_scores = self.evaluate_query(clause)?;
                scores.retain(|doc_id, score| {
                    let Some(clause_score) = clause_scores.get(doc_id) else {
                        return false;
                    };
                    *score += clause_score;
                    true
                });
            }
            scores
        };

        for clause in filter {
            let allowed = self.evaluate_query(clause)?;
            scores.retain(|doc_id, _| allowed.contains_key(doc_id));
        }

        Ok(scores)
    }

    fn field_type(&self, field: &str) -> Result<FieldType, SearchError> {
        self.mappings
            .get(field)
            .copied()
            .ok_or_else(|| CommandError::UnknownField(field.to_string()).into())
    }

    fn require_field_type(&self, field: &str, expected: FieldType) -> Result<(), SearchError> {
        let actual = self.field_type(field)?;
        if actual != expected {
            return Err(CommandError::InvalidCommand(format!(
                "field '{field}' is {actual}, expected {expected}"
            ))
            .into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone)]
pub enum SearchQuery {
    Match {
        field: String,
        query: String,
    },
    Term {
        field: String,
        value: Value,
    },
    Bool {
        must: Vec<SearchQuery>,
        filter: Vec<SearchQuery>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "keyword")]
    Keyword,
    #[serde(rename = "float")]
    Float,
}

impl FieldType {
    fn validate_value(self, field: &str, value: &Value) -> Result<(), SearchError> {
        match self {
            FieldType::Text | FieldType::Keyword => {
                if value.as_str().is_none() {
                    return Err(CommandError::InvalidFieldType {
                        field: field.to_string(),
                        expected: "string".to_string(),
                        actual: value_type_name(value).to_string(),
                    }
                    .into());
                }
            }
            FieldType::Float => {
                if !value.is_number() {
                    return Err(CommandError::InvalidFieldType {
                        field: field.to_string(),
                        expected: "number".to_string(),
                        actual: value_type_name(value).to_string(),
                    }
                    .into());
                }
            }
        }
        Ok(())
    }
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::Text => f.write_str("text"),
            FieldType::Keyword => f.write_str("keyword"),
            FieldType::Float => f.write_str("float"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SearchHit {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_score")]
    score: f64,
    #[serde(rename = "_source")]
    source: Value,
}
fn exact_term_key(
    field: &str,
    field_type: FieldType,
    value: &Value,
) -> Result<String, SearchError> {
    match field_type {
        FieldType::Keyword => value.as_str().map(ToString::to_string).ok_or_else(|| {
            CommandError::InvalidFieldType {
                field: field.to_string(),
                expected: "string".to_string(),
                actual: value_type_name(value).to_string(),
            }
            .into()
        }),
        FieldType::Float => value
            .as_number()
            .map(|value| value.to_string())
            .ok_or_else(|| {
                CommandError::InvalidFieldType {
                    field: field.to_string(),
                    expected: "number".to_string(),
                    actual: value_type_name(value).to_string(),
                }
                .into()
            }),
        FieldType::Text => Err(CommandError::InvalidCommand(
            "term query is not supported on text fields".to_string(),
        )
        .into()),
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_ascii_lowercase())
        .collect()
}

pub fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
