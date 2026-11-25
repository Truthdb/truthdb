//! S3 Tables (`s3tables`, REST-JSON, on the S3 listener, told apart by the
//! service its requests are signed for): an account's table buckets, their
//! namespaces and their Iceberg tables, each table's pointer to its current
//! metadata file swapped by version token. Authorized as `s3tables:`
//! actions on the table bucket's ARN, or the table's (by its id), with the
//! caller's identity policies and the table bucket's policy (and the
//! table's): another account's table bucket is reached as its policies
//! grant.

use base64::Engine as _;
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use ring::rand::SecureRandom;
use serde_json::{Map, Value, json};
use truthdb_iam::{Policy, PolicyKind};
use truthdb_object::{NamespaceInfo, TableBucketInfo, TableError, TableInfo};

use crate::error::S3Error;
use crate::server::{Body, Connection, S3State, full};

/// The largest request body (S3 Tables' are small JSON documents).
const MAX_BODY: usize = 1 << 20;
/// Items a list answers at most, and when none is asked.
const MAX_LISTED: usize = 1000;

/// An S3 Tables error: its status, its type (`x-amzn-ErrorType`) and
/// message.
struct TError {
    status: u16,
    kind: &'static str,
    message: String,
}

impl TError {
    fn bad(message: impl Into<String>) -> TError {
        TError {
            status: 400,
            kind: "BadRequestException",
            message: message.into(),
        }
    }
    fn not_found(message: impl Into<String>) -> TError {
        TError {
            status: 404,
            kind: "NotFoundException",
            message: message.into(),
        }
    }
    fn conflict(message: impl Into<String>) -> TError {
        TError {
            status: 409,
            kind: "ConflictException",
            message: message.into(),
        }
    }
    fn forbidden() -> TError {
        TError {
            status: 403,
            kind: "ForbiddenException",
            message: "The caller isn't authorized to make the request.".to_string(),
        }
    }
    fn not_implemented(what: &str) -> TError {
        TError {
            status: 501,
            kind: "NotImplemented",
            message: format!("{what} is not implemented"),
        }
    }
}

impl From<S3Error> for TError {
    fn from(err: S3Error) -> TError {
        let kind = match err.status {
            403 => "ForbiddenException",
            404 => "NotFoundException",
            409 => "ConflictException",
            429 | 503 => "TooManyRequestsException",
            501 => "NotImplemented",
            500 => "InternalServerErrorException",
            _ => "BadRequestException",
        };
        TError {
            status: err.status,
            kind,
            message: err.message,
        }
    }
}

fn table_error(err: TableError) -> TError {
    match err {
        TableError::NoSuchTableBucket => TError::not_found("The table bucket does not exist."),
        TableError::NoSuchNamespace => TError::not_found("The namespace does not exist."),
        TableError::NoSuchTable => TError::not_found("The table does not exist."),
        TableError::Exists => TError::conflict("A resource of that name already exists."),
        TableError::NotEmpty => TError::conflict("The resource is not empty."),
        TableError::VersionMismatch => {
            TError::conflict("The version token does not match the table's: it has changed since.")
        }
    }
}

/// Answers one S3 Tables request.
pub async fn handle(state: &S3State, request: Request<Incoming>) -> Response<Body> {
    let (status, body) = match serve(state, request).await {
        Ok(answer) => answer,
        Err(err) => {
            let body = json!({ "message": err.message }).to_string();
            let response = Response::builder()
                .status(
                    StatusCode::from_u16(err.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                )
                .header("content-type", "application/json")
                .header("x-amzn-ErrorType", err.kind)
                .body(full(body))
                .expect("a valid response");
            return response;
        }
    };
    let builder = Response::builder().status(status);
    match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(full(body.to_string())),
        None => builder.body(full(bytes::Bytes::new())),
    }
    .expect("a valid response")
}

/// A random id, formatted as a UUID.
fn uuid() -> String {
    let mut bytes = [0u8; 16];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .expect("the system's random numbers");
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

/// AWS's table bucket naming rules.
fn valid_bucket_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (3..=63).contains(&bytes.len())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
        && bytes[0].is_ascii_alphanumeric()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        && !["xn--", "sthree-", "amzn-s3-demo-", "aws"]
            .iter()
            .any(|prefix| name.starts_with(prefix))
        && !["-s3alias", "--ol-s3", "--x-s3", "--table-s3"]
            .iter()
            .any(|suffix| name.ends_with(suffix))
}

/// AWS's namespace and table naming rules (a namespace also may not start
/// with `aws`).
fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=255).contains(&bytes.len())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
        && bytes[0].is_ascii_alphanumeric()
}

fn namespace_name(name: &str) -> Result<&str, TError> {
    if valid_name(name) && !name.starts_with("aws") {
        Ok(name)
    } else {
        Err(TError::bad(
            "A namespace's name is 1 to 255 lowercase letters, digits and underscores, starting with a letter or digit (not aws).",
        ))
    }
}

fn table_name(name: &str) -> Result<&str, TError> {
    if valid_name(name) {
        Ok(name)
    } else {
        Err(TError::bad(
            "A table's name is 1 to 255 lowercase letters, digits and underscores, starting with a letter or digit.",
        ))
    }
}

/// What a request is about: the region, the account whose table buckets
/// it acts on, and the caller's.
struct Caller<'a> {
    region: &'a str,
    /// The resource's account: the one the request's ARN names, else the
    /// caller's.
    account: &'a str,
    /// The caller's account: who made a change, as `createdBy` and
    /// `modifiedBy` name it (an account id).
    who: &'a str,
}

impl Caller<'_> {
    fn bucket_arn(&self, bucket: &str) -> String {
        format!(
            "arn:aws:s3tables:{}:{}:bucket/{bucket}",
            self.region, self.account
        )
    }

    fn table_arn(&self, bucket: &str, id: &str) -> String {
        format!("{}/table/{id}", self.bucket_arn(bucket))
    }

    /// The table bucket an ARN names: of this region and the request's
    /// account.
    fn bucket_of(&self, arn: &str) -> Result<String, TError> {
        let prefix = format!("arn:aws:s3tables:{}:", self.region);
        let rest = arn
            .strip_prefix(&prefix)
            .ok_or_else(|| TError::bad("The table bucket ARN is not this region's."))?;
        let (account, bucket) = rest
            .split_once(":bucket/")
            .ok_or_else(|| TError::bad("The table bucket ARN is not valid."))?;
        if account != self.account {
            return Err(TError::forbidden());
        }
        if !valid_bucket_name(bucket) {
            return Err(TError::bad("The table bucket ARN is not valid."));
        }
        Ok(bucket.to_string())
    }
}

/// The account whose table buckets a request acts on: the one its table
/// bucket's (or table's) ARN names, in the path or the query; else the
/// caller's.
fn resource_account(
    region: &str,
    path: &[String],
    params: &[(String, String)],
    caller: &str,
) -> String {
    let arn = match path.first().map(String::as_str) {
        Some("buckets" | "namespaces" | "tables") => path.get(1).map(String::as_str),
        _ => param(params, "tableArn").or_else(|| param(params, "tableBucketARN")),
    };
    arn.and_then(|arn| arn.strip_prefix(&format!("arn:aws:s3tables:{region}:")))
        .and_then(|rest| rest.split_once(":bucket/"))
        .map(|(account, _)| account)
        .filter(|account| account.len() == 12 && account.bytes().all(|byte| byte.is_ascii_digit()))
        .unwrap_or(caller)
        .to_string()
}

/// The request's path, split and each part percent-decoded.
fn segments(path: &str) -> Result<Vec<String>, TError> {
    path.trim_start_matches('/')
        .split('/')
        .map(|part| {
            percent_encoding::percent_decode_str(part)
                .decode_utf8()
                .map(|part| part.into_owned())
                .map_err(|_| TError::bad("The request path is not UTF-8."))
        })
        .collect()
}

fn query(query: &str) -> Vec<(String, String)> {
    crate::listing::query_params(query)
}

fn param<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn string<'a>(input: &'a Map<String, Value>, field: &str) -> Result<Option<&'a str>, TError> {
    match input.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(TError::bad(format!("{field} is a string."))),
    }
}

fn required<'a>(input: &'a Map<String, Value>, field: &str) -> Result<&'a str, TError> {
    string(input, field)?.ok_or_else(|| TError::bad(format!("{field} is required.")))
}

/// A page's size: `name` (1 to 1000), or 1000.
fn page_size(params: &[(String, String)], name: &str) -> Result<usize, TError> {
    match param(params, name) {
        None => Ok(MAX_LISTED),
        Some(value) => match value.parse::<usize>() {
            Ok(size) if (1..=MAX_LISTED).contains(&size) => Ok(size),
            _ => Err(TError::bad(format!("{name} is from 1 to 1000."))),
        },
    }
}

fn token(last: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(last)
}

fn untoken(params: &[(String, String)]) -> Result<Option<String>, TError> {
    param(params, "continuationToken")
        .filter(|value| !value.is_empty())
        .map(|value| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(value)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .ok_or_else(|| TError::bad("The continuation token is not valid."))
        })
        .transpose()
}

fn at(ms: u64) -> String {
    crate::time::iso8601(ms)
}

fn bucket_json(caller: &Caller<'_>, bucket: &TableBucketInfo) -> Value {
    json!({
        "arn": caller.bucket_arn(&bucket.name),
        "name": bucket.name,
        "ownerAccountId": caller.account,
        "createdAt": at(bucket.created_ms),
        "tableBucketId": bucket.id,
        "type": "customer",
    })
}

fn namespace_json(
    caller: &Caller<'_>,
    bucket: &TableBucketInfo,
    namespace: &NamespaceInfo,
) -> Value {
    json!({
        "namespace": [namespace.name],
        "createdAt": at(namespace.created_ms),
        "createdBy": namespace.created_by,
        "ownerAccountId": caller.account,
        "namespaceId": namespace.id,
        "tableBucketId": bucket.id,
    })
}

/// Answers a request: its status and JSON body (none for 204).
async fn serve(
    state: &S3State,
    request: Request<Incoming>,
) -> Result<(StatusCode, Option<Value>), TError> {
    let crate::signed_body::SignedBody {
        parts,
        body,
        principal,
    } = crate::signed_body::read(state, request, "s3tables", MAX_BODY).await?;
    let input: Map<String, Value> = if body.is_empty() {
        Map::new()
    } else {
        match serde_json::from_slice(&body) {
            Ok(Value::Object(map)) => map,
            _ => return Err(TError::bad("The request body is not a JSON object.")),
        }
    };
    let authz = crate::authz::Authz::load(
        state,
        Some(&principal),
        Some(crate::server::Signed {
            auth_type: "REST-HEADER",
            at_ms: parts
                .headers
                .get("x-amz-date")
                .and_then(|value| value.to_str().ok())
                .and_then(crate::server::parse_amz_date)
                .map(|secs| secs * 1000),
        }),
        &parts.headers,
        "",
        parts.extensions.get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    let path = segments(parts.uri.path())?;
    let params = query(parts.uri.query().unwrap_or(""));
    let owner = resource_account(&state.region, &path, &params, &principal.account);
    let caller = Caller {
        region: &state.region,
        account: &owner,
        who: &principal.account,
    };
    let parts_of: Vec<&str> = path.iter().map(String::as_str).collect();
    // Every catalog read and write in a lock hold that the caller's key
    // still holds in.
    let scope = crate::scope::Scope {
        key: Some((principal.access_key.clone(), principal.account.clone())),
        buckets: Vec::new(),
        origin: None,
    };
    // `action` on `arn`, with the resource's policies.
    let check = |action: &str, arn: &str, weighed: &Weighed| {
        let policies: Vec<&Policy> = weighed.policies.iter().collect();
        authz
            .check_arn_with(
                &format!("s3tables:{action}"),
                arn,
                caller.account,
                &policies,
            )
            .map_err(|_| TError::forbidden())
    };
    crate::scope::within(scope, async {
        match (&parts.method, parts_of.as_slice()) {
            (&Method::PUT, ["buckets"]) => {
                let name = required(&input, "name")?;
                refuse_extras(&input, &["name"])?;
                if !valid_bucket_name(name) {
                    return Err(TError::bad(
                        "A table bucket's name is 3 to 63 lowercase letters, digits and hyphens, starting and ending with a letter or digit, with no reserved prefix or suffix.",
                    ));
                }
                let arn = caller.bucket_arn(name);
                check("CreateTableBucket", &arn, &Weighed::default())?;
                let (account, name, id) = (caller.account.to_string(), name.to_string(), uuid());
                write(state, move |tx| {
                    tx.create_table_bucket(&account, &name, &id, crate::objects::now_ms())
                })
                .await?;
                Ok((StatusCode::OK, Some(json!({ "arn": arn }))))
            }
            (&Method::GET, ["buckets"]) => {
                check("ListTableBuckets", "*", &Weighed::default())?;
                match param(&params, "type") {
                    None | Some("customer") => {}
                    // AWS's own table buckets: none here.
                    Some("aws") => {
                        return Ok((StatusCode::OK, Some(json!({ "tableBuckets": [] }))));
                    }
                    Some(_) => return Err(TError::bad("type is customer or aws.")),
                }
                let limit = page_size(&params, "maxBuckets")?;
                let after = untoken(&params)?;
                let prefix = param(&params, "prefix").unwrap_or("").to_string();
                let account = caller.account.to_string();
                let mut buckets = read(state, move |file| {
                    file.list_table_buckets(&account, &prefix, after.as_deref(), limit + 1)
                })
                .await?;
                let more = buckets.len() > limit;
                buckets.truncate(limit);
                let mut answer = json!({
                    "tableBuckets": buckets.iter().map(|bucket| bucket_json(&caller, bucket)).collect::<Vec<_>>(),
                });
                if more && let Some(last) = buckets.last() {
                    answer["continuationToken"] = json!(token(&last.name));
                }
                Ok((StatusCode::OK, Some(answer)))
            }
            (method, ["buckets", arn, "policy"]) => {
                let bucket = caller.bucket_of(arn)?;
                let action = match *method {
                    Method::PUT => "PutTableBucketPolicy",
                    Method::GET => "GetTableBucketPolicy",
                    Method::DELETE => "DeleteTableBucketPolicy",
                    _ => return Err(TError::not_implemented("This table bucket policy operation")),
                };
                let weighed = policies(state, &caller, &bucket, None).await?;
                check(action, arn, &weighed)?;
                resource_policy(state, &caller, (method, &input), (&bucket, weighed.bucket_id), None).await
            }
            (method, ["buckets", arn]) => {
                let bucket = caller.bucket_of(arn)?;
                let policies = policies(state, &caller, &bucket, None).await?;
                match *method {
                    Method::GET => {
                        check("GetTableBucket", arn, &policies)?;
                        let found = find_bucket(state, &caller, &bucket).await?;
                        Ok((StatusCode::OK, Some(bucket_json(&caller, &found))))
                    }
                    Method::DELETE => {
                        check("DeleteTableBucket", arn, &policies)?;
                        let account = caller.account.to_string();
                        let id = policies.bucket_id.clone();
                        write(state, move |tx| {
                            guarded(tx, (&account, &bucket), id.as_deref(), |tx| {
                                tx.delete_table_bucket(&account, &bucket)
                            })
                        })
                        .await?;
                        Ok((StatusCode::NO_CONTENT, None))
                    }
                    _ => Err(TError::not_implemented("This table bucket operation")),
                }
            }
            (&Method::PUT, ["namespaces", arn]) => {
                let bucket = caller.bucket_of(arn)?;
                refuse_extras(&input, &["namespace"])?;
                let namespace = match input.get("namespace") {
                    Some(Value::Array(names)) if names.len() == 1 => names[0]
                        .as_str()
                        .ok_or_else(|| TError::bad("namespace is one string."))?,
                    _ => return Err(TError::bad("namespace is a list of one name.")),
                };
                let namespace = namespace_name(namespace)?.to_string();
                let weighed = policies(state, &caller, &bucket, None).await?;
                check("CreateNamespace", arn, &weighed)?;
                let (account, id, who) = (caller.account.to_string(), uuid(), caller.who.to_string());
                let created = namespace.clone();
                write(state, move |tx| {
                    guarded(tx, (&account, &bucket), weighed.bucket_id.as_deref(), |tx| {
                        tx.create_namespace(
                            (&account, &bucket),
                            &created,
                            &id,
                            (&who, crate::objects::now_ms()),
                        )
                    })
                })
                .await?;
                Ok((
                    StatusCode::OK,
                    Some(json!({ "tableBucketARN": arn, "namespace": [namespace] })),
                ))
            }
            (&Method::GET, ["namespaces", arn]) => {
                let bucket = caller.bucket_of(arn)?;
                check("ListNamespaces", arn, &policies(state, &caller, &bucket, None).await?)?;
                let found = find_bucket(state, &caller, &bucket).await?;
                let limit = page_size(&params, "maxNamespaces")?;
                let after = untoken(&params)?;
                let prefix = param(&params, "prefix").unwrap_or("").to_string();
                let account = caller.account.to_string();
                let mut namespaces = read(state, move |file| {
                    file.list_namespaces((&account, &bucket), &prefix, after.as_deref(), limit + 1)
                })
                .await?;
                let more = namespaces.len() > limit;
                namespaces.truncate(limit);
                let mut answer = json!({
                    "namespaces": namespaces.iter().map(|namespace| namespace_json(&caller, &found, namespace)).collect::<Vec<_>>(),
                });
                if more && let Some(last) = namespaces.last() {
                    answer["continuationToken"] = json!(token(&last.name));
                }
                Ok((StatusCode::OK, Some(answer)))
            }
            (method, ["namespaces", arn, namespace]) => {
                let bucket = caller.bucket_of(arn)?;
                let namespace = namespace_name(namespace)?.to_string();
                let policies = policies(state, &caller, &bucket, None).await?;
                match *method {
                    Method::GET => {
                        check("GetNamespace", arn, &policies)?;
                        let found = find_bucket(state, &caller, &bucket).await?;
                        let account = caller.account.to_string();
                        let looked = namespace.clone();
                        let space = read(state, move |file| {
                            file.namespace((&account, &bucket), &looked)
                        })
                        .await?
                        .ok_or_else(|| table_error(TableError::NoSuchNamespace))?;
                        Ok((StatusCode::OK, Some(namespace_json(&caller, &found, &space))))
                    }
                    Method::DELETE => {
                        check("DeleteNamespace", arn, &policies)?;
                        let account = caller.account.to_string();
                        let id = policies.bucket_id.clone();
                        write(state, move |tx| {
                            guarded(tx, (&account, &bucket), id.as_deref(), |tx| {
                                tx.delete_namespace((&account, &bucket), &namespace)
                            })
                        })
                        .await?;
                        Ok((StatusCode::NO_CONTENT, None))
                    }
                    _ => Err(TError::not_implemented("This namespace operation")),
                }
            }
            (&Method::PUT, ["tables", arn, namespace]) => {
                let bucket = caller.bucket_of(arn)?;
                let namespace = namespace_name(namespace)?.to_string();
                refuse_extras(&input, &["name", "format"])?;
                let name = table_name(required(&input, "name")?)?.to_string();
                if required(&input, "format")? != "ICEBERG" {
                    return Err(TError::bad("format is ICEBERG."));
                }
                let weighed = policies(state, &caller, &bucket, None).await?;
                let policies: Vec<&Policy> = weighed.policies.iter().collect();
                let authz = authz.with_keys(&[("s3tables:namespace", namespace.clone())]);
                authz
                    .check_arn_with("s3tables:CreateTable", arn, caller.account, &policies)
                    .map_err(|_| TError::forbidden())?;
                let id = uuid();
                let now = crate::objects::now_ms();
                let table = TableInfo {
                    namespace,
                    name,
                    id: id.clone(),
                    created_ms: now,
                    created_by: caller.who.to_string(),
                    modified_ms: now,
                    modified_by: caller.who.to_string(),
                    metadata_location: None,
                    version_token: uuid().replace('-', ""),
                    warehouse_location: format!("s3://{id}--table-s3"),
                };
                let (account, made) = (caller.account.to_string(), table.clone());
                let bucket_id = weighed.bucket_id.clone();
                write(state, move |tx| {
                    guarded(tx, (&account, &bucket), bucket_id.as_deref(), |tx| {
                        tx.create_table((&account, &bucket), &made)
                    })
                })
                .await?;
                let bucket = caller.bucket_of(arn)?;
                Ok((
                    StatusCode::OK,
                    Some(json!({
                        "tableARN": caller.table_arn(&bucket, &table.id),
                        "versionToken": table.version_token,
                    })),
                ))
            }
            (&Method::GET, ["tables", arn]) => {
                let bucket = caller.bucket_of(arn)?;
                check("ListTables", arn, &policies(state, &caller, &bucket, None).await?)?;
                let found = find_bucket(state, &caller, &bucket).await?;
                let limit = page_size(&params, "maxTables")?;
                let namespace = param(&params, "namespace")
                    .map(namespace_name)
                    .transpose()?
                    .map(str::to_string);
                let after = untoken(&params)?;
                let after = after
                    .as_deref()
                    .map(|after| {
                        after
                            .split_once('\0')
                            .map(|(space, name)| (space.to_string(), name.to_string()))
                            .ok_or_else(|| TError::bad("The continuation token is not valid."))
                    })
                    .transpose()?;
                let prefix = param(&params, "prefix").unwrap_or("").to_string();
                let account = caller.account.to_string();
                let listed_bucket = bucket.clone();
                // With the ids of their namespaces, read with them.
                let (mut tables, namespace_ids) = read(state, move |file| {
                    let tables = file.list_tables(
                        (&account, &listed_bucket),
                        namespace.as_deref(),
                        &prefix,
                        after.as_ref().map(|(space, name)| (space.as_str(), name.as_str())),
                        limit + 1,
                    )?;
                    let mut ids = std::collections::BTreeMap::new();
                    for table in &tables {
                        if !ids.contains_key(&table.namespace)
                            && let Some(space) =
                                file.namespace((&account, &listed_bucket), &table.namespace)?
                        {
                            ids.insert(table.namespace.clone(), space.id);
                        }
                    }
                    Ok((tables, ids))
                })
                .await?;
                let more = tables.len() > limit;
                tables.truncate(limit);
                let mut answer = json!({
                    "tables": tables.iter().map(|table| json!({
                        "namespace": [table.namespace],
                        "name": table.name,
                        "type": "customer",
                        "tableARN": caller.table_arn(&bucket, &table.id),
                        "createdAt": at(table.created_ms),
                        "modifiedAt": at(table.modified_ms),
                        "namespaceId": namespace_ids.get(&table.namespace),
                        "tableBucketId": found.id,
                    })).collect::<Vec<_>>(),
                });
                if more && let Some(last) = tables.last() {
                    answer["continuationToken"] =
                        json!(token(&format!("{}\0{}", last.namespace, last.name)));
                }
                Ok((StatusCode::OK, Some(answer)))
            }
            (&Method::GET, ["get-table"]) => {
                let (bucket, table) = match param(&params, "tableArn") {
                    Some(table_arn) => {
                        let (bucket_arn, id) = table_arn
                            .split_once("/table/")
                            .ok_or_else(|| TError::bad("The table ARN is not valid."))?;
                        let bucket = caller.bucket_of(bucket_arn)?;
                        let (account, looked, id) =
                            (caller.account.to_string(), bucket.clone(), id.to_string());
                        let table = read(state, move |file| file.table_by_id((&account, &looked), &id))
                            .await?;
                        (bucket, table)
                    }
                    None => {
                        let arn = param(&params, "tableBucketARN")
                            .ok_or_else(|| TError::bad("tableBucketARN or tableArn is required."))?;
                        let bucket = caller.bucket_of(arn)?;
                        let namespace = namespace_name(
                            param(&params, "namespace")
                                .ok_or_else(|| TError::bad("namespace is required."))?,
                        )?
                        .to_string();
                        let name =
                            table_name(param(&params, "name").ok_or_else(|| TError::bad("name is required."))?)?
                                .to_string();
                        let (account, looked) = (caller.account.to_string(), bucket.clone());
                        let table = read(state, move |file| {
                            file.table((&account, &looked), (&namespace, &name))
                        })
                        .await?;
                        (bucket, table)
                    }
                };
                // A table that is not there is said only to whom may get
                // the bucket's tables.
                let Some(table) = table else {
                    let policies = policies(state, &caller, &bucket, None).await?;
                    check("GetTable", &caller.table_arn(&bucket, "*"), &policies)?;
                    return Err(table_error(TableError::NoSuchTable));
                };
                let table_arn = caller.table_arn(&bucket, &table.id);
                check("GetTable", &table_arn, &policies(state, &caller, &bucket, Some(&table.id)).await?)?;
                let found = find_bucket(state, &caller, &bucket).await?;
                let (account, looked, space) =
                    (caller.account.to_string(), bucket.clone(), table.namespace.clone());
                let namespace_id = read(state, move |file| file.namespace((&account, &looked), &space))
                    .await?
                    .map(|space| space.id);
                let mut answer = json!({
                    "name": table.name,
                    "type": "customer",
                    "tableARN": table_arn,
                    "namespace": [table.namespace],
                    "namespaceId": namespace_id,
                    "versionToken": table.version_token,
                    "warehouseLocation": table.warehouse_location,
                    "createdAt": at(table.created_ms),
                    "createdBy": table.created_by,
                    "modifiedAt": at(table.modified_ms),
                    "modifiedBy": table.modified_by,
                    "ownerAccountId": caller.account,
                    "format": "ICEBERG",
                    "tableBucketId": found.id,
                });
                if let Some(location) = &table.metadata_location {
                    answer["metadataLocation"] = json!(location);
                }
                Ok((StatusCode::OK, Some(answer)))
            }
            (method, ["tables", arn, namespace, name, rest @ ..]) => {
                let bucket = caller.bucket_of(arn)?;
                let namespace = namespace_name(namespace)?.to_string();
                let name = table_name(name)?.to_string();
                // The operation's action, known before the table is read (an
                // operation not built here is refused before it).
                let action = match (method, rest) {
                    (&Method::DELETE, []) => "DeleteTable",
                    (&Method::PUT, ["rename"]) => "RenameTable",
                    (&Method::GET, ["metadata-location"]) => "GetTableMetadataLocation",
                    (&Method::PUT, ["metadata-location"]) => "UpdateTableMetadataLocation",
                    (&Method::PUT, ["policy"]) => "PutTablePolicy",
                    (&Method::GET, ["policy"]) => "GetTablePolicy",
                    (&Method::DELETE, ["policy"]) => "DeleteTablePolicy",
                    _ => return Err(TError::not_implemented("This table operation")),
                };
                let (account, looked) = (caller.account.to_string(), bucket.clone());
                let (space, named) = (namespace.clone(), name.clone());
                let Some(table) =
                    read(state, move |file| file.table((&account, &looked), (&space, &named))).await?
                else {
                    // Said only to whom may act on the bucket's tables.
                    let policies = policies(state, &caller, &bucket, None).await?;
                    check(action, &caller.table_arn(&bucket, "*"), &policies)?;
                    return Err(table_error(TableError::NoSuchTable));
                };
                let table_arn = caller.table_arn(&bucket, &table.id);
                let weighed = policies(state, &caller, &bucket, Some(&table.id)).await?;
                check(action, &table_arn, &weighed)?;
                let account = caller.account.to_string();
                let id = table.id.clone();
                match (method, rest) {
                    (_, ["policy"]) => {
                        resource_policy(
                            state,
                            &caller,
                            (method, &input),
                            (&bucket, weighed.bucket_id),
                            Some(&id),
                        )
                        .await
                    }
                    (&Method::DELETE, []) => {
                        let token = param(&params, "versionToken").map(str::to_string);
                        // The very table authorized (one renamed into its
                        // name since is not it).
                        write(state, move |tx| {
                            tx.delete_table((&account, &bucket), (&namespace, &name, &id), token.as_deref())
                                .map(|deleted| deleted.map(|_| ()))
                        })
                        .await?;
                        Ok((StatusCode::NO_CONTENT, None))
                    }
                    (&Method::PUT, ["rename"]) => {
                        refuse_extras(&input, &["newNamespaceName", "newName", "versionToken"])?;
                        let to_namespace = string(&input, "newNamespaceName")?
                            .map(namespace_name)
                            .transpose()?
                            .unwrap_or(&namespace)
                            .to_string();
                        let to_name = string(&input, "newName")?
                            .map(table_name)
                            .transpose()?
                            .unwrap_or(&name)
                            .to_string();
                        let token = string(&input, "versionToken")?.map(str::to_string);
                        let who = caller.who.to_string();
                        write(state, move |tx| {
                            tx.rename_table(
                                (&account, &bucket),
                                (&namespace, &name, &id),
                                (&to_namespace, &to_name),
                                token.as_deref(),
                                (&who, crate::objects::now_ms()),
                            )
                        })
                        .await?;
                        Ok((StatusCode::NO_CONTENT, None))
                    }
                    (&Method::GET, ["metadata-location"]) => {
                        let mut answer = json!({
                            "versionToken": table.version_token,
                            "warehouseLocation": table.warehouse_location,
                        });
                        if let Some(location) = &table.metadata_location {
                            answer["metadataLocation"] = json!(location);
                        }
                        Ok((StatusCode::OK, Some(answer)))
                    }
                    (&Method::PUT, ["metadata-location"]) => {
                        refuse_extras(&input, &["versionToken", "metadataLocation"])?;
                        let token = required(&input, "versionToken")?.to_string();
                        let location = required(&input, "metadataLocation")?.to_string();
                        // "The metadata location of a table must be an S3
                        // URI that begins with the table's warehouse
                        // location", its file a `.metadata.json` (or gzip).
                        if location.len() > 2048
                            || !location.starts_with(&format!("{}/", table.warehouse_location))
                            || !(location.ends_with(".metadata.json")
                                || location.ends_with(".metadata.json.gz"))
                        {
                            return Err(TError::bad(
                                "The metadata location is a .metadata.json (or .metadata.json.gz) file in the table's warehouse location.",
                            ));
                        }
                        let (who, next) = (caller.who.to_string(), uuid().replace('-', ""));
                        let updated = write_with(state, move |tx| {
                            tx.update_metadata_location(
                                (&account, &bucket),
                                (&namespace, &name, &id),
                                (&token, &next),
                                &location,
                                (&who, crate::objects::now_ms()),
                            )
                        })
                        .await?;
                        Ok((
                            StatusCode::OK,
                            Some(json!({
                                "name": updated.name,
                                "tableARN": table_arn,
                                "namespace": [updated.namespace],
                                "versionToken": updated.version_token,
                                "metadataLocation": updated.metadata_location,
                            })),
                        ))
                    }
                    _ => unreachable!("an action for each operation, above"),
                }
            }
            _ => Err(TError::not_implemented("This S3 Tables operation")),
        }
    })
    .await
}

/// Refuses (501) the request fields not built here (encryption, storage
/// class, tags, a table's initial metadata), and any unknown one (400).
fn refuse_extras(input: &Map<String, Value>, taken: &[&str]) -> Result<(), TError> {
    for field in input.keys() {
        if taken.contains(&field.as_str()) {
            continue;
        }
        return Err(match field.as_str() {
            "encryptionConfiguration" | "storageClassConfiguration" | "tags" | "metadata" => {
                TError::not_implemented(field)
            }
            _ => TError::bad(format!("{field} is not a field of this request.")),
        });
    }
    Ok(())
}

/// The policies a request is authorized by, and the id of the table bucket
/// they were read with (none if there was none): a change authorized by
/// them is made only to that very bucket (see [`guarded`]).
#[derive(Default)]
struct Weighed {
    bucket_id: Option<String>,
    policies: Vec<Policy>,
}

/// `op`, if `account`'s table bucket `bucket` is still the one of id `id`
/// (one deleted and made again since, with other policies, is not it).
fn guarded<T>(
    tx: &mut truthdb_object::ObjTxn<'_, '_>,
    (account, bucket): (&str, &str),
    id: Option<&str>,
    op: impl FnOnce(
        &mut truthdb_object::ObjTxn<'_, '_>,
    ) -> Result<Result<T, TableError>, truthdb_core::storage::StorageError>,
) -> Result<Result<T, TableError>, truthdb_core::storage::StorageError> {
    match tx.table_bucket_is((account, bucket), id)? {
        Ok(()) => op(tx),
        Err(err) => Ok(Err(err)),
    }
}

/// The policies that weigh on a request on table bucket `bucket` (its
/// own), or on its table of id `table` (the bucket's and the table's).
async fn policies(
    state: &S3State,
    caller: &Caller<'_>,
    bucket: &str,
    table: Option<&str>,
) -> Result<Weighed, TError> {
    let (account, bucket) = (caller.account.to_string(), bucket.to_string());
    let table = table.map(str::to_string);
    let (bucket_id, texts) = read(state, move |file| {
        let bucket_id = file.table_bucket(&account, &bucket)?.map(|found| found.id);
        let mut texts = vec![file.table_policy((&account, &bucket), None)?];
        if let Some(id) = &table {
            texts.push(file.table_policy((&account, &bucket), Some(id))?);
        }
        Ok((bucket_id, texts))
    })
    .await?;
    let policies = texts
        .into_iter()
        .flatten()
        .map(|text| {
            // Checked when put: one that does not parse is a corrupt file.
            std::str::from_utf8(&text)
                .ok()
                .and_then(|text| Policy::parse(text, PolicyKind::Bucket).ok())
                .ok_or_else(|| {
                    TError::from(S3Error::internal("A stored table policy does not parse."))
                })
        })
        .collect::<Result<_, _>>()?;
    Ok(Weighed {
        bucket_id,
        policies,
    })
}

/// Put, Get and DeleteTableBucketPolicy (`table` none), or TablePolicy
/// (the table of that id), once authorized.
async fn resource_policy(
    state: &S3State,
    caller: &Caller<'_>,
    (method, input): (&Method, &Map<String, Value>),
    (bucket, bucket_id): (&str, Option<String>),
    table: Option<&str>,
) -> Result<(StatusCode, Option<Value>), TError> {
    let (account, bucket) = (caller.account.to_string(), bucket.to_string());
    let table = table.map(str::to_string);
    match *method {
        Method::PUT => {
            refuse_extras(input, &["resourcePolicy"])?;
            let text = required(input, "resourcePolicy")?.to_string();
            let policy = Policy::parse(&text, PolicyKind::Bucket)
                .map_err(|err| TError::bad(format!("The policy is not valid: {}", err.0)))?;
            // "Public access policies are not supported."
            if truthdb_iam::is_public(&policy) {
                return Err(TError {
                    status: 403,
                    kind: "ForbiddenException",
                    message: "Public access policies are not supported.".to_string(),
                });
            }
            write(state, move |tx| {
                guarded(tx, (&account, &bucket), bucket_id.as_deref(), |tx| {
                    tx.put_table_policy((&account, &bucket), table.as_deref(), text.as_bytes())
                })
            })
            .await?;
            Ok((StatusCode::OK, None))
        }
        Method::GET => {
            let text = read(state, move |file| {
                if file.table_bucket(&account, &bucket)?.is_none() {
                    return Ok(Err(TableError::NoSuchTableBucket));
                }
                Ok(Ok(file.table_policy((&account, &bucket), table.as_deref())?))
            })
            .await?
            .map_err(table_error)?
            .ok_or_else(|| TError::not_found("There is no policy."))?;
            Ok((
                StatusCode::OK,
                Some(json!({ "resourcePolicy": String::from_utf8_lossy(&text) })),
            ))
        }
        _ => {
            write(state, move |tx| {
                guarded(tx, (&account, &bucket), bucket_id.as_deref(), |tx| {
                    tx.delete_table_policy((&account, &bucket), table.as_deref())
                })
            })
            .await?;
            Ok((StatusCode::NO_CONTENT, None))
        }
    }
}

async fn find_bucket(
    state: &S3State,
    caller: &Caller<'_>,
    bucket: &str,
) -> Result<TableBucketInfo, TError> {
    let (account, bucket) = (caller.account.to_string(), bucket.to_string());
    read(state, move |file| file.table_bucket(&account, &bucket))
        .await?
        .ok_or_else(|| table_error(TableError::NoSuchTableBucket))
}

/// A catalog read, in the request's lock hold.
async fn read<T, F>(state: &S3State, op: F) -> Result<T, TError>
where
    T: Send + 'static,
    F: FnOnce(&mut truthdb_object::ObjFile<'_>) -> Result<T, truthdb_core::storage::StorageError>
        + Send
        + 'static,
{
    Ok(crate::scope::object(state, op).await?)
}

/// A catalog change, in its own transaction.
async fn write<F>(state: &S3State, op: F) -> Result<(), TError>
where
    F: FnOnce(
            &mut truthdb_object::ObjTxn<'_, '_>,
        ) -> Result<Result<(), TableError>, truthdb_core::storage::StorageError>
        + Send
        + 'static,
{
    write_with(state, op).await
}

async fn write_with<T, F>(state: &S3State, op: F) -> Result<T, TError>
where
    T: Send + 'static,
    F: FnOnce(
            &mut truthdb_object::ObjTxn<'_, '_>,
        ) -> Result<Result<T, TableError>, truthdb_core::storage::StorageError>
        + Send
        + 'static,
{
    crate::scope::object(state, move |file| file.transaction(op))
        .await?
        .map_err(table_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_follow_the_aws_rules() {
        for good in ["abc", "my-tables-1", &"a".repeat(63)] {
            assert!(valid_bucket_name(good), "{good}");
        }
        for bad in [
            "ab",
            &"a".repeat(64),
            "My-tables",
            "my_tables",
            "my.tables",
            "-tables",
            "tables-",
            "awstables",
            "xn--tables",
            "tables--table-s3",
            "tables--x-s3",
        ] {
            assert!(!valid_bucket_name(bad), "{bad}");
        }
        assert!(valid_name("sales_2024") && valid_name("a"));
        for bad in [
            "",
            "Sales",
            "sales-2024",
            "sales.2024",
            "_sales",
            &"a".repeat(256),
        ] {
            assert!(!valid_name(bad), "{bad}");
        }
        assert!(namespace_name("awsdata").is_err() && table_name("awsdata").is_ok());
    }

    #[test]
    fn a_change_is_made_only_to_the_table_bucket_it_was_authorized_on() {
        use truthdb_core::storage::{Storage, StorageOptions};
        use truthdb_object::ObjFile;
        let path = std::env::temp_dir().join(format!(
            "truthdb-tables-guard-{}-{}",
            std::process::id(),
            crate::objects::now_ms()
        ));
        let storage = Storage::create_with_wal_bounds(
            path.clone(),
            StorageOptions {
                size_bytes: 64 << 20,
                wal_ratio: 0.05,
                metadata_ratio: 0.08,
                snapshot_ratio: 0.02,
                allocator_ratio: 0.02,
                reserved_ratio: 0.17,
                default_collation: None,
            },
            8 << 20,
            8 << 20,
            truthdb_engine::database_engines,
        )
        .expect("create");
        let at = ("111122223333", "shared");
        let tx = |op: &dyn Fn(
            &mut truthdb_object::ObjTxn<'_, '_>,
        ) -> Result<
            Result<(), TableError>,
            truthdb_core::storage::StorageError,
        >| { ObjFile::new(&mut storage.lock()).transaction(op).unwrap() };
        let make = |id: Option<&'static str>| {
            move |tx: &mut truthdb_object::ObjTxn<'_, '_>| {
                guarded(tx, at, id, |tx| {
                    tx.create_namespace(at, "sales", "n", ("me", 2))
                })
            }
        };
        assert_eq!(
            tx(&|tx| tx.create_table_bucket(at.0, at.1, "b1", 1)),
            Ok(())
        );
        // None read when authorized (so by no bucket policy), one made
        // since: not changed.
        assert_eq!(tx(&make(None)), Err(TableError::NoSuchTableBucket));
        // Deleted and made again (with other policies) since b1's were read:
        // not changed.
        assert_eq!(tx(&|tx| tx.delete_table_bucket(at.0, at.1)), Ok(()));
        assert_eq!(
            tx(&|tx| tx.create_table_bucket(at.0, at.1, "b2", 1)),
            Ok(())
        );
        assert_eq!(tx(&make(Some("b1"))), Err(TableError::NoSuchTableBucket));
        assert_eq!(
            ObjFile::new(&mut storage.lock())
                .namespace(at, "sales")
                .unwrap(),
            None
        );
        assert_eq!(tx(&make(Some("b2"))), Ok(()));
        drop(storage);
        let _ = std::fs::remove_file(&path);
    }
}
