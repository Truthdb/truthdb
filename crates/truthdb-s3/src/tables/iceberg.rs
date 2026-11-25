//! S3 Tables' Iceberg REST catalog (`/iceberg/v1/...`, signed for
//! `s3tables`): a table bucket's namespaces and tables as Iceberg's REST
//! catalog API has them, its `{prefix}` the table bucket's ARN, each
//! operation authorized as AWS maps it to `s3tables:` actions.

use base64::Engine as _;
use hyper::{Method, StatusCode};
use serde_json::{Map, Value, json};
use truthdb_object::{TableError, TableInfo, VersionSel};

use super::{
    Caller, TError, Weighed, guarded, make_table, namespace_name, param, policies, read,
    table_error, table_name, write, write_with,
};
use crate::server::S3State;

/// A metadata file is read whole: "a `metadata.json` file over 50MB is not
/// supported" (400).
const MAX_METADATA_BYTES: u64 = 50 * 1024 * 1024;
/// Items a list answers at most, and when none is asked.
const MAX_LISTED: usize = 1000;

/// The operations served, as `/v1/config` names them.
const ENDPOINTS: [&str; 12] = [
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/tables/rename",
];

/// What a catalog request is served with.
pub(super) struct Ctx<'a> {
    pub state: &'a S3State,
    pub caller: &'a Caller<'a>,
    pub authz: &'a crate::authz::Authz,
    /// An `s3tables:` action on an ARN, with the resource's policies.
    pub check: &'a (dyn Fn(&str, &str, &Weighed) -> Result<(), TError> + Sync),
    pub params: &'a [(String, String)],
    pub input: &'a Map<String, Value>,
}

/// The Iceberg error type of an error (its status and message, as the S3
/// Tables API words them).
pub(super) fn error_type(err: &TError) -> &'static str {
    match err.status {
        400 => "BadRequestException",
        403 => "ForbiddenException",
        404 if err.message.contains("namespace") => "NoSuchNamespaceException",
        404 if err.message.contains("table bucket") => "NoSuchWarehouseException",
        404 => "NoSuchTableException",
        409 if err.kind == "CommitFailedException" => "CommitFailedException",
        409 if err.message.contains("not empty") => "NamespaceNotEmptyException",
        409 if err.message.contains("version token") => "CommitFailedException",
        409 => "AlreadyExistsException",
        501 => "UnsupportedOperationException",
        _ => err.kind,
    }
}

/// Answers a catalog request: `path` after `/iceberg`.
pub(super) async fn serve(
    ctx: &Ctx<'_>,
    method: &Method,
    path: &[&str],
) -> Result<(StatusCode, Option<Value>), TError> {
    match (method, path) {
        (&Method::GET, ["v1", "config"]) => config(ctx).await,
        (method, ["v1", prefix, rest @ ..]) => {
            let bucket = ctx.caller.bucket_of(prefix)?;
            match (method, rest) {
                (&Method::GET, ["namespaces"]) => list_namespaces(ctx, &bucket).await,
                (&Method::POST, ["namespaces"]) => create_namespace(ctx, &bucket).await,
                (method, ["namespaces", namespace]) => {
                    let namespace = level(namespace)?;
                    match *method {
                        Method::GET | Method::HEAD => {
                            load_namespace(ctx, &bucket, &namespace, method == Method::HEAD).await
                        }
                        Method::DELETE => drop_namespace(ctx, &bucket, &namespace).await,
                        _ => Err(TError::not_implemented("This namespace operation")),
                    }
                }
                (&Method::GET, ["namespaces", namespace, "tables"]) => {
                    list_tables(ctx, &bucket, &level(namespace)?).await
                }
                (&Method::POST, ["namespaces", namespace, "tables"]) => {
                    create_table(ctx, &bucket, level(namespace)?).await
                }
                (method, ["namespaces", namespace, "tables", name]) => {
                    let at = (level(namespace)?, table_name(name)?.to_string());
                    match *method {
                        Method::GET => load_table(ctx, &bucket, at).await,
                        Method::HEAD => table_exists(ctx, &bucket, at).await,
                        Method::DELETE => drop_table(ctx, &bucket, at).await,
                        Method::POST => commit_table(ctx, &bucket, at).await,
                        _ => Err(TError::not_implemented("This table operation")),
                    }
                }
                (&Method::POST, ["tables", "rename"]) => rename_table(ctx, &bucket).await,
                _ => Err(TError::not_implemented("This Iceberg REST operation")),
            }
        }
        _ => Err(TError::not_implemented("This Iceberg REST operation")),
    }
}

/// A namespace of the path: one level ("Multilevel namespaces are not
/// supported").
fn level(namespace: &str) -> Result<String, TError> {
    if namespace.contains('\u{1f}') {
        return Err(TError::bad("Multilevel namespaces are not supported."));
    }
    Ok(namespace_name(namespace)?.to_string())
}

/// A namespace of a request body: a list of one name.
fn level_of(value: Option<&Value>, what: &str) -> Result<String, TError> {
    match value.and_then(Value::as_array).map(Vec::as_slice) {
        Some([name]) => level(
            name.as_str()
                .ok_or_else(|| TError::bad(format!("{what} is a list of names.")))?,
        ),
        Some(_) => Err(TError::bad("Multilevel namespaces are not supported.")),
        None => Err(TError::bad(format!("{what} is a list of names."))),
    }
}

/// Refuses any member of the body not in `taken`.
fn only(input: &Map<String, Value>, taken: &[&str]) -> Result<(), TError> {
    match input.keys().find(|key| !taken.contains(&key.as_str())) {
        Some(key) => Err(TError::bad(format!(
            "{key} is not a field of this request."
        ))),
        None => Ok(()),
    }
}

/// A page's size (`pageSize`, 1 to 1000; 1000 if none) and where it starts
/// (`pageToken`, a name of the last page's last).
fn page(params: &[(String, String)]) -> Result<(usize, Option<String>), TError> {
    let size = match param(params, "pageSize") {
        None => MAX_LISTED,
        Some(value) => value
            .parse::<usize>()
            .ok()
            .filter(|size| (1..=MAX_LISTED).contains(size))
            .ok_or_else(|| TError::bad("pageSize is from 1 to 1000."))?,
    };
    let after = param(params, "pageToken")
        .filter(|token| !token.is_empty())
        .map(|token| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(token)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .ok_or_else(|| TError::bad("The page token is not valid."))
        })
        .transpose()?;
    Ok((size, after))
}

fn token(last: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(last)
}

/// `GET /v1/config?warehouse=<table bucket ARN>`: its prefix (the ARN,
/// url-encoded) and the operations served.
async fn config(ctx: &Ctx<'_>) -> Result<(StatusCode, Option<Value>), TError> {
    let warehouse = param(ctx.params, "warehouse")
        .ok_or_else(|| TError::bad("warehouse is the table bucket's ARN."))?;
    let bucket = ctx.caller.bucket_of(warehouse)?;
    let arn = ctx.caller.bucket_arn(&bucket);
    (ctx.check)(
        "GetTableBucket",
        &arn,
        &policies(ctx.state, ctx.caller, &bucket, None).await?,
    )?;
    super::find_bucket(ctx.state, ctx.caller, &bucket).await?;
    const KEPT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    let prefix = percent_encoding::utf8_percent_encode(&arn, KEPT).to_string();
    Ok((
        StatusCode::OK,
        Some(json!({
            "defaults": {},
            "overrides": { "prefix": prefix },
            "endpoints": ENDPOINTS,
        })),
    ))
}

async fn list_namespaces(
    ctx: &Ctx<'_>,
    bucket: &str,
) -> Result<(StatusCode, Option<Value>), TError> {
    let arn = ctx.caller.bucket_arn(bucket);
    (ctx.check)(
        "ListNamespaces",
        &arn,
        &policies(ctx.state, ctx.caller, bucket, None).await?,
    )?;
    super::find_bucket(ctx.state, ctx.caller, bucket).await?;
    // One level: a namespace holds none.
    if let Some(parent) = param(ctx.params, "parent").filter(|parent| !parent.is_empty()) {
        let parent = level(parent)?;
        namespace_of(ctx, bucket, &parent).await?;
        return Ok((StatusCode::OK, Some(json!({ "namespaces": [] }))));
    }
    let (limit, after) = page(ctx.params)?;
    let (account, listed) = (ctx.caller.account.to_string(), bucket.to_string());
    let mut namespaces = read(ctx.state, move |file| {
        file.list_namespaces((&account, &listed), "", after.as_deref(), limit + 1)
    })
    .await?;
    let more = namespaces.len() > limit;
    namespaces.truncate(limit);
    let mut answer = json!({
        "namespaces": namespaces.iter().map(|space| json!([space.name])).collect::<Vec<_>>(),
    });
    if more && let Some(last) = namespaces.last() {
        answer["next-page-token"] = json!(token(&last.name));
    }
    Ok((StatusCode::OK, Some(answer)))
}

/// Namespace `namespace` of `bucket`, or NoSuchNamespace.
async fn namespace_of(ctx: &Ctx<'_>, bucket: &str, namespace: &str) -> Result<(), TError> {
    let (account, looked, space) = (
        ctx.caller.account.to_string(),
        bucket.to_string(),
        namespace.to_string(),
    );
    read(ctx.state, move |file| {
        file.namespace((&account, &looked), &space)
    })
    .await?
    .map(|_| ())
    .ok_or_else(|| table_error(TableError::NoSuchNamespace))
}

async fn create_namespace(
    ctx: &Ctx<'_>,
    bucket: &str,
) -> Result<(StatusCode, Option<Value>), TError> {
    only(ctx.input, &["namespace", "properties"])?;
    let namespace = level_of(ctx.input.get("namespace"), "namespace")?;
    // Its properties are not kept.
    if ctx
        .input
        .get("properties")
        .is_some_and(|properties| !properties.is_null() && properties != &json!({}))
    {
        return Err(TError::bad("A namespace's properties are not supported."));
    }
    let arn = ctx.caller.bucket_arn(bucket);
    let weighed = policies(ctx.state, ctx.caller, bucket, None).await?;
    (ctx.check)("CreateNamespace", &arn, &weighed)?;
    let (account, bucket, who) = (
        ctx.caller.account.to_string(),
        bucket.to_string(),
        ctx.caller.who.to_string(),
    );
    let (created, id) = (namespace.clone(), super::uuid());
    write(ctx.state, move |tx| {
        guarded(
            tx,
            (&account, &bucket),
            weighed.bucket_id.as_deref(),
            |tx| {
                tx.create_namespace(
                    (&account, &bucket),
                    &created,
                    &id,
                    (&who, crate::objects::now_ms()),
                )
            },
        )
    })
    .await?;
    Ok((
        StatusCode::OK,
        Some(json!({ "namespace": [namespace], "properties": {} })),
    ))
}

async fn load_namespace(
    ctx: &Ctx<'_>,
    bucket: &str,
    namespace: &str,
    head: bool,
) -> Result<(StatusCode, Option<Value>), TError> {
    let arn = ctx.caller.bucket_arn(bucket);
    (ctx.check)(
        "GetNamespace",
        &arn,
        &policies(ctx.state, ctx.caller, bucket, None).await?,
    )?;
    namespace_of(ctx, bucket, namespace).await?;
    Ok(if head {
        (StatusCode::NO_CONTENT, None)
    } else {
        (
            StatusCode::OK,
            Some(json!({ "namespace": [namespace], "properties": {} })),
        )
    })
}

async fn drop_namespace(
    ctx: &Ctx<'_>,
    bucket: &str,
    namespace: &str,
) -> Result<(StatusCode, Option<Value>), TError> {
    let arn = ctx.caller.bucket_arn(bucket);
    let weighed = policies(ctx.state, ctx.caller, bucket, None).await?;
    (ctx.check)("DeleteNamespace", &arn, &weighed)?;
    let (account, bucket, namespace) = (
        ctx.caller.account.to_string(),
        bucket.to_string(),
        namespace.to_string(),
    );
    write(ctx.state, move |tx| {
        guarded(
            tx,
            (&account, &bucket),
            weighed.bucket_id.as_deref(),
            |tx| tx.delete_namespace((&account, &bucket), &namespace),
        )
    })
    .await?;
    Ok((StatusCode::NO_CONTENT, None))
}

async fn list_tables(
    ctx: &Ctx<'_>,
    bucket: &str,
    namespace: &str,
) -> Result<(StatusCode, Option<Value>), TError> {
    let arn = ctx.caller.bucket_arn(bucket);
    (ctx.check)(
        "ListTables",
        &arn,
        &policies(ctx.state, ctx.caller, bucket, None).await?,
    )?;
    namespace_of(ctx, bucket, namespace).await?;
    let (limit, after) = page(ctx.params)?;
    let (account, listed, space) = (
        ctx.caller.account.to_string(),
        bucket.to_string(),
        namespace.to_string(),
    );
    let mut tables = read(ctx.state, move |file| {
        file.list_tables(
            (&account, &listed),
            Some(&space),
            "",
            after.as_deref().map(|name| (space.as_str(), name)),
            limit + 1,
        )
    })
    .await?;
    let more = tables.len() > limit;
    tables.truncate(limit);
    let mut answer = json!({
        "identifiers": tables
            .iter()
            .map(|table| json!({ "namespace": [table.namespace], "name": table.name }))
            .collect::<Vec<_>>(),
    });
    if more && let Some(last) = tables.last() {
        answer["next-page-token"] = json!(token(&last.name));
    }
    Ok((StatusCode::OK, Some(answer)))
}

/// `createTable`: its schema, partition spec, write order and properties
/// make its first metadata file, as CreateTable's `metadata` does.
async fn create_table(
    ctx: &Ctx<'_>,
    bucket: &str,
    namespace: String,
) -> Result<(StatusCode, Option<Value>), TError> {
    let input = ctx.input;
    only(
        input,
        &[
            "name",
            "schema",
            "location",
            "partition-spec",
            "write-order",
            "stage-create",
            "properties",
        ],
    )?;
    if input
        .get("stage-create")
        .is_some_and(|stage| stage != &json!(false))
    {
        return Err(TError::bad(
            "The stage-create option is not supported for this operation.",
        ));
    }
    if input
        .get("location")
        .is_some_and(|location| !location.is_null())
    {
        return Err(TError::bad(
            "A table's location is its own storage, made with it.",
        ));
    }
    let name = table_name(
        input
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| TError::bad("name is required."))?,
    )?
    .to_string();
    let mut iceberg = Map::new();
    iceberg.insert(
        "schemaV2".to_string(),
        input
            .get("schema")
            .cloned()
            .ok_or_else(|| TError::bad("schema is required."))?,
    );
    for (field, member) in [
        ("partition-spec", "partitionSpec"),
        ("write-order", "writeOrder"),
        ("properties", "properties"),
    ] {
        if let Some(value) = input.get(field).filter(|value| !value.is_null()) {
            iceberg.insert(member.to_string(), value.clone());
        }
    }
    let arn = ctx.caller.bucket_arn(bucket);
    let (table, file) = make_table(
        ctx.state,
        (ctx.caller, ctx.authz),
        (&arn, bucket.to_string()),
        (namespace, name),
        Some(&json!({ "iceberg": iceberg })),
    )
    .await?;
    Ok((StatusCode::OK, Some(loaded(&table, file))))
}

/// A LoadTableResult: where its metadata file is, and the file.
fn loaded(table: &TableInfo, file: Option<Value>) -> Value {
    json!({
        "metadata-location": table.metadata_location,
        "metadata": file,
        "config": {},
    })
}

/// The table at `(namespace, name)`, and the policies on it, its `action`
/// authorized (on the bucket's tables when it is not there: said only to
/// whom may).
async fn table_at(
    ctx: &Ctx<'_>,
    bucket: &str,
    (namespace, name): &(String, String),
    actions: &[&str],
) -> Result<(TableInfo, Weighed), TError> {
    let (account, looked, space, named) = (
        ctx.caller.account.to_string(),
        bucket.to_string(),
        namespace.clone(),
        name.clone(),
    );
    let found = read(ctx.state, move |file| {
        file.table((&account, &looked), (&space, &named))
    })
    .await?;
    let Some(table) = found else {
        let weighed = policies(ctx.state, ctx.caller, bucket, None).await?;
        for action in actions {
            (ctx.check)(action, &ctx.caller.table_arn(bucket, "*"), &weighed)?;
        }
        return Err(table_error(TableError::NoSuchTable));
    };
    let weighed = policies(ctx.state, ctx.caller, bucket, Some(&table.id)).await?;
    for action in actions {
        (ctx.check)(action, &ctx.caller.table_arn(bucket, &table.id), &weighed)?;
    }
    Ok((table, weighed))
}

/// `loadTable`: GetTableMetadataLocation and GetTableData, the file read
/// from its storage.
async fn load_table(
    ctx: &Ctx<'_>,
    bucket: &str,
    at: (String, String),
) -> Result<(StatusCode, Option<Value>), TError> {
    let (table, _) = table_at(
        ctx,
        bucket,
        &at,
        &["GetTableMetadataLocation", "GetTableData"],
    )
    .await?;
    let file = current_file(ctx, &table).await?;
    Ok((StatusCode::OK, Some(loaded(&table, Some(file)))))
}

/// The table's current metadata file, read from its storage (a gzipped
/// one decompressed).
async fn current_file(ctx: &Ctx<'_>, table: &TableInfo) -> Result<Value, TError> {
    let Some(location) = table.metadata_location.clone() else {
        return Err(TError::not_found(
            "The table does not exist as an Iceberg table yet: it has no metadata file.",
        ));
    };
    let gzipped = location.ends_with(".gz");
    let file = read(ctx.state, move |file| metadata_file(file, &location))
        .await?
        .map_err(TError::bad)?;
    // A gzipped one (`.metadata.json.gz`), read as far as the limit.
    let file = if gzipped {
        use std::io::Read as _;
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(file.as_slice())
            .take(MAX_METADATA_BYTES + 1)
            .read_to_end(&mut out)
            .map_err(|_| TError::bad("The table's metadata file is not gzip."))?;
        if out.len() as u64 > MAX_METADATA_BYTES {
            return Err(TError::bad(
                "A metadata.json file over 50MB is not supported.",
            ));
        }
        out
    } else {
        file
    };
    let file: Value = serde_json::from_slice(&file)
        .map_err(|_| TError::bad("The table's metadata file is not JSON."))?;
    Ok(file)
}

/// The bytes of the metadata file at `location` (`s3://<bucket>/<key>`),
/// or why it cannot be read.
fn metadata_file(
    file: &mut truthdb_object::ObjFile<'_>,
    location: &str,
) -> Result<Result<Vec<u8>, String>, truthdb_core::storage::StorageError> {
    let Some((bucket, key)) = location
        .strip_prefix("s3://")
        .and_then(|rest| rest.split_once('/'))
    else {
        return Ok(Err(
            "The table's metadata location is not an S3 URI.".to_string()
        ));
    };
    let info = match file.head_object(bucket, key, VersionSel::Latest)? {
        Ok(info) if !info.delete_marker => info,
        _ => return Ok(Err("The table's metadata file is missing.".to_string())),
    };
    if info.attrs.customer_key.is_some()
        || file.archived_unreadable(
            bucket,
            key,
            info.version,
            info.storage_class,
            crate::objects::now_ms(),
        )?
    {
        return Ok(Err("The table's metadata file cannot be read.".to_string()));
    }
    if info.size > MAX_METADATA_BYTES {
        return Ok(Err(
            "A metadata.json file over 50MB is not supported.".to_string()
        ));
    }
    let mut out = vec![0u8; info.size as usize];
    let mut done = 0usize;
    for (part, _, part_len) in file.body_layout(info.body)? {
        let take = (part_len as usize).min(out.len() - done);
        file.read_body(part, 0, &mut out[done..done + take])?;
        done += take;
    }
    Ok(Ok(out))
}

/// `updateTable`, a table's commit: its requirements checked and updates
/// applied to its current metadata, written as its next metadata file, and
/// the table pointed at it if it has not changed since it was read.
async fn commit_table(
    ctx: &Ctx<'_>,
    bucket: &str,
    at: (String, String),
) -> Result<(StatusCode, Option<Value>), TError> {
    only(ctx.input, &["identifier", "requirements", "updates"])?;
    if let Some(given) = ctx.input.get("identifier").filter(|given| !given.is_null())
        && identifier(Some(given), "identifier")? != at
    {
        return Err(TError::bad("identifier is not the table of the path."));
    }
    let list = |field: &str| -> Result<Vec<Value>, TError> {
        match ctx.input.get(field) {
            None | Some(Value::Null) => Ok(Vec::new()),
            Some(Value::Array(items)) => Ok(items.clone()),
            Some(_) => Err(TError::bad(format!("{field} is a list."))),
        }
    };
    let (requirements, updates) = (list("requirements")?, list("updates")?);
    let (table, _) = table_at(
        ctx,
        bucket,
        &at,
        &[
            "UpdateTableMetadataLocation",
            "PutTableData",
            "GetTableData",
        ],
    )
    .await?;
    let base = current_file(ctx, &table).await?;
    let location = table
        .metadata_location
        .clone()
        .expect("read: it has a metadata file");
    let now = crate::objects::now_ms();
    let next = match crate::iceberg_commit::apply(&base, &location, &requirements, &updates, now) {
        Ok(Some(next)) => next,
        Ok(None) => {
            return Ok((
                StatusCode::OK,
                Some(json!({ "metadata-location": location, "metadata": base })),
            ));
        }
        Err(crate::iceberg_commit::CommitError::Failed(message)) => {
            return Err(TError {
                status: 409,
                kind: "CommitFailedException",
                message,
            });
        }
        Err(crate::iceberg_commit::CommitError::Invalid(message)) => {
            return Err(TError::bad(message));
        }
    };
    // `<version>-<uuid>.metadata.json`, the version after the current's.
    let version = location
        .rsplit('/')
        .next()
        .and_then(|name| name.split(['-', '.']).next())
        .and_then(|digits| digits.parse::<u64>().ok())
        .map_or_else(
            || {
                next["metadata-log"]
                    .as_array()
                    .map_or(1, |log| log.len() as u64)
            },
            |version| version.saturating_add(1),
        );
    let key = format!("metadata/{version:05}-{}.metadata.json", super::uuid());
    let next_location = format!("{}/{key}", table.warehouse_location);
    // As large as a reader takes, at most.
    let file = next.to_string();
    if file.len() as u64 > MAX_METADATA_BYTES {
        return Err(TError::bad(
            "A metadata.json file over 50MB is not supported.",
        ));
    }
    let body = super::metadata_body(ctx.state, file, now).await?;
    let (account, bucket, who) = (
        ctx.caller.account.to_string(),
        bucket.to_string(),
        ctx.caller.who.to_string(),
    );
    let (namespace, name) = at;
    let (token, new_token) = (table.version_token.clone(), super::uuid().replace('-', ""));
    let (id, pointed) = (table.id.clone(), next_location.clone());
    write_with(ctx.state, move |tx| {
        // Only from the token read (the table has not changed since); a
        // refusal writes nothing.
        let pointed_at = tx.update_metadata_location(
            (&account, &bucket),
            (&namespace, &name, &id),
            (&token, &new_token),
            &pointed,
            (&who, now),
        )?;
        if pointed_at.is_err() {
            return Ok(pointed_at);
        }
        let attrs = truthdb_object::ObjectAttrs {
            content_type: Some("application/json".to_string()),
            ..truthdb_object::ObjectAttrs::default()
        };
        // Its storage is there with it (the table is, in this transaction).
        if tx
            .put_object(
                &truthdb_object::warehouse_bucket(&id),
                &key,
                &body,
                &attrs,
                truthdb_object::StorageClass::Standard,
                now,
                0,
            )?
            .is_err()
        {
            return Err(truthdb_core::storage::StorageError::InvalidFile(
                "a table's storage refused its metadata file".to_string(),
            ));
        }
        Ok(pointed_at)
    })
    .await?;
    Ok((
        StatusCode::OK,
        Some(json!({ "metadata-location": next_location, "metadata": next })),
    ))
}

async fn table_exists(
    ctx: &Ctx<'_>,
    bucket: &str,
    at: (String, String),
) -> Result<(StatusCode, Option<Value>), TError> {
    table_at(ctx, bucket, &at, &["GetTable"]).await?;
    Ok((StatusCode::NO_CONTENT, None))
}

/// `dropTable`: only with its files purged ("Dropping tables with
/// `purge=false` is not supported").
async fn drop_table(
    ctx: &Ctx<'_>,
    bucket: &str,
    at: (String, String),
) -> Result<(StatusCode, Option<Value>), TError> {
    // A boolean (PyIceberg's is `True`).
    if !param(ctx.params, "purgeRequested").is_some_and(|purge| purge.eq_ignore_ascii_case("true"))
    {
        return Err(TError::bad(
            "Dropping tables with purge=false is not supported.",
        ));
    }
    let (table, _) = table_at(ctx, bucket, &at, &["DeleteTable"]).await?;
    let (account, bucket) = (ctx.caller.account.to_string(), bucket.to_string());
    let (namespace, name) = at;
    write(ctx.state, move |tx| {
        tx.delete_table((&account, &bucket), (&namespace, &name, &table.id), None)
            .map(|deleted| deleted.map(|_| ()))
    })
    .await?;
    Ok((StatusCode::NO_CONTENT, None))
}

/// A TableIdentifier of a request body.
fn identifier(value: Option<&Value>, what: &str) -> Result<(String, String), TError> {
    let map = value
        .and_then(Value::as_object)
        .ok_or_else(|| TError::bad(format!("{what} is a table identifier.")))?;
    only(map, &["namespace", "name"])?;
    let namespace = level_of(map.get("namespace"), &format!("{what}.namespace"))?;
    let name = table_name(
        map.get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| TError::bad(format!("{what}.name is required.")))?,
    )?
    .to_string();
    Ok((namespace, name))
}

async fn rename_table(ctx: &Ctx<'_>, bucket: &str) -> Result<(StatusCode, Option<Value>), TError> {
    only(ctx.input, &["source", "destination"])?;
    let source = identifier(ctx.input.get("source"), "source")?;
    let (to_namespace, to_name) = identifier(ctx.input.get("destination"), "destination")?;
    let (table, _) = table_at(ctx, bucket, &source, &["RenameTable"]).await?;
    let (account, bucket, who) = (
        ctx.caller.account.to_string(),
        bucket.to_string(),
        ctx.caller.who.to_string(),
    );
    let (namespace, name) = source;
    write_with(ctx.state, move |tx| {
        tx.rename_table(
            (&account, &bucket),
            (&namespace, &name, &table.id),
            (&to_namespace, &to_name),
            None,
            (&who, crate::objects::now_ms()),
        )
    })
    .await?;
    Ok((StatusCode::NO_CONTENT, None))
}
