//! S3 access points, through the S3 Control API (under
//! `/v20180820/accesspoint`): Create, Get, Delete and List, an access
//! point's policy (Put, Get, Delete) and its policy status. An access
//! point is its account's (control calls are not cross-account), named
//! uniquely in it, bound for good to one bucket (another account's too,
//! named with `BucketAccountId`), with Block Public Access settings fixed
//! at creation (all on by default) and a generated alias. Its network
//! origin is the Internet: a VPC configuration is not built (501).
//! Requests through access points (their ARN, alias or host) are the data
//! plane's, not built here.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_iam::{Policy, PolicyKind};
use truthdb_object::AccessPoint;

use crate::authz::Authz;
use crate::error::S3Error;
use crate::server::{Authenticated, Body, Connection, S3State, full, read_body};

const XMLNS: &str = crate::batch::XMLNS;
/// "You can create a maximum of 10,000 access points per AWS account per
/// AWS Region."
const MAX_ACCESS_POINTS: usize = 10_000;
/// ListAccessPoints: "up to 1,000 access points per call".
const MAX_LISTED: usize = 1000;
/// A CreateAccessPoint or PutAccessPointPolicy body, at most (a policy is
/// at most 20 KB).
const MAX_BODY: usize = 64 * 1024;
const MAX_POLICY: usize = 20 * 1024;
/// The alias's generated part ("METADATA" in `<name>-METADATA-s3alias`),
/// and the alias's longest (`Alias`, 63 characters at most).
const ALIAS_RANDOM: usize = 34;
const MAX_ALIAS: usize = 63;

/// Whether `name` is an access point name: 3 to 50 characters, lowercase
/// letters, digits and hyphens, beginning with a letter or digit, not
/// ending with a hyphen, nor with `-s3alias` or `-ext-s3alias`.
fn valid_name(name: &str) -> bool {
    (3..=50).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.ends_with("-s3alias")
        && !name.ends_with("-ext-s3alias")
}

/// A new alias of access point `name`: its name (cut so the alias fits 63
/// characters), a generated part, and `-s3alias`.
fn new_alias(name: &str) -> String {
    use ring::rand::SecureRandom as _;
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut bytes = [0u8; ALIAS_RANDOM];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .expect("the system's random source");
    let random: String = bytes
        .iter()
        .map(|byte| ALPHABET[*byte as usize % ALPHABET.len()] as char)
        .collect();
    let room = MAX_ALIAS - ALIAS_RANDOM - "--s3alias".len();
    let prefix = name[..name.len().min(room)].trim_end_matches('-');
    format!("{prefix}-{random}-s3alias")
}

fn no_such_access_point() -> S3Error {
    S3Error::new(
        404,
        "NoSuchAccessPoint",
        "The specified accesspoint does not exist",
    )
}

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::new(400, "InvalidRequest", message)
}

fn malformed() -> S3Error {
    S3Error::new(
        400,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published schema",
    )
}

fn xml(status: StatusCode, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response")
}

fn empty(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(full(Vec::new()))
        .expect("a valid response")
}

/// The elements of an XML document as paths (`A/B/C`) and their text, in
/// order (an element with no text, as `A/B` and nothing).
fn elements(body: &[u8]) -> Result<Vec<(String, String)>, S3Error> {
    use quick_xml::Reader;
    use quick_xml::events::Event;
    let mut reader = Reader::from_reader(body);
    let mut path: Vec<String> = Vec::new();
    let mut out = Vec::new();
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).map_err(|_| malformed())? {
            Event::Start(e) => {
                path.push(String::from_utf8_lossy(e.local_name().as_ref()).into_owned());
                out.push((path.join("/"), String::new()));
            }
            Event::Empty(e) => {
                path.push(String::from_utf8_lossy(e.local_name().as_ref()).into_owned());
                out.push((path.join("/"), String::new()));
                path.pop();
            }
            Event::Text(text) => {
                let text = text.unescape().map_err(|_| malformed())?;
                if let Some(last) = out.last_mut()
                    && last.0 == path.join("/")
                {
                    last.1.push_str(&text);
                }
            }
            Event::CData(text) => {
                if let Some(last) = out.last_mut()
                    && last.0 == path.join("/")
                {
                    last.1.push_str(&String::from_utf8_lossy(&text));
                }
            }
            Event::End(_) => {
                path.pop();
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(out)
}

fn escape(text: &str) -> String {
    quick_xml::escape::escape(text).into_owned()
}

/// A CreateAccessPoint request's settings: the bucket, the bucket's owner
/// if named, and the Block Public Access settings (all on unless given).
struct Create {
    bucket: String,
    bucket_account: Option<String>,
    block_public: [bool; 4],
}

const BLOCK_NAMES: [&str; 4] = [
    "BlockPublicAcls",
    "IgnorePublicAcls",
    "BlockPublicPolicy",
    "RestrictPublicBuckets",
];

fn parse_create(body: &[u8]) -> Result<Create, S3Error> {
    let elements = elements(body)?;
    if elements.first().map(|(path, _)| path.as_str()) != Some("CreateAccessPointRequest") {
        return Err(malformed());
    }
    let mut create = Create {
        bucket: String::new(),
        bucket_account: None,
        block_public: [true; 4],
    };
    for (path, text) in &elements[1..] {
        let text = text.trim();
        match path.strip_prefix("CreateAccessPointRequest/").unwrap_or("") {
            "Bucket" => create.bucket = text.to_string(),
            "BucketAccountId" => create.bucket_account = Some(text.to_string()),
            "PublicAccessBlockConfiguration" => {}
            setting if setting.starts_with("PublicAccessBlockConfiguration/") => {
                let field = &setting["PublicAccessBlockConfiguration/".len()..];
                let at = BLOCK_NAMES
                    .iter()
                    .position(|name| *name == field)
                    .ok_or_else(malformed)?;
                create.block_public[at] = match text.to_ascii_lowercase().as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(malformed()),
                };
            }
            "VpcConfiguration" | "VpcConfiguration/VpcId" => {
                return Err(S3Error::not_implemented(
                    "An access point of a VPC network origin",
                ));
            }
            other if other.starts_with("Scope") => {
                return Err(S3Error::not_implemented("An access point's Scope"));
            }
            other if other.starts_with("Tags") => {
                return Err(S3Error::not_implemented("An access point's tags"));
            }
            _ => return Err(malformed()),
        }
    }
    if create.bucket.is_empty() {
        return Err(malformed());
    }
    Ok(create)
}

/// The access point's ARN.
fn arn(state: &S3State, point: &AccessPoint) -> String {
    format!(
        "arn:aws:s3:{}:{}:accesspoint/{}",
        state.region, point.account, point.name
    )
}

/// An access point's elements, as Get and List give them.
fn describe(state: &S3State, point: &AccessPoint, whole: bool) -> String {
    let mut out = format!(
        "<Name>{}</Name><NetworkOrigin>Internet</NetworkOrigin><Bucket>{}</Bucket>",
        escape(&point.name),
        escape(&point.bucket)
    );
    if whole {
        let settings: String = BLOCK_NAMES
            .iter()
            .zip(point.block_public)
            .map(|(name, on)| format!("<{name}>{on}</{name}>"))
            .collect();
        out.push_str(&format!(
            "<PublicAccessBlockConfiguration>{settings}</PublicAccessBlockConfiguration><CreationDate>{}</CreationDate>",
            crate::time::iso8601(point.created_ms)
        ));
    }
    out.push_str(&format!(
        "<Alias>{}</Alias><AccessPointArn>{}</AccessPointArn>",
        escape(&point.alias),
        escape(&arn(state, point))
    ));
    if whole {
        // The host its requests go to, under each domain served.
        let endpoints: String = state
            .domains
            .iter()
            .take(1)
            .map(|domain| {
                format!(
                    "<entry><key>ipv4</key><value>s3-accesspoint.{}.{}</value></entry>",
                    escape(&state.region),
                    escape(domain)
                )
            })
            .collect();
        out.push_str(&format!("<Endpoints>{endpoints}</Endpoints>"));
    }
    out.push_str(&format!(
        "<BucketAccountId>{}</BucketAccountId>",
        escape(&point.bucket_account)
    ));
    out
}

/// Handles a request under `/v20180820/accesspoint`.
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
    // "API operations that control access point functionality ... don't
    // support cross-account calls."
    if header_account != principal.account {
        return Err(S3Error::access_denied("Access Denied"));
    }
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let rest = path.strip_prefix("/v20180820/accesspoint").unwrap_or("");
    let segments: Vec<&str> = rest.split('/').skip(1).collect();
    let (action, name, sub) = match (&method, segments.as_slice()) {
        (&Method::GET, []) => ("s3:ListAccessPoints", None, ""),
        (&Method::PUT, [name]) => ("s3:CreateAccessPoint", Some(*name), ""),
        (&Method::GET, [name]) => ("s3:GetAccessPoint", Some(*name), ""),
        (&Method::DELETE, [name]) => ("s3:DeleteAccessPoint", Some(*name), ""),
        (&Method::PUT, [name, "policy"]) => ("s3:PutAccessPointPolicy", Some(*name), "policy"),
        (&Method::GET, [name, "policy"]) => ("s3:GetAccessPointPolicy", Some(*name), "policy"),
        (&Method::DELETE, [name, "policy"]) => {
            ("s3:DeleteAccessPointPolicy", Some(*name), "policy")
        }
        (&Method::GET, [name, "policyStatus"]) => {
            ("s3:GetAccessPointPolicyStatus", Some(*name), "policyStatus")
        }
        _ => return Err(S3Error::not_implemented(format!("{method} {path}"))),
    };
    let name = name
        .map(|name| {
            percent_encoding::percent_decode_str(name)
                .decode_utf8()
                .map(|name| name.into_owned())
                .map_err(|_| invalid("The access point name is not UTF-8."))
        })
        .transpose()?;
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
    let resource_arn = name
        .as_ref()
        .map(|name| format!("arn:aws:s3:{}:{account}:accesspoint/{name}", state.region));
    match &resource_arn {
        Some(resource) => authz.check(action, crate::authz::Resource::Arn(resource))?,
        None => authz.check(action, crate::authz::Resource::Service)?,
    }
    let scope = crate::scope::Scope {
        key: Some((principal.access_key.clone(), principal.account.clone())),
        buckets: Vec::new(),
        origin: None,
    };
    crate::scope::within(scope, async move {
        match (name, sub, method) {
            (None, _, _) => list(state, &account, &query).await,
            (Some(name), "", Method::PUT) => create(state, request, auth, &account, &name).await,
            (Some(name), "", Method::GET) => get(state, &account, &name).await,
            (Some(name), "", _) => delete(state, &account, &name).await,
            (Some(name), "policy", Method::PUT) => {
                put_policy(state, request, auth, &account, &name).await
            }
            (Some(name), "policy", Method::GET) => get_policy(state, &account, &name).await,
            (Some(name), "policy", _) => delete_policy(state, &account, &name).await,
            (Some(name), _, _) => policy_status(state, &account, &name).await,
        }
    })
    .await
}

async fn create(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    account: &str,
    name: &str,
) -> Result<Response<Body>, S3Error> {
    if !valid_name(name) {
        return Err(invalid(
            "An access point name is 3 to 50 lowercase letters, digits and hyphens, beginning with a letter or digit, not ending with a hyphen or -s3alias.",
        ));
    }
    let body = read_body(request, auth, MAX_BODY).await?;
    let create = parse_create(&body)?;
    let bucket = create.bucket.clone();
    let info = crate::scope::object(state, move |file| file.bucket(&bucket)).await?;
    let no_such_bucket = || {
        S3Error::new(
            404,
            "NoSuchBucket",
            format!("The bucket {} does not exist", create.bucket),
        )
    };
    // A general purpose bucket's ("exactly one Amazon S3 general purpose
    // bucket"): a table's storage is reached through S3 Tables only.
    let info = info
        .filter(|info| info.warehouse.is_none())
        .ok_or_else(no_such_bucket)?;
    if info.zone.is_some() {
        return Err(S3Error::not_implemented(
            "An access point of a directory bucket",
        ));
    }
    let owner = info.owner;
    // Another account's bucket only as that account's, named.
    match &create.bucket_account {
        Some(named) if *named != owner => {
            return Err(invalid(
                "BucketAccountId is not the account that owns the bucket.",
            ));
        }
        None if owner != account => {
            return Err(invalid(
                "The bucket is another account's: name it with BucketAccountId.",
            ));
        }
        _ => {}
    }
    let point = AccessPoint {
        account: account.to_string(),
        name: name.to_string(),
        bucket: create.bucket.clone(),
        bucket_account: owner,
        vpc: None,
        block_public: create.block_public,
        alias: new_alias(name),
        created_ms: crate::objects::now_ms(),
    };
    let (stored, of) = (point.clone(), account.to_string());
    let created = crate::scope::object(state, move |file| {
        file.transaction(|tx| {
            if tx.access_point_count(&of)? >= MAX_ACCESS_POINTS {
                return Ok(Err(()));
            }
            tx.create_access_point(&stored).map(Ok)
        })
    })
    .await?;
    match created {
        Err(()) => Err(invalid(format!(
            "An account has at most {MAX_ACCESS_POINTS} access points."
        ))),
        Ok(false) => Err(S3Error::new(
            409,
            "AccessPointAlreadyOwnedByYou",
            "Your previous request to create the named accesspoint succeeded and you already own it.",
        )),
        Ok(true) => Ok(xml(
            StatusCode::OK,
            format!(
                "<CreateAccessPointResult xmlns=\"{XMLNS}\"><AccessPointArn>{}</AccessPointArn><Alias>{}</Alias></CreateAccessPointResult>",
                escape(&arn(state, &point)),
                escape(&point.alias)
            ),
        )),
    }
}

async fn find(state: &S3State, account: &str, name: &str) -> Result<AccessPoint, S3Error> {
    let (account, name) = (account.to_string(), name.to_string());
    crate::scope::object(state, move |file| file.access_point(&account, &name))
        .await?
        .ok_or_else(no_such_access_point)
}

async fn get(state: &S3State, account: &str, name: &str) -> Result<Response<Body>, S3Error> {
    let point = find(state, account, name).await?;
    Ok(xml(
        StatusCode::OK,
        format!(
            "<GetAccessPointResult xmlns=\"{XMLNS}\">{}</GetAccessPointResult>",
            describe(state, &point, true)
        ),
    ))
}

async fn delete(state: &S3State, account: &str, name: &str) -> Result<Response<Body>, S3Error> {
    let (of, named) = (account.to_string(), name.to_string());
    let deleted = crate::scope::object(state, move |file| {
        file.transaction(|tx| tx.delete_access_point(&of, &named))
    })
    .await?;
    if !deleted {
        return Err(no_such_access_point());
    }
    Ok(empty(StatusCode::OK))
}

/// The query parameter `name` of `query`, decoded.
fn param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then(|| {
            percent_encoding::percent_decode_str(&value.replace('+', " "))
                .decode_utf8_lossy()
                .into_owned()
        })
    })
}

async fn list(state: &S3State, account: &str, query: &str) -> Result<Response<Body>, S3Error> {
    // 0 (as none) gives the most: none would give a page no client could
    // go on from.
    let max = match param(query, "maxResults") {
        None => MAX_LISTED,
        Some(max) if max == "0" => MAX_LISTED,
        Some(max) => max
            .parse::<usize>()
            .ok()
            .filter(|max| *max <= MAX_LISTED)
            .ok_or_else(|| invalid("maxResults is from 0 to 1000."))?,
    };
    let bucket = param(query, "bucket");
    if let Some(kind) = param(query, "dataSourceType")
        && kind != "ALL"
        && kind != "S3"
    {
        return Err(S3Error::not_implemented(
            "Access points of data sources other than S3 buckets",
        ));
    }
    // The token is the last name given.
    let after = match param(query, "nextToken") {
        Some(token) if !valid_name(&token) => {
            return Err(S3Error::new(
                400,
                "InvalidNextTokenException",
                "The nextToken provided is invalid",
            ));
        }
        token => token,
    };
    let of = account.to_string();
    let mut listed = Vec::new();
    let mut cursor = after;
    let mut more = false;
    // Filtered by bucket, a page may take more than one read.
    loop {
        let (from, at) = (of.clone(), cursor.clone());
        let page = crate::scope::object(state, move |file| {
            file.access_points(&from, at.as_deref(), MAX_LISTED + 1)
        })
        .await?;
        let full_page = page.len() > MAX_LISTED;
        for point in page.into_iter().take(MAX_LISTED) {
            cursor = Some(point.name.clone());
            if bucket
                .as_ref()
                .is_some_and(|bucket| *bucket != point.bucket)
            {
                continue;
            }
            if listed.len() == max {
                more = true;
                break;
            }
            listed.push(point);
        }
        if more || !full_page {
            break;
        }
    }
    let points: String = listed
        .iter()
        .map(|point| {
            format!(
                "<AccessPoint>{}</AccessPoint>",
                describe(state, point, false)
            )
        })
        .collect();
    let token = match (more, listed.last()) {
        (true, Some(last)) => format!("<NextToken>{}</NextToken>", escape(&last.name)),
        _ => String::new(),
    };
    Ok(xml(
        StatusCode::OK,
        format!(
            "<ListAccessPointsResult xmlns=\"{XMLNS}\"><AccessPointList>{points}</AccessPointList>{token}</ListAccessPointsResult>"
        ),
    ))
}

async fn put_policy(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    account: &str,
    name: &str,
) -> Result<Response<Body>, S3Error> {
    let point = find(state, account, name).await?;
    let body = read_body(request, auth, MAX_BODY).await?;
    let elements = elements(&body)?;
    if elements.first().map(|(path, _)| path.as_str()) != Some("PutAccessPointPolicyRequest") {
        return Err(malformed());
    }
    let text = elements
        .iter()
        .find(|(path, _)| path == "PutAccessPointPolicyRequest/Policy")
        .map(|(_, text)| text.clone())
        .ok_or_else(malformed)?;
    if text.len() > MAX_POLICY {
        return Err(S3Error::new(
            400,
            "MalformedPolicy",
            "Policies must be at most 20 KB.",
        ));
    }
    let policy = Policy::parse(&text, PolicyKind::Bucket)
        .map_err(|err| S3Error::new(400, "MalformedPolicy", err.0))?;
    // BlockPublicPolicy refuses a public policy, as a bucket's does.
    if point.block_public[2] && truthdb_iam::is_public(&policy) {
        return Err(S3Error::access_denied(
            "Block Public Access forbids a public access point policy",
        ));
    }
    let (of, named) = (account.to_string(), name.to_string());
    let put = crate::scope::object(state, move |file| {
        file.transaction(|tx| tx.set_access_point_policy(&of, &named, Some(&text)))
    })
    .await?;
    if !put {
        return Err(no_such_access_point());
    }
    Ok(empty(StatusCode::OK))
}

async fn get_policy(state: &S3State, account: &str, name: &str) -> Result<Response<Body>, S3Error> {
    find(state, account, name).await?;
    let (of, named) = (account.to_string(), name.to_string());
    let policy = crate::scope::object(state, move |file| file.access_point_policy(&of, &named))
        .await?
        .ok_or_else(|| {
            S3Error::new(
                404,
                "NoSuchAccessPointPolicy",
                "The specified accesspoint policy does not exist",
            )
        })?;
    Ok(xml(
        StatusCode::OK,
        format!(
            "<GetAccessPointPolicyResult xmlns=\"{XMLNS}\"><Policy>{}</Policy></GetAccessPointPolicyResult>",
            escape(&policy)
        ),
    ))
}

async fn delete_policy(
    state: &S3State,
    account: &str,
    name: &str,
) -> Result<Response<Body>, S3Error> {
    let (of, named) = (account.to_string(), name.to_string());
    let deleted = crate::scope::object(state, move |file| {
        file.transaction(|tx| tx.set_access_point_policy(&of, &named, None))
    })
    .await?;
    if !deleted {
        return Err(no_such_access_point());
    }
    Ok(empty(StatusCode::OK))
}

async fn policy_status(
    state: &S3State,
    account: &str,
    name: &str,
) -> Result<Response<Body>, S3Error> {
    find(state, account, name).await?;
    let (of, named) = (account.to_string(), name.to_string());
    let public =
        match crate::scope::object(state, move |file| file.access_point_policy(&of, &named)).await?
        {
            Some(text) => {
                truthdb_iam::is_public(&Policy::parse(&text, PolicyKind::Bucket).map_err(
                    |err| S3Error::internal(format!("stored access point policy: {}", err.0)),
                )?)
            }
            None => false,
        };
    Ok(xml(
        StatusCode::OK,
        format!(
            "<GetAccessPointPolicyStatusResult xmlns=\"{XMLNS}\"><PolicyStatus><IsPublic>{public}</IsPublic></PolicyStatus></GetAccessPointPolicyStatusResult>"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_point_names_follow_awss_rules() {
        for good in ["abc", "my-access-point", "a1b2", &"a".repeat(50)] {
            assert!(valid_name(good), "{good}");
        }
        for bad in [
            "ab",
            &"a".repeat(51),
            "-abc",
            "abc-",
            "Abc",
            "a_b",
            "a.b",
            "a b",
            "my-s3alias",
            "my-ext-s3alias",
        ] {
            assert!(!valid_name(bad), "{bad}");
        }
    }

    #[test]
    fn an_alias_is_the_name_a_generated_part_and_s3alias() {
        let alias = new_alias("my-access-point");
        let generated = alias
            .strip_prefix("my-access-point-")
            .and_then(|rest| rest.strip_suffix("-s3alias"))
            .unwrap();
        assert_eq!(generated.len(), ALIAS_RANDOM);
        assert!(
            generated
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        );
        assert_ne!(new_alias("my-access-point"), alias);
        // A long name is cut so the alias fits.
        let long = new_alias(&"a-".repeat(25));
        assert!(long.len() <= MAX_ALIAS, "{long}");
        assert!(!long.starts_with('-') && !long.contains("--"), "{long}");
    }

    #[test]
    fn a_create_request_is_read_and_unbuilt_settings_refused() {
        let body = format!(
            "<CreateAccessPointRequest xmlns=\"{XMLNS}\"><Bucket>data</Bucket><PublicAccessBlockConfiguration><BlockPublicPolicy>false</BlockPublicPolicy></PublicAccessBlockConfiguration></CreateAccessPointRequest>"
        );
        let create = parse_create(body.as_bytes()).unwrap();
        assert_eq!(create.bucket, "data");
        assert_eq!(create.block_public, [true, true, false, true]);
        assert!(create.bucket_account.is_none());
        for (part, status) in [
            (
                "<VpcConfiguration><VpcId>vpc-1</VpcId></VpcConfiguration>",
                501,
            ),
            ("<Tags><Tag><Key>a</Key><Value>b</Value></Tag></Tags>", 501),
            (
                "<Scope><Prefixes><Prefix>a</Prefix></Prefixes></Scope>",
                501,
            ),
            ("<Unknown>1</Unknown>", 400),
        ] {
            let body = format!(
                "<CreateAccessPointRequest><Bucket>data</Bucket>{part}</CreateAccessPointRequest>"
            );
            assert_eq!(
                parse_create(body.as_bytes()).err().unwrap().status,
                status,
                "{part}"
            );
        }
        assert!(parse_create(b"<CreateAccessPointRequest></CreateAccessPointRequest>").is_err());
        assert!(parse_create(b"<Other><Bucket>data</Bucket></Other>").is_err());
    }
}
