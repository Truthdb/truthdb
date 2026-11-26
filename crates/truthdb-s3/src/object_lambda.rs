//! S3 Object Lambda access points, through the S3 Control API (under
//! `/v20180820/accesspointforobjectlambda`): Create, Get, Delete and List,
//! an Object Lambda access point's configuration (Put, Get), its policy
//! (Put, Get, Delete) and its policy status. One is its account's (control
//! calls are not cross-account), named uniquely in it, with a generated
//! alias, and is configured with:
//! - its supporting access point (one of its account's, on this server);
//! - the transformations: for each of GetObject, HeadObject, ListObjects
//!   and ListObjectsV2 at most once, the function that transforms it (a
//!   topic of the account's with a webhook, as Batch Operations' LambdaInvoke
//!   invokes) and an optional payload for it;
//! - the features it allows (Range and partNumber requests) and whether its
//!   CloudWatch metrics are on.
//!
//! Its Block Public Access settings are all on and cannot change, so a
//! public policy is refused.

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use truthdb_iam::{Policy, PolicyKind};
use truthdb_object::ObjectLambdaPoint;

use crate::access_points::{
    elements, empty, escape, invalid, malformed, no_such_access_point, param, valid_name, xml,
};
use crate::authz::Authz;
use crate::error::S3Error;
use crate::server::{Authenticated, Body, Connection, S3State, read_body};

const XMLNS: &str = crate::batch::XMLNS;
/// The S3 Control path prefix of these calls.
pub(crate) const PREFIX: &str = "/v20180820/accesspointforobjectlambda";
const MAX_LISTED: usize = 1000;
/// A request body, at most (a policy is at most 20 KB).
const MAX_BODY: usize = 64 * 1024;
const MAX_POLICY: usize = 20 * 1024;
/// The alias's generated part and the alias's longest.
const ALIAS_RANDOM: usize = 34;
const MAX_ALIAS: usize = 63;
const ACTIONS: [&str; 4] = ["GetObject", "HeadObject", "ListObjects", "ListObjectsV2"];
const FEATURES: [&str; 4] = [
    "GetObject-Range",
    "GetObject-PartNumber",
    "HeadObject-Range",
    "HeadObject-PartNumber",
];

/// Whether `name` is an Object Lambda access point name: 3 to 45
/// characters, lowercase letters, digits and hyphens, beginning and ending
/// with a letter or digit.
fn valid_point_name(name: &str) -> bool {
    (3..=45).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
}

/// A new alias of Object Lambda access point `name`: its name (cut so the
/// alias fits 63 characters), a generated part, and `--ol-s3`.
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
    let room = MAX_ALIAS - ALIAS_RANDOM - "---ol-s3".len();
    let prefix = name[..name.len().min(room)].trim_end_matches('-');
    format!("{prefix}-{random}--ol-s3")
}

/// One transformation: the actions it serves, its function's ARN and the
/// payload it is given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Transformation {
    pub(crate) actions: Vec<String>,
    pub(crate) function_arn: String,
    pub(crate) payload: Option<String>,
}

/// An Object Lambda access point's configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Configuration {
    /// Its supporting access point's ARN.
    pub(crate) supporting: String,
    pub(crate) metrics: bool,
    pub(crate) features: Vec<String>,
    pub(crate) transformations: Vec<Transformation>,
}

impl Configuration {
    /// Its XML, as stored and as GetAccessPointConfigurationForObjectLambda
    /// gives it.
    pub(crate) fn render(&self) -> String {
        let features: String = self
            .features
            .iter()
            .map(|feature| format!("<AllowedFeature>{}</AllowedFeature>", escape(feature)))
            .collect();
        let transformations: String = self
            .transformations
            .iter()
            .map(|transformation| {
                let actions: String = transformation
                    .actions
                    .iter()
                    .map(|action| format!("<Action>{}</Action>", escape(action)))
                    .collect();
                let payload = transformation
                    .payload
                    .as_ref()
                    .map(|payload| format!("<FunctionPayload>{}</FunctionPayload>", escape(payload)))
                    .unwrap_or_default();
                format!(
                    "<TransformationConfiguration><Actions>{actions}</Actions><ContentTransformation><AwsLambda><FunctionArn>{}</FunctionArn>{payload}</AwsLambda></ContentTransformation></TransformationConfiguration>",
                    escape(&transformation.function_arn)
                )
            })
            .collect();
        format!(
            "<Configuration><SupportingAccessPoint>{}</SupportingAccessPoint><CloudWatchMetricsEnabled>{}</CloudWatchMetricsEnabled><AllowedFeatures>{features}</AllowedFeatures><TransformationConfigurations>{transformations}</TransformationConfigurations></Configuration>",
            escape(&self.supporting),
            self.metrics
        )
    }

    /// The configuration in `body` (a request of root `root`), or in a
    /// stored rendering (root `Configuration` itself).
    pub(crate) fn parse(body: &[u8], root: &str) -> Result<Configuration, S3Error> {
        let elements = elements(body)?;
        if elements.first().map(|(path, _)| path.as_str()) != Some(root) {
            return Err(malformed());
        }
        let prefix = match root {
            "Configuration" => "Configuration/".to_string(),
            _ => format!("{root}/Configuration/"),
        };
        let mut supporting = None;
        let mut metrics = false;
        let mut features: Vec<String> = Vec::new();
        let mut transformations: Vec<Transformation> = Vec::new();
        let mut seen_configuration = root == "Configuration";
        const TRANSFORMATION: &str = "TransformationConfigurations/TransformationConfiguration";
        for (path, text) in &elements[1..] {
            let text = text.trim();
            if path.as_str() == format!("{root}/Configuration") {
                seen_configuration = true;
                continue;
            }
            let Some(field) = path.strip_prefix(prefix.as_str()) else {
                return Err(malformed());
            };
            match field {
                "SupportingAccessPoint" => supporting = Some(text.to_string()),
                "CloudWatchMetricsEnabled" => {
                    metrics = match text.to_ascii_lowercase().as_str() {
                        "true" => true,
                        "false" => false,
                        _ => return Err(malformed()),
                    }
                }
                "AllowedFeatures" | "TransformationConfigurations" => {}
                "AllowedFeatures/AllowedFeature" => {
                    if !FEATURES.contains(&text) || features.iter().any(|known| known == text) {
                        return Err(invalid(format!(
                            "AllowedFeature {text} is not one of GetObject-Range, GetObject-PartNumber, HeadObject-Range and HeadObject-PartNumber, given once."
                        )));
                    }
                    features.push(text.to_string());
                }
                TRANSFORMATION => transformations.push(Transformation {
                    actions: Vec::new(),
                    function_arn: String::new(),
                    payload: None,
                }),
                other => {
                    let Some(inner) = other
                        .strip_prefix(TRANSFORMATION)
                        .and_then(|inner| inner.strip_prefix('/'))
                    else {
                        return Err(malformed());
                    };
                    let transformation = transformations.last_mut().ok_or_else(malformed)?;
                    match inner {
                        "Actions" | "ContentTransformation" | "ContentTransformation/AwsLambda" => {
                        }
                        "Actions/Action" => transformation.actions.push(text.to_string()),
                        "ContentTransformation/AwsLambda/FunctionArn" => {
                            transformation.function_arn = text.to_string();
                        }
                        "ContentTransformation/AwsLambda/FunctionPayload" => {
                            transformation.payload = Some(text.to_string());
                        }
                        _ => return Err(malformed()),
                    }
                }
            }
        }
        if !seen_configuration {
            return Err(malformed());
        }
        let supporting = supporting.ok_or_else(malformed)?;
        if transformations.is_empty() {
            return Err(invalid(
                "An Object Lambda access point has one or more TransformationConfigurations.",
            ));
        }
        let mut served: Vec<&str> = Vec::new();
        for transformation in &transformations {
            if transformation.actions.is_empty() {
                return Err(invalid(
                    "A TransformationConfiguration has one or more Actions.",
                ));
            }
            for action in &transformation.actions {
                if !ACTIONS.contains(&action.as_str()) || served.contains(&action.as_str()) {
                    return Err(invalid(format!(
                        "Action {action} is not one of GetObject, HeadObject, ListObjects and ListObjectsV2, transformed once."
                    )));
                }
                served.push(action);
            }
            if transformation.function_arn.len() > 1024
                || crate::batch::function_name(&transformation.function_arn).is_none()
            {
                return Err(invalid(format!(
                    "FunctionArn {} is not a function ARN.",
                    transformation.function_arn
                )));
            }
        }
        Ok(Configuration {
            supporting,
            metrics,
            features,
            transformations,
        })
    }
}

/// The supporting access point's account and name, if `arn` is an access
/// point's ARN in `region`.
pub(crate) fn supporting_point(arn: &str, region: &str) -> Option<(String, String)> {
    let rest = arn.strip_prefix(&format!("arn:aws:s3:{region}:"))?;
    let (account, name) = rest.split_once(":accesspoint/")?;
    (account.len() == 12 && account.bytes().all(|byte| byte.is_ascii_digit()) && valid_name(name))
        .then(|| (account.to_string(), name.to_string()))
}

/// The Object Lambda access point's ARN.
pub(crate) fn arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:s3-object-lambda:{region}:{account}:accesspoint/{name}")
}

fn alias_xml(point: &ObjectLambdaPoint) -> String {
    format!(
        "<Alias><Value>{}</Value><Status>READY</Status></Alias>",
        escape(&point.alias)
    )
}

/// Handles a request under [`PREFIX`].
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
    // Not cross-account, as an access point's control calls are not.
    if header_account != principal.account {
        return Err(S3Error::access_denied("Access Denied"));
    }
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let rest = path.strip_prefix(PREFIX).unwrap_or("");
    let segments: Vec<&str> = rest.split('/').skip(1).collect();
    let (action, name, sub) = match (&method, segments.as_slice()) {
        (&Method::GET, []) => ("s3:ListAccessPointsForObjectLambda", None, ""),
        (&Method::PUT, [name]) => ("s3:CreateAccessPointForObjectLambda", Some(*name), ""),
        (&Method::GET, [name]) => ("s3:GetAccessPointForObjectLambda", Some(*name), ""),
        (&Method::DELETE, [name]) => ("s3:DeleteAccessPointForObjectLambda", Some(*name), ""),
        (&Method::PUT, [name, "configuration"]) => (
            "s3:PutAccessPointConfigurationForObjectLambda",
            Some(*name),
            "configuration",
        ),
        (&Method::GET, [name, "configuration"]) => (
            "s3:GetAccessPointConfigurationForObjectLambda",
            Some(*name),
            "configuration",
        ),
        (&Method::PUT, [name, "policy"]) => (
            "s3:PutAccessPointPolicyForObjectLambda",
            Some(*name),
            "policy",
        ),
        (&Method::GET, [name, "policy"]) => (
            "s3:GetAccessPointPolicyForObjectLambda",
            Some(*name),
            "policy",
        ),
        (&Method::DELETE, [name, "policy"]) => (
            "s3:DeleteAccessPointPolicyForObjectLambda",
            Some(*name),
            "policy",
        ),
        (&Method::GET, [name, "policyStatus"]) => (
            "s3:GetAccessPointPolicyStatusForObjectLambda",
            Some(*name),
            "policyStatus",
        ),
        _ => return Err(S3Error::not_implemented(format!("{method} {path}"))),
    };
    let name = name
        .map(|name| {
            percent_encoding::percent_decode_str(name)
                .decode_utf8()
                .map(|name| name.into_owned())
                .map_err(|_| invalid("The Object Lambda access point name is not UTF-8."))
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
    let resource_arn = name.as_ref().map(|name| arn(&state.region, &account, name));
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
            (Some(name), "configuration", Method::PUT) => {
                put_configuration(state, request, auth, &account, &name).await
            }
            (Some(name), "configuration", _) => get_configuration(state, &account, &name).await,
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

/// The configuration of `body` (a request of root `root`), checked: its
/// supporting access point one of `account`'s, on this server.
async fn checked_configuration(
    state: &S3State,
    body: &[u8],
    root: &str,
    account: &str,
) -> Result<Configuration, S3Error> {
    let configuration = Configuration::parse(body, root)?;
    let (of, named) =
        supporting_point(&configuration.supporting, &state.region).ok_or_else(|| {
            invalid(format!(
                "SupportingAccessPoint {} is not an access point ARN of this Region.",
                configuration.supporting
            ))
        })?;
    if of != account {
        return Err(invalid(
            "The SupportingAccessPoint is another account's: it must be your own.",
        ));
    }
    crate::scope::object(state, move |file| file.access_point(&of, &named))
        .await?
        .ok_or_else(no_such_access_point)?;
    Ok(configuration)
}

async fn create(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    account: &str,
    name: &str,
) -> Result<Response<Body>, S3Error> {
    if !valid_point_name(name) {
        return Err(invalid(
            "An Object Lambda access point name is 3 to 45 lowercase letters, digits and hyphens, beginning and ending with a letter or digit.",
        ));
    }
    let body = read_body(request, auth, MAX_BODY).await?;
    let configuration = checked_configuration(
        state,
        &body,
        "CreateAccessPointForObjectLambdaRequest",
        account,
    )
    .await?;
    let point = ObjectLambdaPoint {
        account: account.to_string(),
        name: name.to_string(),
        configuration: configuration.render(),
        alias: new_alias(name),
        created_ms: crate::objects::now_ms(),
    };
    let stored = point.clone();
    let created = crate::scope::object(state, move |file| {
        file.transaction(|tx| tx.create_object_lambda_point(&stored))
    })
    .await?;
    if !created {
        return Err(S3Error::new(
            409,
            "AccessPointAlreadyOwnedByYou",
            "Your previous request to create the named accesspoint succeeded and you already own it.",
        ));
    }
    Ok(xml(
        StatusCode::OK,
        format!(
            "<CreateAccessPointForObjectLambdaResult xmlns=\"{XMLNS}\"><ObjectLambdaAccessPointArn>{}</ObjectLambdaAccessPointArn>{}</CreateAccessPointForObjectLambdaResult>",
            escape(&arn(&state.region, account, name)),
            alias_xml(&point)
        ),
    ))
}

async fn find(state: &S3State, account: &str, name: &str) -> Result<ObjectLambdaPoint, S3Error> {
    let (account, name) = (account.to_string(), name.to_string());
    crate::scope::object(state, move |file| file.object_lambda_point(&account, &name))
        .await?
        .ok_or_else(no_such_access_point)
}

async fn get(state: &S3State, account: &str, name: &str) -> Result<Response<Body>, S3Error> {
    let point = find(state, account, name).await?;
    Ok(xml(
        StatusCode::OK,
        format!(
            "<GetAccessPointForObjectLambdaResult xmlns=\"{XMLNS}\"><Name>{}</Name><PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration><CreationDate>{}</CreationDate>{}</GetAccessPointForObjectLambdaResult>",
            escape(&point.name),
            crate::time::iso8601(point.created_ms),
            alias_xml(&point)
        ),
    ))
}

async fn delete(state: &S3State, account: &str, name: &str) -> Result<Response<Body>, S3Error> {
    let (of, named) = (account.to_string(), name.to_string());
    let deleted = crate::scope::object(state, move |file| {
        file.transaction(|tx| tx.delete_object_lambda_point(&of, &named))
    })
    .await?;
    if !deleted {
        return Err(no_such_access_point());
    }
    Ok(empty(StatusCode::OK))
}

async fn list(state: &S3State, account: &str, query: &str) -> Result<Response<Body>, S3Error> {
    // 0 (as none) gives the most, as ListAccessPoints does here.
    let max = match param(query, "maxResults") {
        None => MAX_LISTED,
        Some(max) if max == "0" => MAX_LISTED,
        Some(max) => max
            .parse::<usize>()
            .ok()
            .filter(|max| *max <= MAX_LISTED)
            .ok_or_else(|| invalid("maxResults is from 0 to 1000."))?,
    };
    // The token is the last name given.
    let after = match param(query, "nextToken") {
        Some(token) if !valid_point_name(&token) => {
            return Err(S3Error::new(
                400,
                "InvalidNextTokenException",
                "The nextToken provided is invalid",
            ));
        }
        token => token,
    };
    let of = account.to_string();
    let page = crate::scope::object(state, move |file| {
        file.object_lambda_points(&of, after.as_deref(), max + 1)
    })
    .await?;
    let more = page.len() > max;
    let listed = &page[..page.len().min(max)];
    let points: String = listed
        .iter()
        .map(|point| {
            format!(
                "<ObjectLambdaAccessPoint><Name>{}</Name><ObjectLambdaAccessPointArn>{}</ObjectLambdaAccessPointArn>{}</ObjectLambdaAccessPoint>",
                escape(&point.name),
                escape(&arn(&state.region, &point.account, &point.name)),
                alias_xml(point)
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
            "<ListAccessPointsForObjectLambdaResult xmlns=\"{XMLNS}\"><ObjectLambdaAccessPointList>{points}</ObjectLambdaAccessPointList>{token}</ListAccessPointsForObjectLambdaResult>"
        ),
    ))
}

async fn put_configuration(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    account: &str,
    name: &str,
) -> Result<Response<Body>, S3Error> {
    find(state, account, name).await?;
    let body = read_body(request, auth, MAX_BODY).await?;
    let configuration = checked_configuration(
        state,
        &body,
        "PutAccessPointConfigurationForObjectLambdaRequest",
        account,
    )
    .await?;
    let (of, named, rendered) = (
        account.to_string(),
        name.to_string(),
        configuration.render(),
    );
    let set = crate::scope::object(state, move |file| {
        file.transaction(|tx| tx.set_object_lambda_configuration(&of, &named, &rendered))
    })
    .await?;
    if !set {
        return Err(no_such_access_point());
    }
    Ok(empty(StatusCode::OK))
}

async fn get_configuration(
    state: &S3State,
    account: &str,
    name: &str,
) -> Result<Response<Body>, S3Error> {
    let point = find(state, account, name).await?;
    Ok(xml(
        StatusCode::OK,
        format!(
            "<GetAccessPointConfigurationForObjectLambdaResult xmlns=\"{XMLNS}\">{}</GetAccessPointConfigurationForObjectLambdaResult>",
            point.configuration
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
    find(state, account, name).await?;
    let body = read_body(request, auth, MAX_BODY).await?;
    let elements = elements(&body)?;
    if elements.first().map(|(path, _)| path.as_str())
        != Some("PutAccessPointPolicyForObjectLambdaRequest")
    {
        return Err(malformed());
    }
    let text = elements
        .iter()
        .find(|(path, _)| path == "PutAccessPointPolicyForObjectLambdaRequest/Policy")
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
    // Its BlockPublicPolicy is on, for good.
    if truthdb_iam::is_public(&policy) {
        return Err(S3Error::access_denied(
            "Block Public Access forbids a public access point policy",
        ));
    }
    let (of, named) = (account.to_string(), name.to_string());
    let put = crate::scope::object(state, move |file| {
        file.transaction(|tx| tx.set_object_lambda_policy(&of, &named, Some(&text)))
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
    let policy = crate::scope::object(state, move |file| file.object_lambda_policy(&of, &named))
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
            "<GetAccessPointPolicyForObjectLambdaResult xmlns=\"{XMLNS}\"><Policy>{}</Policy></GetAccessPointPolicyForObjectLambdaResult>",
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
        file.transaction(|tx| tx.set_object_lambda_policy(&of, &named, None))
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
        match crate::scope::object(state, move |file| file.object_lambda_policy(&of, &named))
            .await?
        {
            Some(text) => truthdb_iam::is_public(
                &Policy::parse(&text, PolicyKind::Bucket).map_err(|err| {
                    S3Error::internal(format!(
                        "stored Object Lambda access point policy: {}",
                        err.0
                    ))
                })?,
            ),
            None => false,
        };
    Ok(xml(
        StatusCode::OK,
        format!(
            "<GetAccessPointPolicyStatusForObjectLambdaResult xmlns=\"{XMLNS}\"><PolicyStatus><IsPublic>{public}</IsPublic></PolicyStatus></GetAccessPointPolicyStatusForObjectLambdaResult>"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(inner: &str) -> String {
        format!(
            "<CreateAccessPointForObjectLambdaRequest xmlns=\"{XMLNS}\"><Configuration>{inner}</Configuration></CreateAccessPointForObjectLambdaRequest>"
        )
    }

    const SUPPORTING: &str = "<SupportingAccessPoint>arn:aws:s3:us-east-1:111111111111:accesspoint/reader</SupportingAccessPoint>";

    fn transformation(actions: &[&str], function: &str) -> String {
        let actions: String = actions
            .iter()
            .map(|action| format!("<Action>{action}</Action>"))
            .collect();
        format!(
            "<TransformationConfigurations><TransformationConfiguration><Actions>{actions}</Actions><ContentTransformation><AwsLambda><FunctionArn>{function}</FunctionArn></AwsLambda></ContentTransformation></TransformationConfiguration></TransformationConfigurations>"
        )
    }

    #[test]
    fn a_configuration_is_parsed_rendered_and_read_back() {
        let body = request(&format!(
            "{SUPPORTING}<CloudWatchMetricsEnabled>true</CloudWatchMetricsEnabled><AllowedFeatures><AllowedFeature>GetObject-Range</AllowedFeature></AllowedFeatures><TransformationConfigurations><TransformationConfiguration><Actions><Action>GetObject</Action><Action>HeadObject</Action></Actions><ContentTransformation><AwsLambda><FunctionArn>arn:aws:lambda:us-east-1:111111111111:function:shape</FunctionArn><FunctionPayload>{{\"a\": 1}}</FunctionPayload></AwsLambda></ContentTransformation></TransformationConfiguration><TransformationConfiguration><Actions><Action>ListObjectsV2</Action></Actions><ContentTransformation><AwsLambda><FunctionArn>list</FunctionArn></AwsLambda></ContentTransformation></TransformationConfiguration></TransformationConfigurations>"
        ));
        let parsed =
            Configuration::parse(body.as_bytes(), "CreateAccessPointForObjectLambdaRequest")
                .unwrap();
        assert_eq!(
            parsed,
            Configuration {
                supporting: "arn:aws:s3:us-east-1:111111111111:accesspoint/reader".into(),
                metrics: true,
                features: vec!["GetObject-Range".into()],
                transformations: vec![
                    Transformation {
                        actions: vec!["GetObject".into(), "HeadObject".into()],
                        function_arn: "arn:aws:lambda:us-east-1:111111111111:function:shape".into(),
                        payload: Some("{\"a\": 1}".into()),
                    },
                    Transformation {
                        actions: vec!["ListObjectsV2".into()],
                        function_arn: "list".into(),
                        payload: None,
                    },
                ],
            }
        );
        let rendered = parsed.render();
        assert_eq!(
            Configuration::parse(rendered.as_bytes(), "Configuration").unwrap(),
            parsed
        );
    }

    #[test]
    fn a_configuration_is_refused_for_what_it_lacks_or_repeats() {
        let shape = transformation(&["GetObject"], "shape");
        for (inner, why) in [
            (shape.clone(), "no supporting access point"),
            (SUPPORTING.to_string(), "no transformation"),
            (
                format!("{SUPPORTING}{}", transformation(&[], "shape")),
                "no action",
            ),
            (
                format!("{SUPPORTING}{}", transformation(&["PutObject"], "shape")),
                "an action not transformed",
            ),
            (
                format!(
                    "{SUPPORTING}{}",
                    transformation(&["GetObject", "GetObject"], "shape")
                ),
                "an action twice",
            ),
            (
                format!(
                    "{SUPPORTING}{}",
                    transformation(&["GetObject"], "arn:aws:s3:::x")
                ),
                "no function",
            ),
            (
                format!(
                    "{SUPPORTING}<AllowedFeatures><AllowedFeature>PutObject-Range</AllowedFeature></AllowedFeatures>{shape}"
                ),
                "a feature not known",
            ),
            (
                format!(
                    "{SUPPORTING}<AllowedFeatures><AllowedFeature>GetObject-Range</AllowedFeature><AllowedFeature>GetObject-Range</AllowedFeature></AllowedFeatures>{shape}"
                ),
                "a feature twice",
            ),
            (
                format!(
                    "{SUPPORTING}<CloudWatchMetricsEnabled>yes</CloudWatchMetricsEnabled>{shape}"
                ),
                "a flag not a boolean",
            ),
            (
                format!("{SUPPORTING}<Unknown/>{shape}"),
                "an unknown element",
            ),
        ] {
            assert!(
                Configuration::parse(
                    request(&inner).as_bytes(),
                    "CreateAccessPointForObjectLambdaRequest"
                )
                .is_err(),
                "{why}"
            );
        }
        // Two transformations of one action.
        let twice = format!(
            "{SUPPORTING}<TransformationConfigurations><TransformationConfiguration><Actions><Action>GetObject</Action></Actions><ContentTransformation><AwsLambda><FunctionArn>a</FunctionArn></AwsLambda></ContentTransformation></TransformationConfiguration><TransformationConfiguration><Actions><Action>GetObject</Action></Actions><ContentTransformation><AwsLambda><FunctionArn>b</FunctionArn></AwsLambda></ContentTransformation></TransformationConfiguration></TransformationConfigurations>"
        );
        assert!(
            Configuration::parse(
                request(&twice).as_bytes(),
                "CreateAccessPointForObjectLambdaRequest"
            )
            .is_err()
        );
    }

    #[test]
    fn names_aliases_and_supporting_arns_follow_their_rules() {
        assert!(valid_point_name("abc") && valid_point_name(&"a".repeat(45)));
        for bad in ["ab", &"a".repeat(46), "-ab", "ab-", "Abc", "a_b", "a.b"] {
            assert!(!valid_point_name(bad), "{bad}");
        }
        let alias = new_alias(&"n".repeat(45));
        assert!(
            alias.len() <= MAX_ALIAS && alias.ends_with("--ol-s3"),
            "{alias}"
        );
        assert_eq!(
            supporting_point(
                "arn:aws:s3:us-east-1:111111111111:accesspoint/reader",
                "us-east-1"
            ),
            Some(("111111111111".into(), "reader".into()))
        );
        for bad in [
            "arn:aws:s3:eu-west-1:111111111111:accesspoint/reader",
            "arn:aws:s3:us-east-1:1111:accesspoint/reader",
            "arn:aws:s3:us-east-1:111111111111:accesspoint/R",
            "arn:aws:s3-object-lambda:us-east-1:111111111111:accesspoint/reader",
        ] {
            assert_eq!(supporting_point(bad, "us-east-1"), None, "{bad}");
        }
    }
}
