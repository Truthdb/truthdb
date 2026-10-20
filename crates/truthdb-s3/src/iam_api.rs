//! An IAM API subset, and STS GetCallerIdentity, on the S3 listener: the
//! AWS Query protocol (`Action=...&Version=...`, form-encoded, answered in
//! XML), told apart by the SigV4 service (`iam`, `sts`).
//!
//! Users: CreateUser, GetUser, ListUsers, DeleteUser. Access keys:
//! CreateAccessKey, ListAccessKeys, UpdateAccessKey, DeleteAccessKey. Inline
//! policies: Put/Get/List/DeleteUserPolicy. Managed policies (one version
//! each): CreatePolicy, GetPolicy, GetPolicyVersion, ListPolicies,
//! DeletePolicy, Attach/DetachUserPolicy, ListAttachedUserPolicies. No
//! groups, roles or paths other than `/`.
//!
//! Each action is authorized as `iam:<Action>` on the user's or the policy's
//! ARN, within the caller's own account: its root may do everything, its
//! users what their policies allow.

use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use truthdb_iam::{Identity, Policy, PolicyKind};
use truthdb_object::{AccountError, ObjFile};

use crate::error::{S3Error, escape};
use crate::server::{Body, Connection, Principal, S3State, full};

const IAM_XMLNS: &str = "https://iam.amazonaws.com/doc/2010-05-08/";
const STS_XMLNS: &str = "https://sts.amazonaws.com/doc/2011-06-15/";

/// The largest request body.
const MAX_BODY: usize = 1 << 20;

/// An IAM error: HTTP status, code, message.
struct IamError {
    status: u16,
    code: &'static str,
    message: String,
}

impl IamError {
    fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        IamError {
            status,
            code,
            message: message.into(),
        }
    }

    fn validation(message: impl Into<String>) -> Self {
        IamError::new(400, "ValidationError", message)
    }

    fn no_such_entity(message: impl Into<String>) -> Self {
        IamError::new(404, "NoSuchEntity", message)
    }
}

impl From<S3Error> for IamError {
    fn from(err: S3Error) -> Self {
        let code = match (err.status, err.code) {
            (403, "AccessDenied") => "AccessDenied",
            (403, "SignatureDoesNotMatch") => "SignatureDoesNotMatch",
            (403, "RequestTimeTooSkewed") => "RequestExpired",
            (403, _) => "InvalidClientTokenId",
            _ => match err.status {
                400 => "InvalidRequest",
                408 => "RequestTimeout",
                413 => "ValidationError",
                503 => "ServiceUnavailable",
                _ => "ServiceFailure",
            },
        };
        // IAM answers an expired request with 400, where S3 says 403.
        let status = if code == "RequestExpired" {
            400
        } else {
            err.status
        };
        IamError::new(status, code, err.message)
    }
}

fn account_error(err: AccountError) -> IamError {
    match err {
        AccountError::NoSuchAccount => IamError::no_such_entity("The account does not exist."),
        AccountError::NoSuchUser => IamError::no_such_entity("The user does not exist."),
        AccountError::NoSuchPolicy => IamError::no_such_entity("The policy does not exist."),
        AccountError::UserExists => {
            IamError::new(409, "EntityAlreadyExists", "The user already exists.")
        }
        AccountError::PolicyExists => {
            IamError::new(409, "EntityAlreadyExists", "The policy already exists.")
        }
        AccountError::AccountExists | AccountError::DefaultAccount => {
            IamError::new(409, "EntityAlreadyExists", "The account already exists.")
        }
        AccountError::NotEmpty(what) => IamError::new(
            409,
            "DeleteConflict",
            format!("Cannot delete the entity: it still has {what}."),
        ),
        AccountError::InvalidName(why) => IamError::validation(format!("The name {why}.")),
    }
}

/// Answers one IAM (`service` "iam") or STS ("sts") request.
pub async fn handle(state: &S3State, request: Request<Incoming>, service: &str) -> Response<Body> {
    let xmlns = if service == "sts" {
        STS_XMLNS
    } else {
        IAM_XMLNS
    };
    let request_id = request_id();
    let (status, body) = match serve(state, request, service).await {
        Ok((action, result)) => (
            200,
            format!(
                "<{action}Response xmlns=\"{xmlns}\">{result}<ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata></{action}Response>"
            ),
        ),
        Err(err) => (
            err.status,
            format!(
                "<ErrorResponse xmlns=\"{xmlns}\"><Error><Type>{}</Type><Code>{}</Code><Message>{}</Message></Error><RequestId>{request_id}</RequestId></ErrorResponse>",
                if err.status >= 500 {
                    "Receiver"
                } else {
                    "Sender"
                },
                err.code,
                escape(&err.message)
            ),
        ),
    };
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "text/xml")
        .body(full(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"
        )))
        .expect("a valid response")
}

/// A random request id.
fn request_id() -> String {
    random(16, b"0123456789abcdef")
}

/// `len` characters from `alphabet` (16, 32 or 64 symbols, so a random byte
/// maps without bias), by the system's secure random generator.
fn random(len: usize, alphabet: &[u8]) -> String {
    use ring::rand::SecureRandom;
    let mut bytes = vec![0u8; len];
    if ring::rand::SystemRandom::new().fill(&mut bytes).is_err() {
        // No randomness: no identifier either (the caller fails).
        return String::new();
    }
    bytes
        .iter()
        .map(|byte| alphabet[*byte as usize % alphabet.len()] as char)
        .collect()
}

const ID_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
const SECRET_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// A fresh random id: `prefix` and 17 characters (an IAM unique id or an
/// access key id).
fn fresh_id(prefix: &str) -> Result<String, IamError> {
    let tail = random(17, ID_ALPHABET);
    if tail.is_empty() {
        return Err(IamError::new(
            500,
            "ServiceFailure",
            "The system random generator failed.",
        ));
    }
    Ok(format!("{prefix}{tail}"))
}

/// The request's parameters: its query and its form-encoded body.
fn params(query: &str, body: &[u8]) -> Vec<(String, String)> {
    let decode = |text: &str| {
        percent_encoding::percent_decode_str(&text.replace('+', " "))
            .decode_utf8_lossy()
            .into_owned()
    };
    let body = String::from_utf8_lossy(body);
    query
        .split('&')
        .chain(body.split('&'))
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(key), decode(value))
        })
        .collect()
}

struct Input {
    params: Vec<(String, String)>,
}

impl Input {
    fn get(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn required(&self, name: &str) -> Result<&str, IamError> {
        self.get(name)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| IamError::new(400, "MissingParameter", format!("{name} is required.")))
    }

    /// The required `UserName`, which must follow IAM's user name rule.
    fn user(&self) -> Result<String, IamError> {
        let name = self.required("UserName")?;
        check_user_name(name)?;
        Ok(name.to_string())
    }

    /// The required `PolicyName`, which must follow IAM's policy name rule.
    fn policy(&self) -> Result<String, IamError> {
        let name = self.required("PolicyName")?;
        check_policy_name(name)?;
        Ok(name.to_string())
    }
}

/// IAM's user name rule: 1 to 64 letters, digits and `+=,.@_-`.
fn check_user_name(name: &str) -> Result<(), IamError> {
    if !(1..=64).contains(&name.len())
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+=,.@_-".contains(&b))
    {
        return Err(IamError::validation(
            "A user name is 1 to 64 letters, digits or +=,.@_- characters.",
        ));
    }
    Ok(())
}

async fn serve(
    state: &S3State,
    request: Request<Incoming>,
    service: &str,
) -> Result<(String, String), IamError> {
    let signed = crate::signed_body::read(state, request, service, MAX_BODY).await?;
    let input = Input {
        params: params(signed.parts.uri.query().unwrap_or(""), &signed.body),
    };
    let action = input.required("Action")?.to_string();
    let principal = signed.principal;
    let authz = crate::authz::Authz::load(
        state,
        Some(&principal),
        &signed.parts.headers,
        "",
        signed.parts.extensions.get::<Connection>().copied(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    let scope = crate::scope::Scope {
        key: Some((principal.access_key.clone(), principal.account.clone())),
        buckets: Vec::new(),
    };
    let result = crate::scope::within(scope, async {
        if service == "sts" {
            return match action.as_str() {
                "GetCallerIdentity" => Ok(caller_identity(&authz.identity)),
                other => Err(IamError::new(
                    400,
                    "InvalidAction",
                    format!("{other} is not supported."),
                )),
            };
        }
        dispatch(state, &principal, &authz, &action, &input).await
    })
    .await?;
    Ok((action, result))
}

fn caller_identity(identity: &Identity) -> String {
    let (arn, user_id, account) = match identity {
        Identity::Root { account } => (
            format!("arn:aws:iam::{account}:root"),
            account.clone(),
            account.clone(),
        ),
        Identity::User {
            account,
            name,
            user_id,
        } => (
            format!("arn:aws:iam::{account}:user/{name}"),
            user_id.clone(),
            account.clone(),
        ),
        Identity::Anonymous => (String::new(), String::new(), String::new()),
    };
    format!(
        "<GetCallerIdentityResult><Arn>{}</Arn><UserId>{}</UserId><Account>{account}</Account></GetCallerIdentityResult>",
        escape(&arn),
        escape(&user_id)
    )
}

/// Runs `op` on the object engine within the request's scope.
async fn object<T, F>(state: &S3State, op: F) -> Result<T, IamError>
where
    T: Send + 'static,
    F: FnOnce(&mut ObjFile<'_>) -> Result<T, truthdb_core::storage::StorageError> + Send + 'static,
{
    Ok(crate::scope::object(state, op).await?)
}

fn user_arn(account: &str, name: &str) -> String {
    format!("arn:aws:iam::{account}:user/{name}")
}

fn policy_arn(account: &str, name: &str) -> String {
    format!("arn:aws:iam::{account}:policy/{name}")
}

/// A managed policy's name from its ARN, which must be of the caller's
/// account.
fn policy_name<'a>(account: &str, arn: &'a str) -> Result<&'a str, IamError> {
    arn.strip_prefix("arn:aws:iam::")
        .and_then(|rest| rest.strip_prefix(account))
        .and_then(|rest| rest.strip_prefix(":policy/"))
        .filter(|name| check_policy_name(name).is_ok())
        .ok_or_else(|| IamError::no_such_entity(format!("Policy {arn} does not exist.")))
}

/// The user an action names (`UserName`), or the calling user's own name
/// when it names none; `None` for the account's root.
fn subject(input: &Input, principal: &Principal) -> Result<Option<String>, IamError> {
    match input.get("UserName").filter(|name| !name.is_empty()) {
        Some(name) => {
            check_user_name(name)?;
            Ok(Some(name.to_string()))
        }
        None => Ok(principal.user.clone()),
    }
}

fn time(ms: u64) -> String {
    crate::time::iso8601(ms)
}

fn user_xml(account: &str, user: &truthdb_object::UserInfo) -> String {
    format!(
        "<Path>/</Path><UserName>{}</UserName><UserId>{}</UserId><Arn>{}</Arn><CreateDate>{}</CreateDate>",
        escape(&user.name),
        escape(&user.user_id),
        escape(&user_arn(account, &user.name)),
        time(user.created_ms)
    )
}

fn policy_xml(account: &str, policy: &truthdb_object::ManagedPolicy, attachments: u32) -> String {
    format!(
        "<PolicyName>{}</PolicyName><PolicyId>{}</PolicyId><Arn>{}</Arn><Path>/</Path><DefaultVersionId>v1</DefaultVersionId><AttachmentCount>{attachments}</AttachmentCount><PermissionsBoundaryUsageCount>0</PermissionsBoundaryUsageCount><IsAttachable>true</IsAttachable><CreateDate>{}</CreateDate><UpdateDate>{}</UpdateDate>",
        escape(&policy.name),
        escape(&policy.policy_id),
        escape(&policy_arn(account, &policy.name)),
        time(policy.created_ms),
        time(policy.created_ms)
    )
}

/// A policy document as IAM returns it: URL-encoded.
fn encoded(document: &[u8]) -> String {
    percent_encoding::percent_encode(document, percent_encoding::NON_ALPHANUMERIC).to_string()
}

/// IAM's policy name rule: 1 to 128 letters, digits and `+=,.@_-`.
fn check_policy_name(name: &str) -> Result<(), IamError> {
    if !(1..=128).contains(&name.len())
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+=,.@_-".contains(&b))
    {
        return Err(IamError::validation(
            "A policy name is 1 to 128 letters, digits or +=,.@_- characters.",
        ));
    }
    Ok(())
}

fn parse_policy(document: &str, kind: PolicyKind) -> Result<(), IamError> {
    Policy::parse(document, kind)
        .map(|_| ())
        .map_err(|err| IamError::new(400, "MalformedPolicyDocument", err.0))
}

async fn find_user(
    state: &S3State,
    account: &str,
    name: &str,
) -> Result<truthdb_object::UserInfo, IamError> {
    let (account, wanted) = (account.to_string(), name.to_string());
    object(state, move |file| file.users(&account))
        .await?
        .into_iter()
        .find(|user| user.name == wanted)
        .ok_or_else(|| {
            IamError::no_such_entity(format!("The user with name {name} cannot be found."))
        })
}

async fn dispatch(
    state: &S3State,
    principal: &Principal,
    authz: &crate::authz::Authz,
    action: &str,
    input: &Input,
) -> Result<String, IamError> {
    let account = principal.account.clone();
    let check = |resource: &str| -> Result<(), IamError> {
        Ok(authz.check_arn(&format!("iam:{action}"), resource, &account)?)
    };
    let now = crate::objects::now_ms();
    match action {
        "CreateUser" => {
            let name = input.user()?;
            if input.get("Path").is_some_and(|path| path != "/") {
                return Err(IamError::new(
                    400,
                    "InvalidInput",
                    "Only the path / is supported.",
                ));
            }
            check(&user_arn(&account, &name))?;
            let user_id = fresh_id("AIDA")?;
            let (acct, user, id) = (account.clone(), name.clone(), user_id.clone());
            object(state, move |file| {
                file.transaction(|tx| tx.create_user(&acct, &user, &id, now))
            })
            .await?
            .map_err(account_error)?;
            let user = find_user(state, &account, &name).await?;
            Ok(format!(
                "<CreateUserResult><User>{}</User></CreateUserResult>",
                user_xml(&account, &user)
            ))
        }
        "GetUser" => match subject(input, principal)? {
            Some(name) => {
                check(&user_arn(&account, &name))?;
                let user = find_user(state, &account, &name).await?;
                Ok(format!(
                    "<GetUserResult><User>{}</User></GetUserResult>",
                    user_xml(&account, &user)
                ))
            }
            None => Ok(format!(
                "<GetUserResult><User><UserId>{account}</UserId><Arn>arn:aws:iam::{account}:root</Arn><CreateDate>{}</CreateDate></User></GetUserResult>",
                time(0)
            )),
        },
        "ListUsers" => {
            check(&format!("arn:aws:iam::{account}:user/*"))?;
            let acct = account.clone();
            let users = object(state, move |file| file.users(&acct)).await?;
            let members: String = users
                .iter()
                .map(|user| format!("<member>{}</member>", user_xml(&account, user)))
                .collect();
            Ok(format!(
                "<ListUsersResult><Users>{members}</Users><IsTruncated>false</IsTruncated></ListUsersResult>"
            ))
        }
        "DeleteUser" => {
            let name = input.user()?;
            check(&user_arn(&account, &name))?;
            let acct = account.clone();
            object(state, move |file| {
                file.transaction(|tx| tx.drop_user(&acct, &name))
            })
            .await?
            .map_err(account_error)?;
            Ok(String::new())
        }
        "CreateAccessKey" => {
            let user = subject(input, principal)?;
            check(&key_resource(&account, user.as_deref()))?;
            let id = fresh_id("TDB")?;
            let secret = random(40, SECRET_ALPHABET);
            if secret.is_empty() {
                return Err(IamError::new(
                    500,
                    "ServiceFailure",
                    "The system random generator failed.",
                ));
            }
            let (acct, key_user, key_id, key_secret) =
                (account.clone(), user.clone(), id.clone(), secret.clone());
            object(state, move |file| {
                file.transaction(|tx| {
                    tx.create_access_key(&key_id, &key_secret, &acct, key_user.as_deref(), now)
                })
            })
            .await?
            .map_err(account_error)?;
            Ok(format!(
                "<CreateAccessKeyResult><AccessKey>{}<AccessKeyId>{id}</AccessKeyId><Status>Active</Status><SecretAccessKey>{}</SecretAccessKey><CreateDate>{}</CreateDate></AccessKey></CreateAccessKeyResult>",
                user.as_deref()
                    .map(|name| format!("<UserName>{}</UserName>", escape(name)))
                    .unwrap_or_default(),
                escape(&secret),
                time(now)
            ))
        }
        "ListAccessKeys" => {
            let user = subject(input, principal)?;
            check(&key_resource(&account, user.as_deref()))?;
            if let Some(name) = &user {
                find_user(state, &account, name).await?;
            }
            let keys = object(state, |file| file.access_keys()).await?;
            let members: String = keys
                .iter()
                .filter(|key| key.account == account && key.user == user)
                .map(|key| {
                    format!(
                        "<member>{}<AccessKeyId>{}</AccessKeyId><Status>{}</Status><CreateDate>{}</CreateDate></member>",
                        key.user
                            .as_deref()
                            .map(|name| format!("<UserName>{}</UserName>", escape(name)))
                            .unwrap_or_default(),
                        escape(&key.id),
                        if key.active { "Active" } else { "Inactive" },
                        time(key.created_ms)
                    )
                })
                .collect();
            Ok(format!(
                "<ListAccessKeysResult><AccessKeyMetadata>{members}</AccessKeyMetadata><IsTruncated>false</IsTruncated></ListAccessKeysResult>"
            ))
        }
        "UpdateAccessKey" | "DeleteAccessKey" => {
            let user = subject(input, principal)?;
            check(&key_resource(&account, user.as_deref()))?;
            let id = input.required("AccessKeyId")?.to_string();
            let active = match (action, input.get("Status")) {
                ("DeleteAccessKey", _) => None,
                (_, Some("Active")) => Some(true),
                (_, Some("Inactive")) => Some(false),
                _ => return Err(IamError::validation("Status must be Active or Inactive.")),
            };
            // The key must be the named user's (or the root's) of this account.
            let (acct, key_id) = (account.clone(), id.clone());
            let changed = object(state, move |file| {
                let owned = file
                    .access_key(&key_id)?
                    .is_some_and(|(key, _)| key.account == acct && key.user == user);
                if !owned {
                    return Ok(false);
                }
                file.transaction(|tx| match active {
                    Some(active) => tx.set_access_key_status(&key_id, active),
                    None => tx.drop_access_key(&key_id).map(|()| true),
                })
            })
            .await?;
            if !changed {
                return Err(IamError::no_such_entity(format!(
                    "The Access Key with id {id} cannot be found."
                )));
            }
            Ok(String::new())
        }
        "PutUserPolicy" => {
            let name = input.user()?;
            let policy = input.policy()?;
            let document = input.required("PolicyDocument")?.to_string();
            check(&user_arn(&account, &name))?;
            parse_policy(&document, PolicyKind::UserInline)?;
            let acct = account.clone();
            object(state, move |file| {
                file.transaction(|tx| {
                    if !tx.user_exists(&acct, &name)? {
                        return Ok(Err(AccountError::NoSuchUser));
                    }
                    tx.put_user_policy(&acct, &name, &policy, document.as_bytes())
                        .map(Ok)
                })
            })
            .await?
            .map_err(account_error)?;
            Ok(String::new())
        }
        "GetUserPolicy" => {
            let name = input.user()?;
            let policy = input.policy()?;
            check(&user_arn(&account, &name))?;
            let (acct, user) = (account.clone(), name.clone());
            let document = object(state, move |file| file.user_policies(&acct, &user))
                .await?
                .into_iter()
                .find(|(found, _)| *found == policy)
                .map(|(_, document)| document)
                .ok_or_else(|| {
                    IamError::no_such_entity(format!(
                        "The user policy with name {policy} cannot be found."
                    ))
                })?;
            Ok(format!(
                "<GetUserPolicyResult><UserName>{}</UserName><PolicyName>{}</PolicyName><PolicyDocument>{}</PolicyDocument></GetUserPolicyResult>",
                escape(&name),
                escape(&policy),
                encoded(&document)
            ))
        }
        "ListUserPolicies" => {
            let name = input.user()?;
            check(&user_arn(&account, &name))?;
            find_user(state, &account, &name).await?;
            let acct = account.clone();
            let members: String = object(state, move |file| file.user_policies(&acct, &name))
                .await?
                .iter()
                .map(|(policy, _)| format!("<member>{}</member>", escape(policy)))
                .collect();
            Ok(format!(
                "<ListUserPoliciesResult><PolicyNames>{members}</PolicyNames><IsTruncated>false</IsTruncated></ListUserPoliciesResult>"
            ))
        }
        "DeleteUserPolicy" => {
            let name = input.user()?;
            let policy = input.policy()?;
            check(&user_arn(&account, &name))?;
            let acct = account.clone();
            let policy_name = policy.clone();
            if !object(state, move |file| {
                file.transaction(|tx| tx.delete_user_policy(&acct, &name, &policy_name))
            })
            .await?
            {
                return Err(IamError::no_such_entity(format!(
                    "The user policy with name {policy} cannot be found."
                )));
            }
            Ok(String::new())
        }
        "CreatePolicy" => {
            let name = input.policy()?;
            let document = input.required("PolicyDocument")?.to_string();
            if input.get("Path").is_some_and(|path| path != "/") {
                return Err(IamError::new(
                    400,
                    "InvalidInput",
                    "Only the path / is supported.",
                ));
            }
            check(&policy_arn(&account, &name))?;
            parse_policy(&document, PolicyKind::Managed)?;
            let policy_id = fresh_id("ANPA")?;
            let (acct, policy) = (account.clone(), name.clone());
            object(state, move |file| {
                file.transaction(|tx| {
                    tx.create_managed_policy(&acct, &policy, &policy_id, document.as_bytes(), now)
                })
            })
            .await?
            .map_err(account_error)?;
            let policy = managed(state, &account, &name).await?;
            Ok(format!(
                "<CreatePolicyResult><Policy>{}</Policy></CreatePolicyResult>",
                policy_xml(&account, &policy, 0)
            ))
        }
        "GetPolicy" | "GetPolicyVersion" | "DeletePolicy" => {
            let name = policy_name(&account, input.required("PolicyArn")?)?.to_string();
            check(&policy_arn(&account, &name))?;
            match action {
                "GetPolicy" => {
                    let acct = account.clone();
                    let (policy, attachments) =
                        object(state, move |file| file.managed_policies(&acct))
                            .await?
                            .into_iter()
                            .find(|(policy, _)| policy.name == name)
                            .ok_or_else(|| {
                                IamError::no_such_entity(format!("Policy {name} does not exist."))
                            })?;
                    Ok(format!(
                        "<GetPolicyResult><Policy>{}</Policy></GetPolicyResult>",
                        policy_xml(&account, &policy, attachments)
                    ))
                }
                "GetPolicyVersion" => {
                    if input.required("VersionId")? != "v1" {
                        return Err(IamError::no_such_entity(
                            "The policy version does not exist.",
                        ));
                    }
                    let policy = managed(state, &account, &name).await?;
                    Ok(format!(
                        "<GetPolicyVersionResult><PolicyVersion><Document>{}</Document><VersionId>v1</VersionId><IsDefaultVersion>true</IsDefaultVersion><CreateDate>{}</CreateDate></PolicyVersion></GetPolicyVersionResult>",
                        encoded(&policy.document),
                        time(policy.created_ms)
                    ))
                }
                _ => {
                    let acct = account.clone();
                    object(state, move |file| {
                        file.transaction(|tx| tx.delete_managed_policy(&acct, &name))
                    })
                    .await?
                    .map_err(account_error)?;
                    Ok(String::new())
                }
            }
        }
        "ListPolicies" => {
            check(&format!("arn:aws:iam::{account}:policy/*"))?;
            // AWS's managed policies are none of this server's.
            let members: String = if input.get("Scope") == Some("AWS") {
                String::new()
            } else {
                let acct = account.clone();
                object(state, move |file| file.managed_policies(&acct))
                    .await?
                    .iter()
                    .filter(|(_, attachments)| {
                        input.get("OnlyAttached") != Some("true") || *attachments > 0
                    })
                    .map(|(policy, attachments)| {
                        format!(
                            "<member>{}</member>",
                            policy_xml(&account, policy, *attachments)
                        )
                    })
                    .collect()
            };
            Ok(format!(
                "<ListPoliciesResult><Policies>{members}</Policies><IsTruncated>false</IsTruncated></ListPoliciesResult>"
            ))
        }
        "AttachUserPolicy" | "DetachUserPolicy" => {
            let user = input.user()?;
            let name = policy_name(&account, input.required("PolicyArn")?)?.to_string();
            check(&user_arn(&account, &user))?;
            let acct = account.clone();
            let attach = action == "AttachUserPolicy";
            let outcome = object(state, move |file| {
                file.transaction(|tx| {
                    if attach {
                        tx.attach_user_policy(&acct, &user, &name)
                    } else {
                        tx.detach_user_policy(&acct, &user, &name).map(|detached| {
                            if detached {
                                Ok(())
                            } else {
                                Err(AccountError::NoSuchPolicy)
                            }
                        })
                    }
                })
            })
            .await?;
            outcome.map_err(account_error)?;
            Ok(String::new())
        }
        "ListAttachedUserPolicies" => {
            let user = input.user()?;
            check(&user_arn(&account, &user))?;
            find_user(state, &account, &user).await?;
            let acct = account.clone();
            let members: String = object(state, move |file| file.attached_policies(&acct, &user))
                .await?
                .iter()
                .map(|name| {
                    format!(
                        "<member><PolicyName>{}</PolicyName><PolicyArn>{}</PolicyArn></member>",
                        escape(name),
                        escape(&policy_arn(&account, name))
                    )
                })
                .collect();
            Ok(format!(
                "<ListAttachedUserPoliciesResult><AttachedPolicies>{members}</AttachedPolicies><IsTruncated>false</IsTruncated></ListAttachedUserPoliciesResult>"
            ))
        }
        other => Err(IamError::new(
            400,
            "InvalidAction",
            format!("{other} is not supported."),
        )),
    }
}

/// The resource of an access key action: the user's ARN, or the root's.
fn key_resource(account: &str, user: Option<&str>) -> String {
    match user {
        Some(name) => user_arn(account, name),
        None => format!("arn:aws:iam::{account}:root"),
    }
}

async fn managed(
    state: &S3State,
    account: &str,
    name: &str,
) -> Result<truthdb_object::ManagedPolicy, IamError> {
    let (acct, policy) = (account.to_string(), name.to_string());
    object(state, move |file| file.managed_policy(&acct, &policy))
        .await?
        .ok_or_else(|| IamError::no_such_entity(format!("Policy {name} does not exist.")))
}
