//! AWS's policy evaluation for a request against a bucket, and whether a
//! bucket policy is public.

use super::cond::{arn_like, expand, glob};
use super::policy::{Effect, Policy, Principal, Statement};
use super::{Identity, Request};

/// The outcome of evaluating a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    /// Nothing allows the request.
    ImplicitDeny,
    /// A statement denies the request: nothing else can allow it.
    ExplicitDeny,
}

/// Whether `statement` (of `policy`) covers the request: its action, its
/// resource, and every condition.
fn applies(policy: &Policy, statement: &Statement, request: &Request<'_>) -> bool {
    let action = statement.actions.values.iter().any(|pattern| {
        expand(pattern, true, false, request)
            .is_some_and(|tokens| glob(&tokens, request.action, true))
    });
    if action == statement.actions.negated {
        return false;
    }
    let resource = statement.resources.values.iter().any(|pattern| {
        arn_like(pattern, request.resource, policy.variables, true, request).unwrap_or(false)
    });
    if resource == statement.resources.negated {
        return false;
    }
    statement
        .conditions
        .iter()
        .all(|condition| condition.holds(request, policy.variables))
}

/// How a bucket policy statement's principals name the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Named {
    No,
    /// By name: anyone (`*`), the caller's own ARN, or (for an account's
    /// root) its account.
    Directly,
    /// Through its account: a grant a user's own policies must also allow.
    ByAccount,
}

fn named(principals: &[Principal], identity: &Identity) -> Named {
    let mut out = Named::No;
    for principal in principals {
        let this = match (principal, identity) {
            (Principal::Anyone, _) => Named::Directly,
            (Principal::Account(account), Identity::Root { account: own }) if account == own => {
                Named::Directly
            }
            (Principal::Account(account), Identity::User { account: own, .. })
                if account == own =>
            {
                Named::ByAccount
            }
            (
                Principal::User { account, name },
                Identity::User {
                    account: own,
                    name: own_name,
                    ..
                },
            ) if account == own && name == own_name => Named::Directly,
            _ => Named::No,
        };
        if this == Named::Directly {
            return Named::Directly;
        }
        if this == Named::ByAccount {
            out = Named::ByAccount;
        }
    }
    out
}

/// Evaluates `request` as AWS does, against the caller's identity policies
/// (a user's; an account's root and an anonymous caller have none) and the
/// resource's bucket (its owning account and policy; `None` for a request
/// on no bucket, such as ListBuckets):
///
/// - a Deny statement that applies wins over everything;
/// - an account's root is allowed in its own account;
/// - within the bucket owner's account, a user is allowed by its own
///   policies, or by a bucket policy naming it (or anyone);
/// - from another account, the bucket policy must name the caller (or its
///   account, or anyone) and the caller's own policies must allow it too (a
///   root's do);
/// - an anonymous caller is allowed only by a bucket policy naming anyone.
pub fn evaluate(
    request: &Request<'_>,
    identity_policies: &[&Policy],
    bucket_owner: Option<&str>,
    bucket_policy: Option<&Policy>,
) -> Decision {
    let mut identity_allow = matches!(request.identity, Identity::Root { .. });
    for policy in identity_policies {
        for statement in &policy.statements {
            if !applies(policy, statement, request) {
                continue;
            }
            match statement.effect {
                Effect::Deny => return Decision::ExplicitDeny,
                Effect::Allow => identity_allow = true,
            }
        }
    }
    let (mut direct, mut by_account) = (false, false);
    if let Some(policy) = bucket_policy {
        for statement in &policy.statements {
            let named = named(&statement.principals, request.identity);
            if named == Named::No || !applies(policy, statement, request) {
                continue;
            }
            match statement.effect {
                Effect::Deny => return Decision::ExplicitDeny,
                Effect::Allow if named == Named::Directly => direct = true,
                Effect::Allow => by_account = true,
            }
        }
    }
    let allowed = match (bucket_owner, request.identity) {
        (_, Identity::Anonymous) => direct,
        (None, _) => identity_allow,
        (Some(owner), Identity::Root { account }) if owner == account => true,
        (Some(owner), Identity::User { account, .. }) if owner == account => {
            identity_allow || direct
        }
        (Some(_), _) => identity_allow && (direct || by_account),
    };
    if allowed {
        Decision::Allow
    } else {
        Decision::ImplicitDeny
    }
}

/// The condition keys that, pinned to fixed values, keep a statement naming
/// anyone from being public (S3's rule for Block Public Access).
const PINNING_KEYS: &[&str] = &[
    "aws:SourceIp",
    "aws:SourceArn",
    "aws:SourceVpc",
    "aws:SourceVpce",
    "aws:SourceOwner",
    "aws:SourceAccount",
    "aws:userid",
    "aws:PrincipalOrgID",
    "aws:PrincipalAccount",
    "aws:PrincipalArn",
    "s3:DataAccessPointArn",
    "s3:DataAccessPointAccount",
];

/// Whether a statement grants to the public: it allows anyone, without a
/// condition pinning one of [`PINNING_KEYS`] to fixed values.
fn public_grant(statement: &Statement) -> bool {
    statement.effect == Effect::Allow
        && statement.principals.contains(&Principal::Anyone)
        && !statement
            .conditions
            .iter()
            .any(|condition| PINNING_KEYS.iter().any(|key| condition.pins(key)))
}

/// Whether a bucket policy is public: some statement grants to the public.
pub fn is_public(policy: &Policy) -> bool {
    policy.statements.iter().any(public_grant)
}

/// The policy without its public grants (what still applies to the bucket
/// owner's account under Block Public Access's RestrictPublicBuckets): a
/// public statement keeps the principals it names, without anyone.
pub fn without_public_grants(policy: &Policy) -> Policy {
    Policy {
        variables: policy.variables,
        statements: policy
            .statements
            .iter()
            .filter_map(|statement| {
                if !public_grant(statement) {
                    return Some(statement.clone());
                }
                let mut named = statement.clone();
                named
                    .principals
                    .retain(|principal| *principal != Principal::Anyone);
                (!named.principals.is_empty()).then_some(named)
            })
            .collect(),
    }
}
