//! S3 access policies: the IAM policy language (a bucket's policy, and the
//! identity policies of an account's users) and AWS's evaluation of it.
//!
//! Supported: `Version` 2012-10-17 (policy variables) and 2008-10-17;
//! statements with `Effect`, `Principal` (bucket policies only: `*`, or
//! `{"AWS": ...}` naming accounts and users), `Action`/`NotAction`,
//! `Resource`/`NotResource` and `Condition` (the String, Numeric, Date,
//! Bool, Binary, IpAddress, Arn and Null operators, `...IfExists`, and the
//! `ForAnyValue:`/`ForAllValues:` set operators). `NotPrincipal`, and
//! principals other than accounts and users, are refused as malformed.

mod cond;
mod eval;
mod policy;

#[cfg(test)]
mod tests;

use std::collections::HashMap;

pub use eval::{Decision, evaluate, is_public};
pub use policy::{MalformedPolicy, Policy, PolicyKind};

/// Who makes a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Identity {
    Anonymous,
    /// An account's root.
    Root {
        account: String,
    },
    /// One of an account's users.
    User {
        account: String,
        name: String,
        /// The user's unique id (`aws:userid`).
        user_id: String,
    },
}

impl Identity {
    pub fn account(&self) -> Option<&str> {
        match self {
            Identity::Anonymous => None,
            Identity::Root { account } | Identity::User { account, .. } => Some(account),
        }
    }

    /// The identity's ARN (`aws:PrincipalArn`); none for an anonymous caller.
    pub fn arn(&self) -> Option<String> {
        match self {
            Identity::Anonymous => None,
            Identity::Root { account } => Some(format!("arn:aws:iam::{account}:root")),
            Identity::User { account, name, .. } => {
                Some(format!("arn:aws:iam::{account}:user/{name}"))
            }
        }
    }
}

/// A request's condition keys and their values (a key may have several).
/// Key names compare without case, as in AWS.
#[derive(Debug, Clone, Default)]
pub struct Context {
    values: HashMap<String, Vec<String>>,
}

impl Context {
    pub fn set(&mut self, key: &str, values: Vec<String>) {
        self.values.insert(key.to_ascii_lowercase(), values);
    }

    pub fn get(&self, key: &str) -> Option<&[String]> {
        self.values
            .get(&key.to_ascii_lowercase())
            .map(Vec::as_slice)
    }
}

/// What a request asks for: an action (`s3:GetObject`) on a resource ARN, by
/// an identity, with its condition context.
pub struct Request<'a> {
    pub identity: &'a Identity,
    pub action: &'a str,
    pub resource: &'a str,
    pub context: &'a Context,
}

impl Request<'_> {
    /// The values of condition key `key`: the identity's own keys
    /// (`aws:username`, `aws:userid`, `aws:PrincipalType`,
    /// `aws:PrincipalAccount`, `aws:PrincipalArn`), then the context's.
    fn values(&self, key: &str) -> Option<Vec<String>> {
        let identity = match key.to_ascii_lowercase().as_str() {
            "aws:username" => match self.identity {
                Identity::User { name, .. } => Some(name.clone()),
                _ => None,
            },
            "aws:userid" => Some(match self.identity {
                Identity::Anonymous => "anonymous".to_string(),
                Identity::Root { account } => account.clone(),
                Identity::User { user_id, .. } => user_id.clone(),
            }),
            "aws:principaltype" => Some(
                match self.identity {
                    Identity::Anonymous => "Anonymous",
                    Identity::Root { .. } => "Account",
                    Identity::User { .. } => "User",
                }
                .to_string(),
            ),
            "aws:principalaccount" => self.identity.account().map(str::to_string),
            "aws:principalarn" => self.identity.arn(),
            _ => return self.context.get(key).map(<[String]>::to_vec),
        };
        identity.map(|value| vec![value])
    }

    /// A single-valued key's value, for a policy variable.
    fn value(&self, key: &str) -> Option<String> {
        match self.values(key)?.as_slice() {
            [value] => Some(value.clone()),
            _ => None,
        }
    }
}
