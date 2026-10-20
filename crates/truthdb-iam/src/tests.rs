use super::cond::{Tok, date_ms, expand, glob};
use super::*;

const OWNER: &str = "111111111111";
const OTHER: &str = "222222222222";

fn bucket_policy(json: &str) -> Policy {
    Policy::parse(json, PolicyKind::Bucket).unwrap()
}

fn identity_policy(json: &str) -> Policy {
    Policy::parse(json, PolicyKind::Managed).unwrap()
}

fn root(account: &str) -> Identity {
    Identity::Root {
        account: account.to_string(),
    }
}

fn user(account: &str, name: &str) -> Identity {
    Identity::User {
        account: account.to_string(),
        name: name.to_string(),
        user_id: format!("AIDA{}", name.to_uppercase()),
    }
}

/// Evaluates `action` on `resource` with `context` pairs.
fn decide(
    identity: &Identity,
    action: &str,
    resource: &str,
    context: &[(&str, &str)],
    identity_policies: &[&Policy],
    bucket: Option<&Policy>,
) -> Decision {
    let mut ctx = Context::default();
    for (key, value) in context {
        ctx.set(key, vec![value.to_string()]);
    }
    let request = Request {
        identity,
        action,
        resource,
        context: &ctx,
    };
    evaluate(&request, identity_policies, Some(OWNER), bucket)
}

#[test]
fn a_home_folder_policy_scopes_each_user_to_its_prefix() {
    // AWS's documented home-folder policy.
    let policy = identity_policy(
        r#"{
          "Version": "2012-10-17",
          "Statement": [
            {"Effect": "Allow", "Action": ["s3:ListAllMyBuckets", "s3:GetBucketLocation"],
             "Resource": "arn:aws:s3:::*"},
            {"Effect": "Allow", "Action": "s3:ListBucket",
             "Resource": "arn:aws:s3:::docs",
             "Condition": {"StringLike": {"s3:prefix": ["", "home/", "home/${aws:username}/"]}}},
            {"Effect": "Allow", "Action": "s3:*",
             "Resource": ["arn:aws:s3:::docs/home/${aws:username}",
                          "arn:aws:s3:::docs/home/${aws:username}/*"]}
          ]
        }"#,
    );
    let alice = user(OWNER, "alice");
    let p = [&policy];
    let list = |prefix: &str| {
        decide(
            &alice,
            "s3:ListBucket",
            "arn:aws:s3:::docs",
            &[("s3:prefix", prefix)],
            &p,
            None,
        )
    };
    assert_eq!(list("home/alice/"), Decision::Allow);
    assert_eq!(list("home/"), Decision::Allow);
    assert_eq!(list("home/bob/"), Decision::ImplicitDeny);
    let object = |key: &str, action: &str| {
        decide(
            &alice,
            action,
            &format!("arn:aws:s3:::docs/{key}"),
            &[],
            &p,
            None,
        )
    };
    assert_eq!(
        object("home/alice/notes.txt", "s3:GetObject"),
        Decision::Allow
    );
    assert_eq!(
        object("home/alice/a/b", "S3:PutObject"),
        Decision::Allow,
        "actions ignore case"
    );
    assert_eq!(
        object("home/bob/notes.txt", "s3:GetObject"),
        Decision::ImplicitDeny
    );
    // The root has no user name: the variable matches nothing for it, but a
    // root is allowed in its own account anyway.
    assert_eq!(
        decide(
            &root(OWNER),
            "s3:GetObject",
            "arn:aws:s3:::docs/home/x",
            &[],
            &[],
            None
        ),
        Decision::Allow
    );
}

#[test]
fn an_explicit_deny_wins_even_over_the_bucket_owner() {
    // AWS's documented deny of insecure transport.
    let policy = bucket_policy(
        r#"{"Version": "2012-10-17", "Statement": [
            {"Effect": "Deny", "Principal": "*", "Action": "s3:*",
             "Resource": ["arn:aws:s3:::docs", "arn:aws:s3:::docs/*"],
             "Condition": {"Bool": {"aws:SecureTransport": "false"}}},
            {"Effect": "Allow", "Principal": {"AWS": "222222222222"}, "Action": "s3:GetObject",
             "Resource": "arn:aws:s3:::docs/*"}
        ]}"#,
    );
    let get = |identity: &Identity, tls: &str| {
        decide(
            identity,
            "s3:GetObject",
            "arn:aws:s3:::docs/k",
            &[("aws:SecureTransport", tls)],
            &[],
            Some(&policy),
        )
    };
    assert_eq!(get(&root(OWNER), "false"), Decision::ExplicitDeny);
    assert_eq!(get(&root(OWNER), "true"), Decision::Allow);
    assert_eq!(get(&root(OTHER), "true"), Decision::Allow);
    assert_eq!(get(&root(OTHER), "false"), Decision::ExplicitDeny);
    // A missing key: Bool does not match, so the deny does not apply.
    assert_eq!(
        decide(
            &root(OTHER),
            "s3:GetObject",
            "arn:aws:s3:::docs/k",
            &[],
            &[],
            Some(&policy)
        ),
        Decision::Allow
    );
}

#[test]
fn cross_account_access_needs_both_the_bucket_and_the_identity_policies() {
    let to_account = bucket_policy(
        r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::222222222222:root"},
            "Action": "s3:GetObject", "Resource": "arn:aws:s3:::docs/*"}}"#,
    );
    let to_user = bucket_policy(
        r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow",
            "Principal": {"AWS": ["arn:aws:iam::222222222222:user/bob"]},
            "Action": "s3:GetObject", "Resource": "arn:aws:s3:::docs/*"}}"#,
    );
    let own = identity_policy(
        r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow",
            "Action": "s3:GetObject", "Resource": "*"}}"#,
    );
    let bob = user(OTHER, "bob");
    let get = |identity: &Identity, policies: &[&Policy], bucket: &Policy| {
        decide(
            identity,
            "s3:GetObject",
            "arn:aws:s3:::docs/k",
            &[],
            policies,
            Some(bucket),
        )
    };
    assert_eq!(get(&root(OTHER), &[], &to_account), Decision::Allow);
    assert_eq!(get(&bob, &[], &to_account), Decision::ImplicitDeny);
    assert_eq!(get(&bob, &[&own], &to_account), Decision::Allow);
    assert_eq!(
        get(&bob, &[], &to_user),
        Decision::ImplicitDeny,
        "named, but not allowed at home"
    );
    assert_eq!(get(&bob, &[&own], &to_user), Decision::Allow);
    // Not named at all: the caller's own policy is not enough.
    let carol = user("333333333333", "carol");
    assert_eq!(get(&carol, &[&own], &to_account), Decision::ImplicitDeny);
    // And not for another action.
    assert_eq!(
        decide(
            &bob,
            "s3:PutObject",
            "arn:aws:s3:::docs/k",
            &[],
            &[&own],
            Some(&to_user)
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn within_the_owner_account_either_policy_allows_a_user() {
    let names_user = bucket_policy(
        r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::111111111111:user/alice"},
            "Action": "s3:GetObject", "Resource": "arn:aws:s3:::docs/*"}}"#,
    );
    let names_account = bucket_policy(
        r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow",
            "Principal": {"AWS": "111111111111"},
            "Action": "s3:GetObject", "Resource": "arn:aws:s3:::docs/*"}}"#,
    );
    let alice = user(OWNER, "alice");
    let get = |bucket: Option<&Policy>| {
        decide(
            &alice,
            "s3:GetObject",
            "arn:aws:s3:::docs/k",
            &[],
            &[],
            bucket,
        )
    };
    assert_eq!(get(None), Decision::ImplicitDeny);
    assert_eq!(get(Some(&names_user)), Decision::Allow);
    assert_eq!(
        get(Some(&names_account)),
        Decision::ImplicitDeny,
        "a grant to the account delegates to the account's own policies"
    );
    assert_eq!(get(None), Decision::ImplicitDeny);
}

#[test]
fn an_anonymous_caller_is_allowed_only_by_a_policy_naming_anyone() {
    let public_read = bucket_policy(
        r#"{"Version": "2012-10-17", "Statement": {"Effect": "Allow", "Principal": "*",
            "Action": "s3:GetObject", "Resource": "arn:aws:s3:::site/public/*",
            "Condition": {"IpAddress": {"aws:SourceIp": ["203.0.113.0/24", "2001:db8::/32"]}}}}"#,
    );
    let to_account = bucket_policy(
        r#"{"Statement": {"Effect": "Allow", "Principal": {"AWS": "111111111111"},
            "Action": "s3:*", "Resource": "*"}}"#,
    );
    let get = |key: &str, ip: &str, bucket: &Policy| {
        decide(
            &Identity::Anonymous,
            "s3:GetObject",
            &format!("arn:aws:s3:::site/{key}"),
            &[("aws:SourceIp", ip)],
            &[],
            Some(bucket),
        )
    };
    assert_eq!(
        get("public/a.html", "203.0.113.7", &public_read),
        Decision::Allow
    );
    assert_eq!(
        get("public/a.html", "2001:db8::1", &public_read),
        Decision::Allow
    );
    assert_eq!(
        get("public/a.html", "198.51.100.1", &public_read),
        Decision::ImplicitDeny
    );
    assert_eq!(
        get("private/a", "203.0.113.7", &public_read),
        Decision::ImplicitDeny
    );
    assert_eq!(
        get("public/a.html", "203.0.113.7", &to_account),
        Decision::ImplicitDeny
    );
}

fn condition(json: &str, context: &[(&str, &[&str])]) -> bool {
    let policy = identity_policy(&format!(
        r#"{{"Version": "2012-10-17", "Statement": {{"Effect": "Allow", "Action": "s3:GetObject",
            "Resource": "*", "Condition": {json}}}}}"#
    ));
    let mut ctx = Context::default();
    for (key, values) in context {
        ctx.set(key, values.iter().map(|v| v.to_string()).collect());
    }
    let alice = user(OWNER, "alice");
    let request = Request {
        identity: &alice,
        action: "s3:GetObject",
        resource: "arn:aws:s3:::b/k",
        context: &ctx,
    };
    evaluate(&request, &[&policy], None, None) == Decision::Allow
}

#[test]
fn conditions_follow_aws_on_missing_keys_sets_and_types() {
    let k = "s3:x-amz-acl";
    // A missing key: positive operators fail, negated ones hold, IfExists
    // holds, Null checks absence.
    assert!(!condition(
        r#"{"StringEquals": {"s3:x-amz-acl": "private"}}"#,
        &[]
    ));
    assert!(condition(
        r#"{"StringNotEquals": {"s3:x-amz-acl": "private"}}"#,
        &[]
    ));
    assert!(condition(
        r#"{"StringEqualsIfExists": {"s3:x-amz-acl": "private"}}"#,
        &[]
    ));
    assert!(condition(r#"{"Null": {"s3:x-amz-acl": "true"}}"#, &[]));
    assert!(!condition(r#"{"Null": {"s3:x-amz-acl": "false"}}"#, &[]));
    assert!(condition(
        r#"{"Null": {"s3:x-amz-acl": "false"}}"#,
        &[(k, &["x"])]
    ));
    // Several policy values: any matches; negated: none may.
    assert!(condition(
        r#"{"StringEquals": {"S3:X-AMZ-ACL": ["a", "private"]}}"#,
        &[(k, &["private"])]
    ));
    assert!(!condition(
        r#"{"StringNotEquals": {"s3:x-amz-acl": ["a", "private"]}}"#,
        &[(k, &["private"])]
    ));
    assert!(!condition(
        r#"{"StringEquals": {"s3:x-amz-acl": "Private"}}"#,
        &[(k, &["private"])]
    ));
    assert!(condition(
        r#"{"StringEqualsIgnoreCase": {"s3:x-amz-acl": "Private"}}"#,
        &[(k, &["private"])]
    ));
    assert!(!condition(
        r#"{"StringEquals": {"s3:x-amz-acl": "pri*"}}"#,
        &[(k, &["private"])]
    ));
    assert!(condition(
        r#"{"StringLike": {"s3:x-amz-acl": "pri?ate*"}}"#,
        &[(k, &["private"])]
    ));
    // Operators are ANDed.
    assert!(!condition(
        r#"{"StringEquals": {"s3:x-amz-acl": "private"}, "NumericLessThan": {"s3:max-keys": "10"}}"#,
        &[(k, &["private"]), ("s3:max-keys", &["20"])]
    ));
    // Set operators.
    let tags = "s3:RequestObjectTagKeys";
    assert!(condition(
        r#"{"ForAllValues:StringEquals": {"s3:RequestObjectTagKeys": ["a", "b"]}}"#,
        &[]
    ));
    assert!(condition(
        r#"{"ForAllValues:StringEquals": {"s3:RequestObjectTagKeys": ["a", "b"]}}"#,
        &[(tags, &["a", "b"])]
    ));
    assert!(!condition(
        r#"{"ForAllValues:StringEquals": {"s3:RequestObjectTagKeys": ["a", "b"]}}"#,
        &[(tags, &["a", "c"])]
    ));
    assert!(!condition(
        r#"{"ForAnyValue:StringEquals": {"s3:RequestObjectTagKeys": ["a"]}}"#,
        &[]
    ));
    assert!(condition(
        r#"{"ForAnyValue:StringEquals": {"s3:RequestObjectTagKeys": ["a"]}}"#,
        &[(tags, &["c", "a"])]
    ));
    assert!(!condition(
        r#"{"ForAllValues:StringNotEquals": {"s3:RequestObjectTagKeys": ["a"]}}"#,
        &[(tags, &["c", "a"])]
    ));
    assert!(condition(
        r#"{"ForAnyValue:StringNotEquals": {"s3:RequestObjectTagKeys": ["a"]}}"#,
        &[(tags, &["c", "a"])]
    ));
    // Numbers, dates, booleans, binary, addresses, ARNs.
    assert!(condition(
        r#"{"NumericLessThanEquals": {"s3:max-keys": 10}}"#,
        &[("s3:max-keys", &["10"])]
    ));
    assert!(!condition(
        r#"{"NumericGreaterThan": {"s3:max-keys": "10"}}"#,
        &[("s3:max-keys", &["10"])]
    ));
    assert!(!condition(
        r#"{"NumericEquals": {"s3:max-keys": "10"}}"#,
        &[("s3:max-keys", &["ten"])]
    ));
    let now = [("aws:CurrentTime", &["2026-10-20T12:00:00Z"][..])];
    assert!(condition(
        r#"{"DateLessThan": {"aws:CurrentTime": "2026-10-21"}}"#,
        &now
    ));
    assert!(condition(
        r#"{"DateGreaterThan": {"aws:CurrentTime": "2026-10-20T13:00:00+02:00"}}"#,
        &now
    ));
    assert!(condition(
        r#"{"DateEquals": {"aws:CurrentTime": 1792497600}}"#,
        &now
    ));
    assert!(condition(
        r#"{"Bool": {"aws:SecureTransport": true}}"#,
        &[("aws:SecureTransport", &["true"])]
    ));
    assert!(condition(
        r#"{"BinaryEquals": {"k": "aGk="}}"#,
        &[("k", &["aGk="])]
    ));
    assert!(condition(
        r#"{"NotIpAddress": {"aws:SourceIp": "10.0.0.0/8"}}"#,
        &[("aws:SourceIp", &["11.0.0.1"])]
    ));
    assert!(condition(
        r#"{"IpAddress": {"aws:SourceIp": "11.0.0.1"}}"#,
        &[("aws:SourceIp", &["11.0.0.1"])]
    ));
    assert!(condition(
        r#"{"ArnLike": {"aws:SourceArn": "arn:aws:s3:::logs-*"}}"#,
        &[("aws:SourceArn", &["arn:aws:s3:::logs-2026"])]
    ));
    assert!(
        !condition(
            r#"{"ArnLike": {"aws:SourceArn": "arn:aws:s3:*"}}"#,
            &[("aws:SourceArn", &["arn:aws:s3:::logs"])]
        ),
        "a wildcard stays within its ARN part"
    );
    // The identity's own keys.
    assert!(condition(
        r#"{"StringEquals": {"aws:PrincipalType": "User", "aws:username": "alice"}}"#,
        &[]
    ));
    assert!(condition(
        r#"{"StringEquals": {"aws:PrincipalArn": "arn:aws:iam::111111111111:user/alice"}}"#,
        &[]
    ));
}

#[test]
fn policy_variables_expand_only_in_version_2012_10_17() {
    let resource = |version: &str, pattern: &str, key: &str| {
        let policy = identity_policy(&format!(
            r#"{{{version} "Statement": {{"Effect": "Allow", "Action": "s3:GetObject",
                "Resource": "{pattern}"}}}}"#
        ));
        decide(
            &user(OWNER, "alice"),
            "s3:GetObject",
            &format!("arn:aws:s3:::b/{key}"),
            &[],
            &[&policy],
            None,
        ) == Decision::Allow
    };
    let v12 = r#""Version": "2012-10-17","#;
    let v08 = r#""Version": "2008-10-17","#;
    assert!(resource(v12, "arn:aws:s3:::b/${aws:username}/*", "alice/x"));
    assert!(!resource(
        v08,
        "arn:aws:s3:::b/${aws:username}/*",
        "alice/x"
    ));
    assert!(resource(
        v08,
        "arn:aws:s3:::b/${aws:username}/*",
        "${aws:username}/x"
    ));
    assert!(
        !resource("", "arn:aws:s3:::b/${aws:username}/*", "alice/x"),
        "no Version is 2008"
    );
    // A literal `*` through `${*}`; a default for a missing key.
    assert!(resource(v12, "arn:aws:s3:::b/star${*}", "star*"));
    assert!(!resource(v12, "arn:aws:s3:::b/star${*}", "starry"));
    assert!(resource(
        v12,
        "arn:aws:s3:::b/${aws:PrincipalTag/team, 'shared'}/*",
        "shared/x"
    ));
    assert!(!resource(
        v12,
        "arn:aws:s3:::b/${aws:PrincipalTag/team}/*",
        "/x"
    ));
    // A variable only counts in the resource part of the ARN.
    assert!(!resource(v12, "arn:aws:s3:::${aws:username}", "x"));
}

#[test]
fn malformed_policies_are_refused() {
    let bucket = |json: &str| Policy::parse(json, PolicyKind::Bucket).map(|_| ());
    let identity = |json: &str| Policy::parse(json, PolicyKind::UserInline).map(|_| ());
    let ok_statement = r#""Effect": "Allow", "Action": "s3:GetObject", "Resource": "*""#;
    assert!(
        bucket(&format!(
            r#"{{"Statement": {{"Principal": "*", {ok_statement}}}}}"#
        ))
        .is_ok()
    );
    for bad in [
        "not json".to_string(),
        "[]".to_string(),
        r#"{"Statement": []}"#.to_string(),
        r#"{"Version": "2020-01-01", "Statement": {}}"#.to_string(),
        format!(r#"{{"Statement": {{{ok_statement}}}}}"#),
        format!(r#"{{"Statement": {{"NotPrincipal": "*", {ok_statement}}}}}"#),
        format!(r#"{{"Statement": {{"Principal": {{"Service": "x"}}, {ok_statement}}}}}"#),
        format!(r#"{{"Statement": {{"Principal": {{"AWS": "arn:aws:iam::1:root"}}, {ok_statement}}}}}"#),
        format!(r#"{{"Statement": {{"Principal": {{"AWS": "arn:aws:iam::111111111111:user/*"}}, {ok_statement}}}}}"#),
        format!(r#"{{"Statement": {{"Principal": "*", {ok_statement}, "Condition": {{"StringSorta": {{"k": "v"}}}}}}}}"#),
        format!(r#"{{"Statement": {{"Principal": "*", {ok_statement}, "Condition": {{"NumericEquals": {{"k": "ten"}}}}}}}}"#),
        format!(r#"{{"Statement": {{"Principal": "*", {ok_statement}, "Condition": {{"IpAddress": {{"k": "1.2.3.4/40"}}}}}}}}"#),
        r#"{"Statement": {"Principal": "*", "Effect": "Allow", "Action": "GetObject", "Resource": "*"}}"#.to_string(),
        r#"{"Statement": {"Principal": "*", "Effect": "Allow", "Action": "s3:GetObject", "Resource": "bucket/*"}}"#.to_string(),
        r#"{"Statement": {"Principal": "*", "Effect": "Allow", "Action": "s3:GetObject", "NotAction": "s3:x", "Resource": "*"}}"#.to_string(),
        r#"{"Statement": {"Principal": "*", "Effect": "Maybe", "Action": "s3:GetObject", "Resource": "*"}}"#.to_string(),
        r#"{"Statement": {"Principal": "*", "Effect": "Allow", "Action": "s3:GetObject", "Resource": "*", "Extra": 1}}"#.to_string(),
    ] {
        assert!(bucket(&bad).is_err(), "{bad}");
    }
    assert!(identity(&format!(r#"{{"Statement": {{{ok_statement}}}}}"#)).is_ok());
    assert!(
        identity(&format!(
            r#"{{"Statement": {{"Principal": "*", {ok_statement}}}}}"#
        ))
        .is_err()
    );
    let big = format!(
        r#"{{"Statement": {{{ok_statement}, "Sid": "{}"}}}}"#,
        "x".repeat(2100)
    );
    assert!(
        identity(&big).is_err(),
        "an inline policy is at most 2,048 characters"
    );
    assert!(Policy::parse(&big, PolicyKind::Managed).is_ok());
}

#[test]
fn a_policy_is_public_unless_its_grants_to_anyone_are_pinned() {
    let public = |statement: &str| {
        is_public(&bucket_policy(&format!(
            r#"{{"Statement": {{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*", {statement}}}}}"#
        )))
    };
    assert!(public(r#""Principal": "*""#));
    assert!(public(r#""Principal": {"AWS": "*"}"#));
    assert!(!public(r#""Principal": {"AWS": "222222222222"}"#));
    assert!(public(
        r#""Principal": "*", "Condition": {"StringLike": {"aws:SourceVpc": "vpc-*"}}"#
    ));
    assert!(!public(
        r#""Principal": "*", "Condition": {"StringEquals": {"aws:SourceVpc": "vpc-91237329"}}"#
    ));
    assert!(public(
        r#""Principal": "*", "Condition": {"StringEqualsIfExists": {"aws:SourceVpc": "vpc-1"}}"#
    ));
    assert!(public(
        r#""Principal": "*", "Condition": {"StringEquals": {"aws:SourceAccount": "${aws:username}"}}"#
    ));
    assert!(public(
        r#""Principal": "*", "Condition": {"IpAddress": {"aws:SourceIp": "0.0.0.0/1"}}"#
    ));
    assert!(!public(
        r#""Principal": "*", "Condition": {"IpAddress": {"aws:SourceIp": "203.0.113.0/24"}}"#
    ));
    assert!(public(
        r#""Principal": "*", "Condition": {"IpAddress": {"aws:SourceIp": "2001:db8::/16"}}"#
    ));
    assert!(public(
        r#""Principal": "*", "Condition": {"StringEquals": {"s3:prefix": "x"}}"#
    ));
    // A deny to anyone is not a grant.
    assert!(!is_public(&bucket_policy(
        r#"{"Statement": {"Effect": "Deny", "Principal": "*", "Action": "s3:*", "Resource": "*"}}"#
    )));
}

/// A plain recursive matcher, the reference for [`glob`].
fn reference(pattern: &[char], text: &[char]) -> bool {
    match pattern.split_first() {
        None => text.is_empty(),
        Some(('*', rest)) => (0..=text.len()).any(|i| reference(rest, &text[i..])),
        Some(('?', rest)) => !text.is_empty() && reference(rest, &text[1..]),
        Some((c, rest)) => text.first() == Some(c) && reference(rest, &text[1..]),
    }
}

#[test]
fn wildcard_matching_agrees_with_a_reference_matcher() {
    // Every pattern over {a, b, *, ?} up to 5 long, against every text over
    // {a, b} up to 5 long.
    let words = |alphabet: &[char], max: usize| {
        let mut out = vec![String::new()];
        let mut layer = vec![String::new()];
        for _ in 0..max {
            layer = layer
                .iter()
                .flat_map(|w| alphabet.iter().map(move |c| format!("{w}{c}")))
                .collect();
            out.extend(layer.iter().cloned());
        }
        out
    };
    let ctx = Context::default();
    let identity = Identity::Anonymous;
    let request = Request {
        identity: &identity,
        action: "",
        resource: "",
        context: &ctx,
    };
    for pattern in words(&['a', 'b', '*', '?'], 5) {
        let tokens: Vec<Tok> = expand(&pattern, true, false, &request).unwrap();
        let p: Vec<char> = pattern.chars().collect();
        for text in words(&['a', 'b'], 5) {
            let t: Vec<char> = text.chars().collect();
            assert_eq!(
                glob(&tokens, &text, false),
                reference(&p, &t),
                "{pattern} {text}"
            );
        }
    }
}

#[test]
fn dates_parse_in_the_w3c_forms_and_epoch_seconds() {
    assert_eq!(date_ms("0"), Some(0));
    assert_eq!(date_ms("1970-01-02"), Some(86_400_000));
    assert_eq!(date_ms("2026-10-20T12:00:00Z"), Some(1_792_497_600_000));
    assert_eq!(
        date_ms("2026-10-20T14:00:00+02:00"),
        Some(1_792_497_600_000)
    );
    assert_eq!(date_ms("2026-10-20T12:00Z"), Some(1_792_497_600_000));
    assert_eq!(date_ms("2026-10-20T12:00:00.25Z"), Some(1_792_497_600_250));
    // A bare number is epoch seconds, not a year.
    assert_eq!(date_ms("2026"), Some(2_026_000));
    assert_eq!(date_ms("2026-10"), date_ms("2026-10-01"));
    for bad in [
        "",
        "2026-13-01",
        "2026-10-20T25:00Z",
        "2026-1020",
        "2026-10-20T12:00",
    ] {
        assert_eq!(date_ms(bad), None, "{bad}");
    }
}

#[test]
fn review_findings_hold_fixed() {
    let ok = r#""Effect": "Allow", "Action": "s3:GetObject", "Resource": "*""#;
    let bucket = |json: &str| Policy::parse(json, PolicyKind::Bucket);
    // Out-of-range dates are refused, not wrapped (a far-future deny stays
    // in the future).
    assert!(
        bucket(&format!(
            r#"{{"Statement": {{"Principal": "*", {ok},
              "Condition": {{"DateLessThan": {{"aws:CurrentTime": "99999999999999-01-01"}}}}}}}}"#
        ))
        .is_err()
    );
    assert_eq!(date_ms("2026-01-01T00:00+99999999999999999:00"), None);
    assert_eq!(date_ms("2026-01-01T00:00:99999999999999999Z"), None);
    assert_eq!(date_ms("2026-02-31"), None);
    assert_eq!(date_ms("2024-02-29"), Some(1_709_164_800_000));
    assert_eq!(date_ms("2023-02-29"), None);
    // A key given twice is refused.
    assert!(bucket(r#"{"Statement": {"Effect": "Deny", "Effect": "Allow", "Principal": "*", "Action": "s3:*", "Resource": "*"}}"#).is_err());
    assert!(
        bucket(&format!(
            r#"{{"Statement": {{"Principal": "*", {ok}, "Condition": {{
              "StringEquals": {{"aws:SourceVpc": "vpc-1"}}, "StringEquals": {{"aws:Referer": "x"}}}}}}}}"#
        ))
        .is_err()
    );
    // An IPv4-mapped IPv6 address is its IPv4 address, in blocks and in
    // requests.
    let deny_block = bucket_policy(
        r#"{"Statement": [
          {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "*"},
          {"Effect": "Deny", "Principal": "*", "Action": "s3:GetObject", "Resource": "*",
           "Condition": {"IpAddress": {"aws:SourceIp": "203.0.113.0/24"}}}]}"#,
    );
    for ip in ["203.0.113.7", "::ffff:203.0.113.7"] {
        assert_eq!(
            decide(
                &Identity::Anonymous,
                "s3:GetObject",
                "arn:aws:s3:::b/k",
                &[("aws:SourceIp", ip)],
                &[],
                Some(&deny_block)
            ),
            Decision::ExplicitDeny,
            "{ip}"
        );
    }
    assert!(is_public(&bucket_policy(
        r#"{"Statement": {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "*",
            "Condition": {"IpAddress": {"aws:SourceIp": "::ffff:0.0.0.0/96"}}}}"#
    )));
    // Numbers are decimals: no exponent, infinity or NaN in a policy, and a
    // request value that is none does not compare.
    for number in ["1e9", "inf", "NaN", "0x20", "+1"] {
        assert!(
            bucket(&format!(
                r#"{{"Statement": {{"Principal": "*", {ok},
                  "Condition": {{"NumericLessThan": {{"s3:max-keys": "{number}"}}}}}}}}"#
            ))
            .is_err(),
            "{number}"
        );
    }
    assert!(!condition(
        r#"{"NumericLessThan": {"s3:max-keys": "10"}}"#,
        &[("s3:max-keys", &["1e0"])]
    ));
    assert!(condition(
        r#"{"NumericLessThan": {"s3:max-keys": "10.5"}}"#,
        &[("s3:max-keys", &["-3.25"])]
    ));
    // A malformed variable is refused rather than matching nothing.
    for pattern in [
        "arn:aws:s3:::b/${aws:username, nobody}/*",
        "arn:aws:s3:::b/${aws:username",
        "arn:aws:s3:::b/${aws:username, 'a}b'}",
        "arn:aws:s3:::b/${}",
    ] {
        assert!(
            bucket(&format!(
                r#"{{"Version": "2012-10-17", "Statement": {{"Principal": "*", "Effect": "Deny",
                  "Action": "s3:GetObject", "Resource": "{pattern}"}}}}"#
            ))
            .is_err(),
            "{pattern}"
        );
    }
    assert!(
        bucket(
            r#"{"Version": "2008-10-17", "Statement": {"Principal": "*", "Effect": "Deny",
            "Action": "s3:GetObject", "Resource": "arn:aws:s3:::b/${aws:username"}}"#
        )
        .is_ok(),
        "without variables it is literal text"
    );
    // Round 2: no default on `${*}`; no variable before the resource part.
    for pattern in [
        "arn:aws:s3:::b/${*, 'x'}",
        "arn:aws:s3:${aws:username}::b/*",
    ] {
        assert!(
            bucket(&format!(
                r#"{{"Version": "2012-10-17", "Statement": {{"Principal": "*", "Effect": "Deny",
                  "Action": "s3:GetObject", "Resource": "{pattern}"}}}}"#
            ))
            .is_err(),
            "{pattern}"
        );
    }
    // Large JSON numbers are decimals too.
    assert!(condition(
        r#"{"NumericLessThan": {"s3:max-keys": 1e21}}"#,
        &[("s3:max-keys", &["10"])]
    ));
    assert!(condition(
        r#"{"NumericLessThan": {"s3:max-keys": 18446744073709551616}}"#,
        &[("s3:max-keys", &["10"])]
    ));
    // An empty condition block is refused.
    assert!(
        bucket(&format!(
            r#"{{"Statement": {{"Principal": "*", {ok}, "Condition": {{"IpAddress": {{}}}}}}}}"#
        ))
        .is_err()
    );
    // Valid AWS documents: Bool with a variable; a short ARN in an identity
    // policy (its missing parts are wildcards); no Id in an identity policy.
    assert!(condition(
        r#"{"Bool": {"aws:SecureTransport": "${aws:SecureTransport}"}}"#,
        &[("aws:SecureTransport", &["true"])]
    ));
    let short = identity_policy(
        r#"{"Statement": {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3"}}"#,
    );
    assert_eq!(
        decide(
            &user(OWNER, "alice"),
            "s3:GetObject",
            "arn:aws:s3:::b/k",
            &[],
            &[&short],
            None
        ),
        Decision::Allow
    );
    assert!(bucket(r#"{"Statement": {"Principal": "*", "Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3"}}"#).is_err());
    assert!(
        Policy::parse(
            &format!(r#"{{"Id": "x", "Statement": {{{ok}}}}}"#),
            PolicyKind::Managed
        )
        .is_err()
    );
    assert!(
        bucket(&format!(
            r#"{{"Id": "x", "Statement": {{"Principal": "*", {ok}}}}}"#
        ))
        .is_ok()
    );
}

#[test]
fn without_public_grants_keeps_the_principals_a_statement_names() {
    let policy = bucket_policy(
        r#"{"Statement": [
            {"Effect": "Allow", "Principal": {"AWS": ["*", "arn:aws:iam::111111111111:user/alice"]},
             "Action": "s3:GetObject", "Resource": "*"},
            {"Effect": "Allow", "Principal": "*", "Action": "s3:ListBucket", "Resource": "*"},
            {"Effect": "Deny", "Principal": "*", "Action": "s3:DeleteObject", "Resource": "*"}]}"#,
    );
    let restricted = without_public_grants(&policy);
    assert!(!is_public(&restricted));
    let alice = user(OWNER, "alice");
    let bob = user(OWNER, "bob");
    let get = |identity: &Identity, action: &str| {
        decide(
            identity,
            action,
            "arn:aws:s3:::b/k",
            &[],
            &[],
            Some(&restricted),
        )
    };
    assert_eq!(
        get(&alice, "s3:GetObject"),
        Decision::Allow,
        "named, so kept"
    );
    assert_eq!(
        get(&bob, "s3:GetObject"),
        Decision::ImplicitDeny,
        "only through anyone"
    );
    assert_eq!(get(&alice, "s3:ListBucket"), Decision::ImplicitDeny);
    assert_eq!(
        get(&root(OWNER), "s3:DeleteObject"),
        Decision::ExplicitDeny,
        "a deny stays"
    );
}
