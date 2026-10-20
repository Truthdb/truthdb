//! Conditions, and the matching of patterns: wildcards (`*`, `?`), policy
//! variables (`${key}`, `${key, 'default'}`, `${*}`, `${?}`, `${$}`) and
//! ARNs.

use std::net::IpAddr;

use base64::Engine as _;
use serde_json::Value;

use super::Request;
use super::policy::MalformedPolicy;

/// One character of a pattern: a literal, or a wildcard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tok {
    Lit(char),
    /// `*`: any run of characters.
    Many,
    /// `?`: one character.
    One,
}

/// `pattern` as tokens: `*` and `?` are wildcards when `wildcards` is set,
/// and (when `variables` is set) each `${...}` is replaced by its value from
/// the request, as literal text. `None` when a variable has no value (and no
/// default): the pattern then matches nothing.
pub(crate) fn expand(
    pattern: &str,
    wildcards: bool,
    variables: bool,
    request: &Request<'_>,
) -> Option<Vec<Tok>> {
    let mut out = Vec::new();
    let mut rest = pattern;
    while let Some(c) = rest.chars().next() {
        if variables
            && let Some(after) = rest.strip_prefix("${")
            && let Some(end) = after.find('}')
        {
            let inner = &after[..end];
            rest = &after[end + 1..];
            match inner {
                "*" => out.push(Tok::Lit('*')),
                "?" => out.push(Tok::Lit('?')),
                "$" => out.push(Tok::Lit('$')),
                _ => {
                    // `${key, 'default'}`.
                    let (key, default) = match inner.split_once(',') {
                        Some((key, default)) => {
                            let default = default.trim();
                            let default = default
                                .strip_prefix('\'')
                                .and_then(|d| d.strip_suffix('\''))?;
                            (key.trim(), Some(default.to_string()))
                        }
                        None => (inner.trim(), None),
                    };
                    let value = request.value(key).or(default)?;
                    out.extend(value.chars().map(Tok::Lit));
                }
            }
            continue;
        }
        out.push(match c {
            '*' if wildcards => Tok::Many,
            '?' if wildcards => Tok::One,
            c => Tok::Lit(c),
        });
        rest = &rest[c.len_utf8()..];
    }
    Some(out)
}

/// Whether `text` matches the tokens (`ignore_case`: ASCII letters match
/// either case).
pub(crate) fn glob(tokens: &[Tok], text: &str, ignore_case: bool) -> bool {
    let text: Vec<char> = text.chars().collect();
    let same = |a: char, b: char| {
        if ignore_case {
            a.eq_ignore_ascii_case(&b)
        } else {
            a == b
        }
    };
    // The classic two-pointer match, backtracking to the last `*`.
    let (mut t, mut s) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while s < text.len() {
        match tokens.get(t) {
            Some(Tok::Many) => {
                star = Some((t, s));
                t += 1;
            }
            Some(Tok::One) => {
                t += 1;
                s += 1;
            }
            Some(Tok::Lit(c)) if same(*c, text[s]) => {
                t += 1;
                s += 1;
            }
            _ => match star {
                Some((star_t, star_s)) => {
                    t = star_t + 1;
                    s = star_s + 1;
                    star = Some((star_t, star_s + 1));
                }
                None => return false,
            },
        }
    }
    tokens[t..].iter().all(|tok| *tok == Tok::Many)
}

/// Whether ARN `value` matches ARN pattern `pattern`, each of their six
/// colon-separated parts on its own. Policy variables count in the resource
/// part (after the fifth colon) only when `resource_part_only`; otherwise
/// (ARN conditions) anywhere. `None` when a variable has no value.
pub(crate) fn arn_like(
    pattern: &str,
    value: &str,
    variables: bool,
    resource_part_only: bool,
    request: &Request<'_>,
) -> Option<bool> {
    if pattern == "*" {
        return Some(true);
    }
    let patterns: Vec<&str> = pattern.splitn(6, ':').collect();
    let values: Vec<&str> = value.splitn(6, ':').collect();
    if patterns.len() != 6 || values.len() != 6 {
        return Some(false);
    }
    for (index, (pattern, value)) in patterns.iter().zip(&values).enumerate() {
        let variables = variables && (index == 5 || !resource_part_only);
        if !glob(&expand(pattern, true, variables, request)?, value, false) {
            return Some(false);
        }
    }
    Some(true)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cmp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Cmp {
    fn holds(self, ordering: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::*;
        matches!(
            (self, ordering),
            (Cmp::Eq, Equal)
                | (Cmp::Lt, Less)
                | (Cmp::Le, Less | Equal)
                | (Cmp::Gt, Greater)
                | (Cmp::Ge, Greater | Equal)
        )
    }
}

#[derive(Debug, Clone)]
enum Kind {
    StringEquals,
    StringEqualsIgnoreCase,
    StringLike,
    Arn,
    Numeric(Cmp, Vec<f64>),
    Date(Cmp, Vec<i64>),
    Bool,
    Binary(Vec<Vec<u8>>),
    Ip(Vec<(IpAddr, u8)>),
    /// `Null`: `true` for each policy value `"true"` (the key is absent).
    Null(Vec<bool>),
}

/// How a condition treats a key's several values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Set {
    Single,
    AnyValue,
    AllValues,
}

/// One condition: an operator applied to one key and its policy values.
#[derive(Debug, Clone)]
pub(crate) struct Condition {
    kind: Kind,
    negated: bool,
    if_exists: bool,
    set: Set,
    pub key: String,
    pub values: Vec<String>,
}

fn malformed(message: String) -> MalformedPolicy {
    MalformedPolicy(message)
}

impl Condition {
    /// Parses one condition (`variables`: the policy's version has policy
    /// variables).
    pub fn parse(
        operator: &str,
        key: &str,
        values: &Value,
        variables: bool,
    ) -> Result<Condition, MalformedPolicy> {
        let (set, name) = match operator.split_once(':') {
            Some(("ForAnyValue", name)) => (Set::AnyValue, name),
            Some(("ForAllValues", name)) => (Set::AllValues, name),
            Some(_) => return Err(malformed(format!("Unknown condition operator {operator}."))),
            None => (Set::Single, operator),
        };
        let (name, if_exists) = match name.strip_suffix("IfExists") {
            Some(name) if name != "Null" => (name, true),
            _ => (name, false),
        };
        let values: Vec<String> = match values {
            Value::Array(items) => items.iter().map(scalar).collect::<Option<_>>(),
            other => scalar(other).map(|value| vec![value]),
        }
        .filter(|values: &Vec<String>| !values.is_empty())
        .ok_or_else(|| {
            malformed(format!(
                "Condition {operator} {key} must have one or more string, number or boolean values."
            ))
        })?;
        let bad = |what: &str| malformed(format!("Condition {operator} {key}: {what}."));
        let cmp = |suffix: &str| match suffix {
            "Equals" | "NotEquals" => Some(Cmp::Eq),
            "LessThan" => Some(Cmp::Lt),
            "LessThanEquals" => Some(Cmp::Le),
            "GreaterThan" => Some(Cmp::Gt),
            "GreaterThanEquals" => Some(Cmp::Ge),
            _ => None,
        };
        let (kind, negated) = match name {
            "StringEquals" => (Kind::StringEquals, false),
            "StringNotEquals" => (Kind::StringEquals, true),
            "StringEqualsIgnoreCase" => (Kind::StringEqualsIgnoreCase, false),
            "StringNotEqualsIgnoreCase" => (Kind::StringEqualsIgnoreCase, true),
            "StringLike" => (Kind::StringLike, false),
            "StringNotLike" => (Kind::StringLike, true),
            "ArnEquals" | "ArnLike" => (Kind::Arn, false),
            "ArnNotEquals" | "ArnNotLike" => (Kind::Arn, true),
            "Bool" => {
                let valid = |v: &String| {
                    v.eq_ignore_ascii_case("true")
                        || v.eq_ignore_ascii_case("false")
                        || (variables && v.contains("${"))
                };
                if !values.iter().all(valid) {
                    return Err(bad("values must be true or false"));
                }
                (Kind::Bool, false)
            }
            "BinaryEquals" => (
                Kind::Binary(
                    values
                        .iter()
                        .map(|v| base64::engine::general_purpose::STANDARD.decode(v))
                        .collect::<Result<_, _>>()
                        .map_err(|_| bad("values must be base64"))?,
                ),
                false,
            ),
            "IpAddress" | "NotIpAddress" => (
                Kind::Ip(
                    values
                        .iter()
                        .map(|v| cidr(v))
                        .collect::<Option<_>>()
                        .ok_or_else(|| bad("values must be IP addresses or CIDR blocks"))?,
                ),
                name == "NotIpAddress",
            ),
            "Null" => (
                Kind::Null(
                    values
                        .iter()
                        .map(|v| match v.to_ascii_lowercase().as_str() {
                            "true" => Ok(true),
                            "false" => Ok(false),
                            _ => Err(bad("values must be true or false")),
                        })
                        .collect::<Result<_, _>>()?,
                ),
                false,
            ),
            _ => {
                if let Some(suffix) = name.strip_prefix("Numeric")
                    && let Some(cmp) = cmp(suffix)
                {
                    (
                        Kind::Numeric(
                            cmp,
                            values
                                .iter()
                                .map(|v| decimal(v))
                                .collect::<Option<_>>()
                                .ok_or_else(|| bad("values must be numbers"))?,
                        ),
                        suffix == "NotEquals",
                    )
                } else if let Some(suffix) = name.strip_prefix("Date")
                    && let Some(cmp) = cmp(suffix)
                {
                    (
                        Kind::Date(
                            cmp,
                            values
                                .iter()
                                .map(|v| date_ms(v))
                                .collect::<Option<_>>()
                                .ok_or_else(|| {
                                    bad("values must be ISO 8601 dates or epoch seconds")
                                })?,
                        ),
                        suffix == "NotEquals",
                    )
                } else {
                    return Err(malformed(format!("Unknown condition operator {operator}.")));
                }
            }
        };
        if variables
            && matches!(
                kind,
                Kind::StringEquals
                    | Kind::StringEqualsIgnoreCase
                    | Kind::StringLike
                    | Kind::Arn
                    | Kind::Bool
            )
        {
            for value in &values {
                check_variables(value).map_err(malformed)?;
            }
        }
        Ok(Condition {
            kind,
            negated,
            if_exists,
            set,
            key: key.to_string(),
            values,
        })
    }

    /// Whether the condition holds for `request` (`variables`: the policy's
    /// version expands policy variables).
    pub fn holds(&self, request: &Request<'_>, variables: bool) -> bool {
        let present = request
            .values(&self.key)
            .filter(|values| !values.is_empty());
        if let Kind::Null(expect_absent) = &self.kind {
            return expect_absent.contains(&present.is_none());
        }
        let Some(present) = present else {
            return match self.set {
                Set::Single => self.if_exists || self.negated,
                Set::AnyValue => self.if_exists,
                Set::AllValues => true,
            };
        };
        // A request value "matches" a negated operator when it matches none
        // of the policy values.
        let matches = |value: &String| self.matches_any(value, request, variables) != self.negated;
        match self.set {
            Set::Single | Set::AnyValue => present.iter().any(matches),
            Set::AllValues => present.iter().all(matches),
        }
    }

    /// Whether request value `value` matches any of the policy values.
    fn matches_any(&self, value: &str, request: &Request<'_>, variables: bool) -> bool {
        match &self.kind {
            Kind::StringEquals | Kind::StringEqualsIgnoreCase | Kind::StringLike => {
                let wildcards = matches!(self.kind, Kind::StringLike);
                let ignore_case = matches!(self.kind, Kind::StringEqualsIgnoreCase);
                self.values.iter().any(|pattern| {
                    expand(pattern, wildcards, variables, request)
                        .is_some_and(|tokens| glob(&tokens, value, ignore_case))
                })
            }
            Kind::Arn => self.values.iter().any(|pattern| {
                arn_like(pattern, value, variables, false, request).unwrap_or(false)
            }),
            Kind::Numeric(cmp, numbers) => decimal(value).is_some_and(|value| {
                numbers.iter().any(|number| {
                    value
                        .partial_cmp(number)
                        .is_some_and(|ordering| cmp.holds(ordering))
                })
            }),
            Kind::Date(cmp, dates) => date_ms(value)
                .is_some_and(|value| dates.iter().any(|date| cmp.holds(value.cmp(date)))),
            Kind::Bool => self.values.iter().any(|v| {
                expand(v, false, variables, request).is_some_and(|tokens| {
                    let v: String = tokens
                        .iter()
                        .map(|tok| match tok {
                            Tok::Lit(c) => *c,
                            Tok::Many => '*',
                            Tok::One => '?',
                        })
                        .collect();
                    v.eq_ignore_ascii_case(value)
                })
            }),
            Kind::Binary(bytes) => base64::engine::general_purpose::STANDARD
                .decode(value)
                .is_ok_and(|value| bytes.contains(&value)),
            Kind::Ip(blocks) => value.parse::<IpAddr>().is_ok_and(|address| {
                let address = address.to_canonical();
                blocks
                    .iter()
                    .any(|(net, bits)| in_block(address, *net, *bits))
            }),
            Kind::Null(_) => false,
        }
    }

    /// Whether this condition pins `key` (case aside) to fixed values: a
    /// positive equality or match, every value without wildcard or variable
    /// (and, for `aws:SourceIp`, no block wider than /8 or /32 for IPv6).
    pub fn pins(&self, key: &str) -> bool {
        if !self.key.eq_ignore_ascii_case(key)
            || self.negated
            || self.if_exists
            || self.set == Set::AllValues
        {
            return false;
        }
        let fixed = |v: &String| !v.contains(['*', '?']) && !v.contains("${");
        match &self.kind {
            Kind::StringEquals | Kind::StringEqualsIgnoreCase | Kind::StringLike | Kind::Arn => {
                self.values.iter().all(fixed)
            }
            Kind::Ip(blocks) => blocks.iter().all(|(net, bits)| match net {
                IpAddr::V4(_) => *bits >= 8,
                IpAddr::V6(_) => *bits >= 32,
            }),
            _ => false,
        }
    }
}

/// A JSON scalar as a condition value.
fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        // As a plain decimal: a float's Display has no exponent.
        Value::Number(number) => Some(match number.as_f64() {
            Some(float) if !number.is_i64() && !number.is_u64() => float.to_string(),
            _ => number.to_string(),
        }),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

/// An address or CIDR block (a bare address is /32, or /128 for IPv6).
fn cidr(text: &str) -> Option<(IpAddr, u8)> {
    let (address, bits) = match text.split_once('/') {
        Some((address, bits)) => (address, Some(bits.parse::<u8>().ok()?)),
        None => (text, None),
    };
    let address: IpAddr = address.parse().ok()?;
    let max = if address.is_ipv4() { 32 } else { 128 };
    let bits = bits.unwrap_or(max);
    if bits > max {
        return None;
    }
    // An IPv4-mapped IPv6 block is the IPv4 block it maps (a request's
    // address is compared in its canonical form too).
    match (address, address.to_canonical()) {
        (IpAddr::V6(_), v4 @ IpAddr::V4(_)) if bits >= 96 => Some((v4, bits - 96)),
        _ => Some((address, bits)),
    }
}

/// A decimal number as IAM writes one: an optional `-`, digits, and an
/// optional fraction (no exponent, infinity or NaN).
fn decimal(text: &str) -> Option<f64> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, "0"));
    let all_digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    (all_digits(whole) && all_digits(fraction))
        .then(|| text.parse().ok())
        .flatten()
}

/// Checks a pattern's policy variables: each `${` closes with `}` and holds
/// `*`, `?`, `$`, a key, or a key and a quoted default (`${key, 'value'}`).
pub(crate) fn check_variables(pattern: &str) -> Result<(), String> {
    let mut rest = pattern;
    while let Some(at) = rest.find("${") {
        let after = &rest[at + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| format!("{pattern}: a policy variable is not closed."))?;
        let inner = &after[..end];
        let key = match inner.split_once(',') {
            Some((key, _)) if matches!(key.trim(), "*" | "?" | "$") => {
                return Err(format!("{pattern}: ${{{}}} takes no default.", key.trim()));
            }
            Some((key, default)) => {
                let default = default.trim();
                if default.len() < 2
                    || !default.starts_with('\'')
                    || !default.ends_with('\'')
                    || default[1..default.len() - 1].contains('\'')
                {
                    return Err(format!(
                        "{pattern}: a policy variable's default must be quoted: ${{key, 'value'}}."
                    ));
                }
                key.trim()
            }
            None => inner,
        };
        if !matches!(key, "*" | "?" | "$")
            && (key.is_empty()
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b":/-_.".contains(&b)))
        {
            return Err(format!("{pattern}: {inner} is not a policy variable."));
        }
        rest = &after[end + 1..];
    }
    Ok(())
}

fn in_block(address: IpAddr, net: IpAddr, bits: u8) -> bool {
    match (address, net) {
        (IpAddr::V4(address), IpAddr::V4(net)) => {
            let mask = u32::MAX.checked_shl(32 - u32::from(bits)).unwrap_or(0);
            u32::from(address) & mask == u32::from(net) & mask
        }
        (IpAddr::V6(address), IpAddr::V6(net)) => {
            let mask = u128::MAX.checked_shl(128 - u32::from(bits)).unwrap_or(0);
            u128::from(address) & mask == u128::from(net) & mask
        }
        _ => false,
    }
}

/// A date as milliseconds since the epoch: epoch seconds (any bare number,
/// so `2026` is a second, not a year), or one of the W3C ISO 8601 forms
/// (`YYYY-MM`, `YYYY-MM-DD`, then `Thh:mm`, `Thh:mm:ss` or `Thh:mm:ss.s…`,
/// with `Z` or `±hh:mm`).
pub(crate) fn date_ms(text: &str) -> Option<i64> {
    if let Ok(seconds) = text.parse::<i64>() {
        return seconds.checked_mul(1000);
    }
    let number = |s: &str| -> Option<i64> {
        (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse().ok())
            .flatten()
    };
    let (date, time) = match text.split_once('T') {
        Some((date, time)) => (date, Some(time)),
        None => (text, None),
    };
    let mut parts = date.split('-');
    let year = number(parts.next()?).filter(|year| (0..=9999).contains(year))?;
    let month = parts.next().map(number).unwrap_or(Some(1))?;
    let day = parts.next().map(number).unwrap_or(Some(1))?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=days_in_month).contains(&day) {
        return None;
    }
    let mut ms = days_from_civil(year, month, day) * 86_400_000;
    if let Some(time) = time {
        let (clock, offset_minutes) = if let Some(clock) = time.strip_suffix('Z') {
            (clock, 0)
        } else {
            let at = time.rfind(['+', '-'])?;
            let (clock, offset) = time.split_at(at);
            let sign = if offset.starts_with('-') { -1 } else { 1 };
            let (hours, minutes) = offset[1..].split_once(':')?;
            let (hours, minutes) = (number(hours)?, number(minutes)?);
            if hours > 23 || minutes > 59 {
                return None;
            }
            (clock, sign * (hours * 60 + minutes))
        };
        let mut fields = clock.split(':');
        let hours = number(fields.next()?)?;
        let minutes = number(fields.next()?)?;
        let seconds_ms = match fields.next() {
            None => 0,
            Some(seconds) => {
                let (whole, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
                let fraction_ms = if fraction.is_empty() {
                    0
                } else {
                    let digits: String = format!("{fraction:0<3}").chars().take(3).collect();
                    number(&digits)?
                };
                number(whole).filter(|seconds| *seconds < 61)? * 1000 + fraction_ms
            }
        };
        if fields.next().is_some() || hours > 23 || minutes > 59 || seconds_ms >= 61_000 {
            return None;
        }
        ms += (hours * 60 + minutes - offset_minutes) * 60_000 + seconds_ms;
    }
    Some(ms)
}

/// Days from 1970-01-01 to a civil date (Howard Hinnant's days-from-civil).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
