//! Conditional writes and deletes: `If-None-Match: *` (write only if the key
//! has no current version) and `If-Match` (write or delete only if its
//! current version has this ETag), on PutObject, CopyObject's destination,
//! CompleteMultipartUpload and DeleteObject (and DeleteObjects' per-object
//! ETag). The condition is decided in the write's own transaction, against
//! the key's current version: for a write a delete marker is none, for a
//! delete `If-Match` finds a marker no match (412). A write checks it once
//! before its body arrives too, so it fails before sending it; a condition
//! that held then and fails at the write is a conflict.

use hyper::HeaderMap;

use crate::error::S3Error;

/// What a request's conditional headers ask of the key's current version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Condition {
    /// `If-Match`: its ETags (or `*`, any).
    pub if_match: Option<String>,
    /// `If-None-Match: *`.
    pub if_none_match: bool,
}

impl Condition {
    /// A write's conditional headers, if it has any. `If-None-Match` takes
    /// only `*`, as S3 takes it.
    pub(crate) fn of_write(headers: &HeaderMap) -> Result<Option<Condition>, S3Error> {
        let if_none_match = match crate::objects::header(headers, "if-none-match") {
            None => false,
            Some(value) if value.trim() == "*" => true,
            Some(_) => {
                return Err(S3Error::not_implemented(
                    "If-None-Match other than * on a write is not implemented",
                ));
            }
        };
        Ok(Self::with(
            crate::objects::header(headers, "if-match"),
            if_none_match,
        ))
    }

    /// A delete's: `If-Match` only.
    pub(crate) fn of_delete(headers: &HeaderMap) -> Result<Option<Condition>, S3Error> {
        // Only directory buckets take these, as S3 takes them.
        for name in ["x-amz-if-match-last-modified-time", "x-amz-if-match-size"] {
            if crate::objects::header(headers, name).is_some() {
                return Err(S3Error::not_implemented(format!(
                    "{name} is for directory buckets, which are not implemented"
                )));
            }
        }
        Ok(Self::with(
            crate::objects::header(headers, "if-match"),
            false,
        ))
    }

    fn with(if_match: Option<String>, if_none_match: bool) -> Option<Condition> {
        (if_match.is_some() || if_none_match).then_some(Condition {
            if_match,
            if_none_match,
        })
    }

    /// Whether the key's current version for a write (its quoted ETag;
    /// none: it has none, or a delete marker) meets the condition: 412 when
    /// it does not, 404 when `If-Match` finds no version to compare.
    pub(crate) fn check(&self, current: Option<&str>) -> Result<(), S3Error> {
        if self.if_none_match && current.is_some() {
            return Err(failed());
        }
        if let Some(wanted) = &self.if_match {
            let Some(current) = current else {
                return Err(S3Error::new(
                    404,
                    "NoSuchKey",
                    "The specified key does not exist.",
                ));
            };
            if !crate::objects::etag_matches(wanted, current) {
                return Err(failed());
            }
        }
        Ok(())
    }

    /// [`Self::check`] at the write, its check before the body having held:
    /// a version `If-Match` named that was removed meanwhile is a conflict
    /// (409), as S3 answers a delete that won the race.
    pub(crate) fn recheck(&self, current: Option<&str>) -> Result<(), S3Error> {
        match (&self.if_match, current) {
            (Some(_), None) => Err(S3Error::new(
                409,
                "ConditionalRequestConflict",
                "A conflicting operation occurred. If using PutObject you can retry the request. \
                 If using multipart upload you should initiate another CreateMultipartUpload \
                 request and re-upload each part.",
            )),
            _ => self.check(current),
        }
    }

    /// [`Self::check`] for a delete, with the current version as
    /// `current_etag` reads it: a delete marker is there, matching nothing.
    pub(crate) fn check_delete(&self, current: Option<Option<&str>>) -> Result<(), S3Error> {
        match current {
            Some(None) if self.if_match.is_some() => Err(failed()),
            current => self.check(current.flatten()),
        }
    }

    /// The condition keys a bucket policy may test (`s3:if-match`,
    /// `s3:if-none-match`), with the headers' values.
    pub(crate) fn keys(headers: &HeaderMap) -> Vec<(&'static str, String)> {
        ["if-match", "if-none-match"]
            .into_iter()
            .filter_map(|name| {
                crate::objects::header(headers, name).map(|value| {
                    (
                        match name {
                            "if-match" => "s3:if-match",
                            _ => "s3:if-none-match",
                        },
                        value,
                    )
                })
            })
            .collect()
    }
}

fn failed() -> S3Error {
    S3Error::new(
        412,
        "PreconditionFailed",
        "At least one of the pre-conditions you specified did not hold",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(*name, value.parse().unwrap());
        }
        headers
    }

    fn code(result: Result<(), S3Error>) -> Option<(u16, &'static str)> {
        result.err().map(|err| (err.status, err.code))
    }

    #[test]
    fn conditions_hold_or_fail_as_s3_answers_them() {
        let etag = "\"abc\"";
        let absent = Condition::of_write(&headers(&[("if-none-match", "*")]))
            .unwrap()
            .unwrap();
        assert_eq!(code(absent.check(None)), None);
        assert_eq!(
            code(absent.check(Some(etag))),
            Some((412, "PreconditionFailed"))
        );
        let matching = Condition::of_write(&headers(&[("if-match", "\"abc\"")]))
            .unwrap()
            .unwrap();
        assert_eq!(code(matching.check(Some(etag))), None);
        assert_eq!(
            code(matching.check(Some("\"other\""))),
            Some((412, "PreconditionFailed"))
        );
        assert_eq!(code(matching.check(None)), Some((404, "NoSuchKey")));
        // Unquoted, or any.
        for given in ["abc", "*"] {
            let condition = Condition::of_write(&headers(&[("if-match", given)]))
                .unwrap()
                .unwrap();
            assert_eq!(code(condition.check(Some(etag))), None, "{given}");
        }
        // Held before the body, removed at the write: a conflict; replaced:
        // failed.
        assert_eq!(
            code(matching.recheck(None)),
            Some((409, "ConditionalRequestConflict"))
        );
        assert_eq!(
            code(matching.recheck(Some("\"new\""))),
            Some((412, "PreconditionFailed"))
        );
        assert_eq!(
            code(absent.recheck(Some(etag))),
            Some((412, "PreconditionFailed"))
        );
        // None asked; If-None-Match other than *; the directory buckets'
        // delete conditions.
        assert_eq!(Condition::of_write(&headers(&[])).unwrap(), None);
        assert_eq!(
            Condition::of_write(&headers(&[("if-none-match", "\"abc\"")]))
                .unwrap_err()
                .status,
            501
        );
        assert_eq!(
            Condition::of_delete(&headers(&[("x-amz-if-match-size", "3")]))
                .unwrap_err()
                .status,
            501
        );
        let delete = Condition::of_delete(&headers(&[("if-match", "*")]))
            .unwrap()
            .unwrap();
        assert_eq!(code(delete.check_delete(Some(Some(etag)))), None);
        assert_eq!(code(delete.check_delete(None)), Some((404, "NoSuchKey")));
        assert_eq!(
            code(delete.check_delete(Some(None))),
            Some((412, "PreconditionFailed")),
            "a delete marker matches nothing"
        );
        assert_eq!(
            Condition::keys(&headers(&[("if-none-match", "*"), ("if-match", "\"e\"")])),
            [
                ("s3:if-match", "\"e\"".to_string()),
                ("s3:if-none-match", "*".to_string())
            ]
        );
    }
}
