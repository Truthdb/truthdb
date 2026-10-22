//! RestoreObject: makes a version in an archive class (GLACIER,
//! DEEP_ARCHIVE) readable for a number of days. Nothing is copied: the
//! object engine keeps a window of time in which reads are allowed (see
//! `truthdb_object::Restore`). S3 takes minutes to hours to restore; here
//! each retrieval tier takes a configured number of seconds (none by
//! default).

use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use truthdb_object::{RestoreState, Restored};

use crate::error::S3Error;
use crate::object_lock::{leaf, leaves, malformed};
use crate::objects::{http_date, now_ms, object_error, version_sel};
use crate::server::{Authenticated, Body, S3State, full};

/// A restore request's body is small.
const MAX_BODY: usize = 64 * 1024;

/// How long a restore takes, in seconds, per retrieval tier.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreDelays {
    pub expedited: u64,
    pub standard: u64,
    pub bulk: u64,
}

/// A restore's retrieval tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
    Expedited,
    Standard,
    Bulk,
}

impl RestoreDelays {
    fn of(&self, tier: Tier) -> u64 {
        match tier {
            Tier::Expedited => self.expedited,
            Tier::Standard => self.standard,
            Tier::Bulk => self.bulk,
        }
    }
}

/// GET of an archived version that is not restored, and a copy from one.
pub fn invalid_object_state() -> S3Error {
    S3Error::new(
        403,
        "InvalidObjectState",
        "The operation is not valid for the object's storage class",
    )
}

/// The `x-amz-restore` value of a version with a restore under way or done.
pub fn header(state: Option<RestoreState>) -> Option<String> {
    match state? {
        RestoreState::Ongoing => Some("ongoing-request=\"true\"".to_string()),
        RestoreState::Restored { expiry_ms } => Some(format!(
            "ongoing-request=\"false\", expiry-date=\"{}\"",
            http_date(expiry_ms)
        )),
    }
}

/// A RestoreRequest body: the days the restored copy is kept, and the
/// retrieval tier (`GlacierJobParameters/Tier`, else `Tier`, else
/// Standard). S3 Select restores (`Type` SELECT) are not offered.
fn parse(body: &[u8]) -> Result<(u32, Tier), S3Error> {
    let found = leaves(
        body,
        "RestoreRequest",
        &[
            "Days",
            "Description",
            "GlacierJobParameters",
            "GlacierJobParameters/Tier",
            "Tier",
            "Type",
        ],
    )?;
    if leaf(&found, "Type").is_some() {
        return Err(S3Error::not_implemented(
            "S3 Select restores are not offered",
        ));
    }
    let days: u32 = leaf(&found, "Days")
        .ok_or_else(malformed)?
        .parse()
        .map_err(|_| malformed())?;
    if days == 0 {
        return Err(S3Error::new(
            400,
            "InvalidArgument",
            "The number of days must be greater than zero",
        ));
    }
    let tier = match leaf(&found, "GlacierJobParameters/Tier").or_else(|| leaf(&found, "Tier")) {
        None | Some("Standard") => Tier::Standard,
        Some("Expedited") => Tier::Expedited,
        Some("Bulk") => Tier::Bulk,
        Some(_) => return Err(malformed()),
    };
    Ok((days, tier))
}

/// POST `?restore` on an object (S3 RestoreObject): 202 when a restore
/// begins, 200 when the version was already restored (its expiry moves).
pub async fn restore_object(
    state: &S3State,
    request: Request<Incoming>,
    auth: Authenticated,
    bucket: String,
    key: String,
) -> Result<Response<Body>, S3Error> {
    let which = version_sel(request.uri().query().unwrap_or(""))?;
    let body = crate::server::read_body(request, auth, MAX_BODY).await?;
    let (days, tier) = parse(&body)?;
    let delay_ms = state.restore_delays.of(tier).saturating_mul(1000);
    let (bucket_name, key_name) = (bucket.clone(), key.clone());
    let restored = crate::scope::object(state, move |file| {
        file.transaction(|tx| {
            tx.restore_object(&bucket_name, &key_name, which, days, delay_ms, now_ms())
        })
    })
    .await?
    .map_err(|err| object_error(err, &bucket, &key))?;
    let status = match restored.0 {
        Restored::Started => StatusCode::ACCEPTED,
        Restored::Extended => StatusCode::OK,
    };
    Ok(Response::builder()
        .status(status)
        .body(full(bytes::Bytes::new()))
        .expect("a valid response"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_restore_request_names_its_days_and_tier() {
        let parsed = |xml: &str| parse(xml.as_bytes());
        assert_eq!(
            parsed("<RestoreRequest><Days>3</Days></RestoreRequest>").unwrap(),
            (3, Tier::Standard)
        );
        assert_eq!(
            parsed(
                "<RestoreRequest xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Days>1</Days>\
                 <GlacierJobParameters><Tier>Bulk</Tier></GlacierJobParameters>\
                 <Tier>Expedited</Tier></RestoreRequest>"
            )
            .unwrap(),
            (1, Tier::Bulk)
        );
        assert_eq!(
            parsed("<RestoreRequest><Days>2</Days><Tier>Expedited</Tier></RestoreRequest>")
                .unwrap()
                .1,
            Tier::Expedited
        );
        for bad in [
            "<RestoreRequest/>",
            "<RestoreRequest><Days>x</Days></RestoreRequest>",
            "<RestoreRequest><Days>-1</Days></RestoreRequest>",
            "<RestoreRequest><Days>1</Days><Days>2</Days></RestoreRequest>",
            "<RestoreRequest><Days>1</Days><OutputLocation/></RestoreRequest>",
            "<RestoreRequest><Days>1</Days><Tier>Glacial</Tier></RestoreRequest>",
        ] {
            assert_eq!(parsed(bad).unwrap_err().code, "MalformedXML", "{bad}");
        }
        assert_eq!(
            parsed("<RestoreRequest><Days>0</Days></RestoreRequest>")
                .unwrap_err()
                .code,
            "InvalidArgument"
        );
        assert_eq!(
            parsed("<RestoreRequest><Type>SELECT</Type></RestoreRequest>")
                .unwrap_err()
                .code,
            "NotImplemented"
        );
    }

    #[test]
    fn the_restore_header_says_whether_it_is_done_and_until_when() {
        assert_eq!(header(None), None);
        assert_eq!(
            header(Some(RestoreState::Ongoing)).unwrap(),
            "ongoing-request=\"true\""
        );
        assert_eq!(
            header(Some(RestoreState::Restored {
                expiry_ms: 1_356_048_000_000
            }))
            .unwrap(),
            "ongoing-request=\"false\", expiry-date=\"Fri, 21 Dec 2012 00:00:00 GMT\""
        );
    }
}
