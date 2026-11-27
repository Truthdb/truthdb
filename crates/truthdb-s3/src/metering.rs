//! A request's part in the request metrics (see
//! `truthdb_engine::metrics`): the bucket it counts for and its kind,
//! decided before it is routed, and its answer's body, counted as it is
//! sent.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use hyper::body::{Frame, Incoming, SizeHint};
use hyper::{Method, Request};
use truthdb_engine::metrics::{BucketRequests, RequestKind};

use crate::api::{Target, has_param, plain};
use crate::metric_history::{Matched, MetricRecorder};
use crate::server::Body;

/// What the route found of a request's buckets, in its extensions: whether
/// the bucket it names exists (set where the route loads the bucket to
/// authorize the request, or, on a website endpoint, to read its
/// configuration), and the bucket a copy reads, when the route copies from
/// one that exists. Only buckets that exist are counted: a name anyone may
/// send must not take memory, nor crowd out the buckets there are.
#[derive(Clone, Default)]
pub(crate) struct BucketSeen(Arc<Seen>);

#[derive(Default)]
struct Seen {
    bucket: AtomicBool,
    /// The bucket's owner when the request was authorized (none: not
    /// known where it was marked).
    owner: Mutex<Option<String>>,
    /// Who sent the request and how it was signed (for its access log
    /// line).
    requester: Mutex<(Option<String>, Option<crate::server::Signed>)>,
    /// A copy's source bucket, and its owner when the read was allowed.
    copy_source: Mutex<Option<(String, String)>>,
    /// The access point the request was made through: its bucket and ARN.
    through: Mutex<Option<(String, String)>>,
    /// The Object Lambda access point the request was made to, if its
    /// metrics are on.
    lambda: Mutex<Option<LambdaSeen>>,
    /// The Object Lambda access point whose function answered the request:
    /// its ARN and account.
    answered_by: Mutex<Option<(String, String)>>,
}

/// A request to an Object Lambda access point whose metrics are on (or a
/// function's answer to one): its series, when the access point was made
/// (ms), and what the request was.
#[derive(Clone)]
pub(crate) struct LambdaSeen {
    pub(crate) series: truthdb_object::MetricSeries,
    pub(crate) made_ms: u64,
    pub(crate) role: LambdaRole,
}

/// What a request to an Object Lambda access point was.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LambdaRole {
    /// Answered by its function.
    Invoked,
    /// Passed on to its supporting access point.
    Proxied,
    /// A function's WriteGetObjectResponse.
    Responding,
}

impl BucketSeen {
    pub(crate) fn mark(&self, owner: Option<&str>) {
        *self
            .0
            .owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = owner.map(str::to_string);
        self.0.bucket.store(true, Ordering::Relaxed);
    }

    pub(crate) fn seen(&self) -> bool {
        self.0.bucket.load(Ordering::Relaxed)
    }

    pub(crate) fn mark_requester(
        &self,
        requester: Option<String>,
        signed: Option<crate::server::Signed>,
    ) {
        *self
            .0
            .requester
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = (requester, signed);
    }

    /// Who sent the request, whether it was signed, and `AuthHeader` or
    /// `QueryString` (none: anonymous, a form upload, or not known).
    pub(crate) fn requester(&self) -> (Option<String>, bool, Option<&'static str>) {
        let (requester, signed) = self
            .0
            .requester
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let auth_type = signed.and_then(|signed| match signed.auth_type {
            "REST-HEADER" => Some("AuthHeader"),
            "REST-QUERY-STRING" => Some("QueryString"),
            _ => None,
        });
        (requester, signed.is_some(), auth_type)
    }

    /// The owner the bucket had when the request was authorized.
    pub(crate) fn owner(&self) -> Option<String> {
        self.0
            .owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn mark_copy_source(&self, bucket: &str, owner: &str) {
        *self
            .0
            .copy_source
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some((bucket.to_string(), owner.to_string()));
    }

    pub(crate) fn copy_source(&self) -> Option<String> {
        self.0
            .copy_source
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|(bucket, _)| bucket.clone())
    }

    pub(crate) fn mark_answered_by(&self, arn: &str, account: &str) {
        *self
            .0
            .answered_by
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some((arn.to_string(), account.to_string()));
    }

    /// The Object Lambda access point whose function answered the request:
    /// its ARN and account.
    pub(crate) fn answered_by(&self) -> Option<(String, String)> {
        self.0
            .answered_by
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn mark_lambda(&self, seen: LambdaSeen) {
        *self
            .0
            .lambda
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(seen);
    }

    pub(crate) fn lambda(&self) -> Option<LambdaSeen> {
        self.0
            .lambda
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn mark_through(&self, bucket: &str, arn: &str) {
        *self
            .0
            .through
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some((bucket.to_string(), arn.to_string()));
    }

    /// The access point the request was made through: its bucket and ARN.
    pub(crate) fn through(&self) -> Option<(String, String)> {
        self.0
            .through
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// The copy source bucket's owner when the read was allowed.
    pub(crate) fn copy_source_owner(&self) -> Option<String> {
        self.0
            .copy_source
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|(_, owner)| owner.clone())
    }
}

/// The record of what the route finds, from `request`'s extensions.
pub(crate) fn seen_of<B>(request: &Request<B>) -> Option<BucketSeen> {
    request.extensions().get::<BucketSeen>().cloned()
}

/// Marks, in `request`'s extensions, that the bucket it names exists (and
/// whose it is, if known).
pub(crate) fn mark_seen<B>(request: &Request<B>, owner: Option<&str>) {
    if let Some(seen) = request.extensions().get::<BucketSeen>() {
        seen.mark(owner);
    }
}

/// What a request counts as: its bucket and its kind.
pub(crate) struct Metered {
    pub bucket: String,
    pub kind: RequestKind,
    /// The object's key (none: the bucket's own request), which metrics
    /// configurations filter on.
    pub key: Option<String>,
}

/// What `request` counts as, if it is for a bucket: not for the service
/// (a listing of buckets), S3 Control, S3 Vectors, IAM or STS.
pub(crate) fn metered(request: &Request<Incoming>) -> Option<Metered> {
    let method = request.method();
    if let Some(crate::website::WebsiteHost(bucket)) = request.extensions().get() {
        let kind = match *method {
            Method::GET => RequestKind::Get,
            Method::HEAD => RequestKind::Head,
            _ => RequestKind::Other,
        };
        let key = crate::sigv4::decode_path(request.uri().path())
            .map(|path| path.trim_start_matches('/').to_string());
        return Some(Metered {
            bucket: bucket.clone(),
            kind,
            key,
        });
    }
    if matches!(
        crate::server::request_service(request).as_deref(),
        Some("s3vectors" | "s3tables" | "iam" | "sts" | "monitoring" | "cloudtrail")
    ) || crate::api::control_route(request).is_some()
    {
        return None;
    }
    let (bucket, key) = match crate::api::target(request).ok()? {
        Target::Service => return None,
        Target::Bucket(bucket) => (bucket, None),
        Target::Object(bucket, key) => (bucket, Some(key)),
    };
    let object = key.is_some();
    let kind = match (object, method.clone()) {
        (_, Method::HEAD) => RequestKind::Head,
        (true, Method::GET) if !has_param(request, "uploadId") => RequestKind::Get,
        (true, Method::PUT) => RequestKind::Put,
        (true, Method::DELETE) => RequestKind::Delete,
        (false, Method::POST) if has_param(request, "delete") => RequestKind::Delete,
        (_, Method::POST) => RequestKind::Post,
        (false, Method::GET) if plain(request, &["versions", "uploads"]) => RequestKind::List,
        _ => RequestKind::Other,
    };
    Some(Metered { bucket, kind, key })
}

/// The bytes a request's head says its payload has: an aws-chunked body's
/// decoded length (its chunk framing and signatures left out), else its
/// Content-Length.
pub(crate) fn uploaded(request: &Request<Incoming>) -> u64 {
    ["x-amz-decoded-content-length", "content-length"]
        .iter()
        .find_map(|name| {
            request
                .headers()
                .get(*name)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(0)
}

/// An answer's body, counted as it is sent: its bytes and, once it ends
/// (or is dropped, its client gone), the request's total latency.
pub(crate) struct MeteredBody {
    inner: Body,
    /// Its bucket's request counters (none: it is no bucket's request, but
    /// an Object Lambda access point's).
    counters: Option<Arc<BucketRequests>>,
    started: Instant,
    /// The metric history's series the request counts in, and when it
    /// came (seconds).
    history: Vec<(Arc<MetricRecorder>, Matched, u64)>,
    /// The request's access log line, if its bucket's logging is on.
    log: Option<(
        Arc<crate::access_log::AccessLogger>,
        crate::access_log::Entry,
    )>,
    sent: u64,
    done: bool,
}

impl MeteredBody {
    pub(crate) fn new(
        inner: Body,
        counters: Option<Arc<BucketRequests>>,
        started: Instant,
        history: Vec<(Arc<MetricRecorder>, Matched, u64)>,
        log: Option<(
            Arc<crate::access_log::AccessLogger>,
            crate::access_log::Entry,
        )>,
    ) -> Self {
        MeteredBody {
            inner,
            counters,
            started,
            history,
            log,
            sent: 0,
            done: false,
        }
    }

    fn finish(&mut self) {
        if !self.done {
            self.done = true;
            let total = self.started.elapsed();
            if let Some(counters) = &self.counters {
                counters.finished(self.sent, total);
            }
            for (recorder, series, at) in &self.history {
                recorder.record(
                    series,
                    *at,
                    &crate::metric_history::finished(self.sent, total),
                );
            }
            if let Some((logger, entry)) = &self.log {
                logger.record(entry, self.sent, total.as_millis() as u64);
            }
        }
    }
}

impl hyper::body::Body for MeteredBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.sent += data.len() as u64;
                }
            }
            Poll::Ready(None) => self.finish(),
            _ => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for MeteredBody {
    fn drop(&mut self) {
        self.finish();
    }
}
