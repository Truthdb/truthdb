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
    copy_source: Mutex<Option<String>>,
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

    /// Who sent the request, and `AuthHeader` or `QueryString` (none:
    /// anonymous, or not known).
    pub(crate) fn requester(&self) -> (Option<String>, Option<&'static str>) {
        let (requester, signed) = self
            .0
            .requester
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let auth_type = signed.map(|signed| match signed.auth_type {
            "REST-QUERY-STRING" => "QueryString",
            _ => "AuthHeader",
        });
        (requester, auth_type)
    }

    /// The owner the bucket had when the request was authorized.
    pub(crate) fn owner(&self) -> Option<String> {
        self.0
            .owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn mark_copy_source(&self, bucket: &str) {
        *self
            .0
            .copy_source
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(bucket.to_string());
    }

    pub(crate) fn copy_source(&self) -> Option<String> {
        self.0
            .copy_source
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
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
        Some("s3vectors" | "iam" | "sts" | "monitoring")
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
    counters: Arc<BucketRequests>,
    started: Instant,
    /// The metric history's series the request counts in, and when it
    /// came (seconds).
    history: Option<(Arc<MetricRecorder>, Matched, u64)>,
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
        counters: Arc<BucketRequests>,
        started: Instant,
        history: Option<(Arc<MetricRecorder>, Matched, u64)>,
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
            self.counters.finished(self.sent, total);
            if let Some((recorder, series, at)) = &self.history {
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
