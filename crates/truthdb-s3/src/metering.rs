//! A request's part in the request metrics (see
//! `truthdb_engine::metrics`): the bucket it counts for and its kind,
//! decided before it is routed, and its answer's body, counted as it is
//! sent.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use hyper::body::{Frame, Incoming, SizeHint};
use hyper::{Method, Request};
use truthdb_engine::metrics::{BucketRequests, RequestKind};

use crate::api::{Target, has_param, plain};
use crate::server::Body;

/// What a request counts as: its bucket, its kind, and the bucket a copy
/// reads (S3 counts a GET there too).
pub(crate) struct Metered {
    pub bucket: String,
    pub kind: RequestKind,
    pub copy_source: Option<String>,
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
        return Some(Metered {
            bucket: bucket.clone(),
            kind,
            copy_source: None,
        });
    }
    if matches!(
        crate::server::request_service(request).as_deref(),
        Some("s3vectors" | "iam" | "sts")
    ) || crate::api::control_route(request).is_some()
    {
        return None;
    }
    let (bucket, object) = match crate::api::target(request).ok()? {
        Target::Service => return None,
        Target::Bucket(bucket) => (bucket, false),
        Target::Object(bucket, _) => (bucket, true),
    };
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
    let copy_source = (object && *method == Method::PUT)
        .then(|| crate::copy::copy_source(request.headers()).ok())
        .flatten()
        .map(|(bucket, _, _)| bucket);
    Some(Metered {
        bucket,
        kind,
        copy_source,
    })
}

/// The bytes a request's head says its body has.
pub(crate) fn uploaded(request: &Request<Incoming>) -> u64 {
    request
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

/// An answer's body, counted as it is sent: its bytes and, once it ends
/// (or is dropped, its client gone), the request's total latency.
pub(crate) struct MeteredBody {
    inner: Body,
    counters: Arc<BucketRequests>,
    started: Instant,
    sent: u64,
    done: bool,
}

impl MeteredBody {
    pub(crate) fn new(inner: Body, counters: Arc<BucketRequests>, started: Instant) -> Self {
        MeteredBody {
            inner,
            counters,
            started,
            sent: 0,
            done: false,
        }
    }

    fn finish(&mut self) {
        if !self.done {
            self.done = true;
            self.counters.finished(self.sent, self.started.elapsed());
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
