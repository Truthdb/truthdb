//! The admin listener (`[admin]`): `/metrics` in Prometheus' text format
//! (the S3 API's requests and storage usage per bucket, the log, the object
//! store's queues, the journal consumers' lag), `/healthz` (the process
//! answers) and `/readyz` (the database is usable). With a token set,
//! `/metrics` needs it as a bearer token.

use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::watch;
use truthdb_core::storage::{Storage, StorageError};
use truthdb_engine::metrics::{Histogram, RequestMetrics};
use truthdb_object::ObjFile;

use crate::server::{Body, full};

/// Connections served at once (scrapers and probes are few).
const MAX_CONNECTIONS: usize = 64;
/// How long a connection may take to send its request's head.
const HEAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// What the admin listener reads.
pub struct AdminState {
    pub storage: Arc<Storage>,
    pub request_metrics: Arc<RequestMetrics>,
    /// The bearer token `/metrics` needs, if any.
    pub token: Option<String>,
}

/// Serves the admin endpoints on `listener` until `shutdown` turns true.
pub async fn serve(
    listener: TcpListener,
    state: Arc<AdminState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    loop {
        let slot = tokio::select! {
            slot = Arc::clone(&slots).acquire_owned() => slot.expect("the semaphore is never closed"),
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
                continue;
            }
        };
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
                continue;
            }
        };
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let state = Arc::clone(&state);
                async move { Ok::<_, Infallible>(handle(&state, request).await) }
            });
            let _ = http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(HEAD_TIMEOUT)
                .serve_connection(TokioIo::new(stream), service)
                .await;
            drop(slot);
        });
    }
}

fn text(status: StatusCode, body: impl Into<Bytes>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(full(body.into()))
        .expect("a valid response")
}

async fn handle(state: &Arc<AdminState>, request: Request<Incoming>) -> Response<Body> {
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n");
    }
    match request.uri().path() {
        "/healthz" => text(StatusCode::OK, "ok\n"),
        "/readyz" => {
            let storage = Arc::clone(&state.storage);
            let ready = tokio::task::spawn_blocking(move || {
                let usable = storage.lock().ensure_rel_usable();
                usable.map(|()| {
                    if storage.is_standby() {
                        "standby"
                    } else {
                        "primary"
                    }
                })
            })
            .await;
            match ready {
                Ok(Ok(role)) => text(StatusCode::OK, format!("ready: {role}\n")),
                Ok(Err(err)) => text(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("not ready: {err}\n"),
                ),
                Err(err) => text(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("not ready: {err}\n"),
                ),
            }
        }
        "/metrics" => {
            if !authorized(state, &request) {
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header("www-authenticate", "Bearer")
                    .body(full(Bytes::from_static(b"unauthorized\n")))
                    .expect("a valid response");
            }
            let state = Arc::clone(state);
            match tokio::task::spawn_blocking(move || render(&state)).await {
                Ok(Ok(page)) => Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                    .body(full(page))
                    .expect("a valid response"),
                Ok(Err(err)) => text(StatusCode::SERVICE_UNAVAILABLE, format!("{err}\n")),
                Err(err) => text(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")),
            }
        }
        _ => text(StatusCode::NOT_FOUND, "not found\n"),
    }
}

/// Whether `request` carries the token, when one is set (compared by
/// digest: how long the comparison takes tells nothing of the token).
fn authorized(state: &AdminState, request: &Request<Incoming>) -> bool {
    let Some(token) = &state.token else {
        return true;
    };
    let digest = |text: &str| ring::digest::digest(&ring::digest::SHA256, text.as_bytes());
    request
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|given| digest(given).as_ref() == digest(token).as_ref())
}

/// A label value as Prometheus' text format writes it.
fn label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Writes a metric family's HELP and TYPE lines.
fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

/// Writes `histogram` (milliseconds) as a Prometheus histogram in seconds.
fn histogram(out: &mut String, name: &str, labels: &str, histogram: &Histogram) {
    let mut cumulative = 0;
    for (bound, count) in histogram.counts() {
        cumulative += count;
        let le = bound.map_or("+Inf".to_string(), |bound| {
            format!("{}", bound as f64 / 1000.0)
        });
        let _ = writeln!(out, "{name}_bucket{{{labels},le=\"{le}\"}} {cumulative}");
    }
    let _ = writeln!(
        out,
        "{name}_sum{{{labels}}} {}",
        histogram.sum().as_secs_f64()
    );
    let _ = writeln!(out, "{name}_count{{{labels}}} {cumulative}");
}

/// The metrics page.
fn render(state: &AdminState) -> Result<String, StorageError> {
    let mut out = String::new();
    let requests = state.request_metrics.snapshot();
    family(
        &mut out,
        "truthdb_s3_requests_total",
        "counter",
        "S3 requests to a bucket since the server started.",
    );
    for (bucket, counters) in &requests {
        let _ = writeln!(
            out,
            "truthdb_s3_requests_total{{bucket=\"{}\"}} {}",
            label(bucket),
            counters.counts().all
        );
    }
    family(
        &mut out,
        "truthdb_s3_requests_by_kind_total",
        "counter",
        "S3 requests to a bucket by kind, as S3's request metrics count them.",
    );
    for (bucket, counters) in &requests {
        let counts = counters.counts();
        for (kind, count) in [
            ("get", counts.get),
            ("put", counts.put),
            ("delete", counts.delete),
            ("head", counts.head),
            ("post", counts.post),
            ("list", counts.list),
        ] {
            let _ = writeln!(
                out,
                "truthdb_s3_requests_by_kind_total{{bucket=\"{}\",kind=\"{kind}\"}} {count}",
                label(bucket)
            );
        }
    }
    for (name, help, read) in [
        (
            "truthdb_s3_bytes_downloaded_total",
            "Bytes of the answers' bodies sent.",
            (|counts: &truthdb_engine::metrics::RequestCounts| counts.bytes_downloaded)
                as fn(&truthdb_engine::metrics::RequestCounts) -> u64,
        ),
        (
            "truthdb_s3_bytes_uploaded_total",
            "Bytes of the requests' payloads received.",
            |counts| counts.bytes_uploaded,
        ),
    ] {
        family(&mut out, name, "counter", help);
        for (bucket, counters) in &requests {
            let _ = writeln!(
                out,
                "{name}{{bucket=\"{}\"}} {}",
                label(bucket),
                read(&counters.counts())
            );
        }
    }
    family(
        &mut out,
        "truthdb_s3_errors_total",
        "counter",
        "S3 answers with a 4xx or 5xx status.",
    );
    for (bucket, counters) in &requests {
        let counts = counters.counts();
        for (class, count) in [("4xx", counts.errors_4xx), ("5xx", counts.errors_5xx)] {
            let _ = writeln!(
                out,
                "truthdb_s3_errors_total{{bucket=\"{}\",class=\"{class}\"}} {count}",
                label(bucket)
            );
        }
    }
    for (name, help, pick) in [
        (
            "truthdb_s3_first_byte_latency_seconds",
            "Time until an answer starts.",
            true,
        ),
        (
            "truthdb_s3_total_latency_seconds",
            "Time until an answer's last byte is sent.",
            false,
        ),
    ] {
        family(&mut out, name, "histogram", help);
        for (bucket, counters) in &requests {
            let labels = format!("bucket=\"{}\"", label(bucket));
            let picked = if pick {
                &counters.first_byte
            } else {
                &counters.total
            };
            histogram(&mut out, name, &labels, picked);
        }
    }

    // The log.
    let storage = &state.storage;
    family(
        &mut out,
        "truthdb_wal_usage_ratio",
        "gauge",
        "The share of the log ring in use.",
    );
    let _ = writeln!(out, "truthdb_wal_usage_ratio {}", storage.wal_usage_ratio());
    for (name, help, value) in [
        (
            "truthdb_wal_head_lsn",
            "The log's reclaim floor.",
            storage.wal_head(),
        ),
        (
            "truthdb_wal_tail_lsn",
            "The log's write position.",
            storage.wal_tail(),
        ),
        (
            "truthdb_wal_flushed_lsn",
            "The log's durable position.",
            storage.wal_flushed_lsn(),
        ),
    ] {
        family(&mut out, name, "gauge", help);
        let _ = writeln!(out, "{name} {value}");
    }
    family(
        &mut out,
        "truthdb_standby",
        "gauge",
        "1 on a replication standby.",
    );
    let _ = writeln!(out, "truthdb_standby {}", u8::from(storage.is_standby()));

    // The object store, read under one storage-lock hold.
    let mut guard = storage.lock();
    let Some(mut file) = ObjFile::try_new(&mut guard) else {
        return Ok(out);
    };
    let backlogs = file.backlogs()?;
    family(
        &mut out,
        "truthdb_s3_gc_backlog_bodies",
        "gauge",
        "Bodies held by nothing, waiting to be freed.",
    );
    let _ = writeln!(out, "truthdb_s3_gc_backlog_bodies {}", backlogs.collect);
    family(
        &mut out,
        "truthdb_s3_pack_backlog_bodies",
        "gauge",
        "Bodies of cold storage classes waiting to be packed.",
    );
    let _ = writeln!(out, "truthdb_s3_pack_backlog_bodies {}", backlogs.pack);
    let last = file.last_change()?;
    family(
        &mut out,
        "truthdb_s3_journal_consumer_lag",
        "gauge",
        "Change journal rows a consumer has yet to apply.",
    );
    for consumer in file.journal_consumers()? {
        let _ = writeln!(
            out,
            "truthdb_s3_journal_consumer_lag{{consumer=\"{}\"}} {}",
            label(&consumer.name),
            last.saturating_sub(consumer.cursor)
        );
    }
    let usage = file.bucket_usage()?;
    for (name, help, read) in [
        (
            "truthdb_s3_bucket_objects",
            "Versions, delete markers or upload parts a bucket holds.",
            (|usage: &truthdb_object::BucketUsage| usage.objects)
                as fn(&truthdb_object::BucketUsage) -> u64,
        ),
        (
            "truthdb_s3_bucket_bytes",
            "Their bytes as written.",
            |usage| usage.bytes,
        ),
    ] {
        family(&mut out, name, "gauge", help);
        for row in &usage {
            let _ = writeln!(
                out,
                "{name}{{bucket=\"{}\",kind=\"{}\",storage_class=\"{}\"}} {}",
                label(&row.bucket),
                row.kind.as_str(),
                row.storage_class.as_str(),
                read(row)
            );
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_are_escaped_and_histograms_cumulative_in_seconds() {
        assert_eq!(label("a\\b\"c\nd"), "a\\\\b\\\"c\\nd");
        let latencies = Histogram::default();
        for ms in [0, 3, 3, 5_000_000] {
            latencies.record(std::time::Duration::from_millis(ms));
        }
        let mut out = String::new();
        histogram(&mut out, "x_seconds", "bucket=\"b\"", &latencies);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "x_seconds_bucket{bucket=\"b\",le=\"0.001\"} 1");
        assert_eq!(lines[1], "x_seconds_bucket{bucket=\"b\",le=\"0.002\"} 1");
        assert_eq!(lines[2], "x_seconds_bucket{bucket=\"b\",le=\"0.004\"} 3");
        assert!(lines.contains(&"x_seconds_bucket{bucket=\"b\",le=\"1048.576\"} 3"));
        assert!(lines.contains(&"x_seconds_bucket{bucket=\"b\",le=\"+Inf\"} 4"));
        assert!(lines.contains(&"x_seconds_sum{bucket=\"b\"} 5000.006"));
        assert_eq!(lines.last(), Some(&"x_seconds_count{bucket=\"b\"} 4"));
    }
}
