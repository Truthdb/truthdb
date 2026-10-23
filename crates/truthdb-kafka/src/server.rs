//! The listener: TLS connections, each a sequence of size-prefixed
//! requests answered in order.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::api::{self, Auth, KafkaState, Outcome};
use crate::wire::Reader;

/// Connections served at once; more wait in the accept queue.
const MAX_CONNECTIONS: usize = 1024;
/// Connections from one address at once (Kafka's
/// `max.connections.per.ip`); more are closed when accepted, so one host
/// cannot hold every slot.
const MAX_CONNECTIONS_PER_IP: usize = 64;
/// How long a connection has for its TLS handshake and its SASL
/// authentication.
const AUTHENTICATION_TIMEOUT: Duration = Duration::from_secs(30);
/// How long an authenticated connection may stay silent (Kafka's
/// `connections.max.idle.ms`).
const IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// A request's size at most, before and after authentication (a consumer's
/// requests are small).
const MAX_UNAUTHENTICATED_REQUEST: usize = 64 << 10;
const MAX_REQUEST: usize = 1 << 20;
/// How often members past their session timeout are dropped.
const EXPIRY_TICK: Duration = Duration::from_millis(500);

/// Serves Kafka clients on `listener` over TLS until `shutdown` turns
/// true.
pub async fn serve(
    listener: TcpListener,
    state: Arc<KafkaState>,
    tls: tokio_rustls::TlsAcceptor,
    mut shutdown: watch::Receiver<bool>,
) {
    let expiry = {
        let state = Arc::clone(&state);
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(EXPIRY_TICK);
            loop {
                tokio::select! {
                    _ = tick.tick() => state.coordinator.expire(std::time::Instant::now()),
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            return;
                        }
                    }
                }
            }
        })
    };
    let slots = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    let per_ip: Arc<Mutex<HashMap<IpAddr, usize>>> = Arc::default();
    loop {
        let slot = tokio::select! {
            slot = Arc::clone(&slots).acquire_owned() => slot.expect("the semaphore is never closed"),
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
                continue;
            }
        };
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(_) => {
                    // Out of descriptors, say: back off instead of spinning.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
                continue;
            }
        };
        let (stream, peer) = stream;
        let Some(held) = PerIp::take(&per_ip, peer.ip()) else {
            continue;
        };
        let state = Arc::clone(&state);
        let tls = tls.clone();
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + AUTHENTICATION_TIMEOUT;
            if let Ok(Ok(stream)) = tokio::time::timeout_at(deadline, tls.accept(stream)).await {
                serve_connection(stream, &state, deadline).await;
            }
            drop(held);
            drop(slot);
        });
    }
    let _ = expiry.await;
}

/// A connection counted against its address's share; released when
/// dropped.
struct PerIp {
    counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
    ip: IpAddr,
}

impl PerIp {
    /// Counts a connection from `ip`, unless it has its share already.
    fn take(counts: &Arc<Mutex<HashMap<IpAddr, usize>>>, ip: IpAddr) -> Option<PerIp> {
        let mut map = counts.lock().expect("connection counts poisoned");
        let count = map.entry(ip).or_default();
        if *count >= MAX_CONNECTIONS_PER_IP {
            return None;
        }
        *count += 1;
        Some(PerIp {
            counts: Arc::clone(counts),
            ip,
        })
    }
}

impl Drop for PerIp {
    fn drop(&mut self) {
        let mut map = self.counts.lock().expect("connection counts poisoned");
        if let Some(count) = map.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                map.remove(&self.ip);
            }
        }
    }
}

/// Answers a connection's requests until it closes, breaks the protocol,
/// fails to authenticate by `deadline`, or idles too long.
async fn serve_connection<I>(mut io: I, state: &KafkaState, deadline: tokio::time::Instant)
where
    I: AsyncRead + AsyncWrite + Unpin,
{
    let mut auth = Auth::Start;
    loop {
        let authenticated = matches!(auth, Auth::Done(_));
        let (limit, until) = if authenticated {
            (MAX_REQUEST, tokio::time::Instant::now() + IDLE_TIMEOUT)
        } else {
            (MAX_UNAUTHENTICATED_REQUEST, deadline)
        };
        let Some(request) = read_request(&mut io, limit, until).await else {
            return;
        };
        let mut reader = Reader::new(&request);
        let Ok(header) = api::read_header(&mut reader) else {
            return;
        };
        let (answer, close) = match api::handle(state, &mut auth, &header, reader.rest()).await {
            Outcome::Answer(answer) => (answer, false),
            Outcome::AnswerAndClose(answer) => (answer, true),
            Outcome::Close => return,
        };
        let mut frame = Vec::with_capacity(8 + answer.len());
        frame.extend_from_slice(&((4 + answer.len()) as i32).to_be_bytes());
        frame.extend_from_slice(&header.correlation_id.to_be_bytes());
        frame.extend_from_slice(&answer);
        if io.write_all(&frame).await.is_err() || io.flush().await.is_err() || close {
            return;
        }
    }
}

async fn read_request<I>(io: &mut I, limit: usize, until: tokio::time::Instant) -> Option<Vec<u8>>
where
    I: AsyncRead + Unpin,
{
    tokio::time::timeout_at(until, async {
        let mut size = [0u8; 4];
        io.read_exact(&mut size).await.ok()?;
        let size = usize::try_from(i32::from_be_bytes(size)).ok()?;
        if size > limit {
            return None;
        }
        let mut request = vec![0u8; size];
        io.read_exact(&mut request).await.ok()?;
        Some(request)
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_address_holds_at_most_its_share_of_connections() {
        let counts: Arc<Mutex<HashMap<IpAddr, usize>>> = Arc::default();
        let (one, two): (IpAddr, IpAddr) =
            ("10.0.0.1".parse().unwrap(), "10.0.0.2".parse().unwrap());
        let mut held: Vec<PerIp> = (0..MAX_CONNECTIONS_PER_IP)
            .map(|_| PerIp::take(&counts, one).expect("within the share"))
            .collect();
        assert!(PerIp::take(&counts, one).is_none(), "past the share");
        assert!(PerIp::take(&counts, two).is_some(), "another address");
        held.pop();
        assert!(
            PerIp::take(&counts, one).is_some(),
            "a closed connection frees one"
        );
        drop(held);
        assert!(counts.lock().unwrap().is_empty(), "every count released");
    }
}
