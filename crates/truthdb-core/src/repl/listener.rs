//! The primary's replication listener: a tokio accept loop (mirroring
//! `client_listener`) that TLS-wraps each standby connection and runs the
//! [`serve_handshake`] exchange. TLS is completed BEFORE the handshake — the
//! shared-secret proof is never evaluated on a plaintext socket. On acceptance
//! the connection is ready for the sender to stream log over (slice 4c); until
//! that lands, an accepted connection simply completes its handshake.

use std::io;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

use super::handshake::HandshakeParams;
use super::server::{AcceptedStandby, serve_handshake};

/// The primary's replication identity + secret, shared across all standby
/// connections. Cheap to clone (the secret and the LSN source are `Arc`s).
#[derive(Clone)]
pub struct PrimaryReplContext {
    /// The cluster's shared secret — verified against each standby's HMAC proof.
    pub shared_secret: Arc<Vec<u8>>,
    /// This cluster's uuid.
    pub cluster_uuid: [u8; 16],
    /// The primary's replication epoch (bumped on promotion).
    pub epoch: u64,
    /// Reads the primary's current durable WAL tail — the ship ceiling the
    /// HelloAck advertises. Called fresh per connection so it reflects live
    /// progress.
    pub flushed_lsn: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl PrimaryReplContext {
    fn params(&self, primary_flushed_lsn: u64) -> HandshakeParams<'_> {
        HandshakeParams {
            shared_secret: &self.shared_secret,
            cluster_uuid: self.cluster_uuid,
            primary_epoch: self.epoch,
            primary_flushed_lsn,
        }
    }
}

/// Accepts standby connections until `shutdown` flips, TLS-wrapping and
/// handshaking each in its own task. Per-connection failures are isolated (a bad
/// standby never stops the loop).
pub async fn run_repl_listener(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    ctx: PrimaryReplContext,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            accepted = listener.accept() => {
                let (tcp, _peer) = match accepted {
                    Ok(pair) => pair,
                    Err(_) => continue,
                };
                let acceptor = acceptor.clone();
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    let _ = handle_standby(tcp, acceptor, ctx).await;
                });
            }
        }
    }
}

/// Completes the TLS handshake, then the replication handshake, over one
/// connection. Returns the accepted standby (for the caller/sender to stream to)
/// or `None` if the handshake was rejected.
async fn handle_standby(
    tcp: tokio::net::TcpStream,
    acceptor: TlsAcceptor,
    ctx: PrimaryReplContext,
) -> io::Result<Option<AcceptedStandby>> {
    // TLS FIRST — the shared-secret handshake never runs on a plaintext socket.
    let mut tls = acceptor.accept(tcp).await?;
    let flushed = (ctx.flushed_lsn)();
    let params = ctx.params(flushed);
    serve_handshake(&mut tls, &params).await
    // On accept the stream is ready for the sender (4c); dropping it here (until
    // the sender lands) closes the connection after the handshake.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::framing::{read_repl_frame, write_repl_frame};
    use crate::repl::handshake::compute_auth;
    use crate::repl::tls::{client_config_trusting, server_config_from_pem};
    use crate::repl::{Hello, HelloAck, REPL_PROTOCOL_VERSION, ReplFrame, ReplMsgType};

    fn self_signed() -> (String, String) {
        let c = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        (c.cert.pem(), c.key_pair.serialize_pem())
    }

    const SECRET: &[u8] = b"listener-secret";
    const UUID: [u8; 16] = [6u8; 16];

    /// Spins up the listener on an ephemeral port with a self-signed cert, then
    /// connects a real TLS standby that sends `hello` and returns the ack it
    /// gets. `flushed` is what the primary reports as its durable tail.
    async fn connect_and_handshake(hello: Hello, flushed: u64) -> HelloAck {
        let (cert_pem, key_pem) = self_signed();
        let acceptor = TlsAcceptor::from(
            server_config_from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap(),
        );
        let connector =
            tokio_rustls::TlsConnector::from(client_config_trusting(cert_pem.as_bytes()).unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let ctx = PrimaryReplContext {
            shared_secret: Arc::new(SECRET.to_vec()),
            cluster_uuid: UUID,
            epoch: 2,
            flushed_lsn: Arc::new(move || flushed),
        };
        let server = tokio::spawn(run_repl_listener(listener, acceptor, ctx, shutdown_rx));

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let domain = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(domain, tcp).await.unwrap();
        write_repl_frame(
            &mut tls,
            &ReplFrame::encode(ReplMsgType::Hello, &hello).unwrap(),
        )
        .await
        .unwrap();
        let ack: HelloAck = read_repl_frame(&mut tls).await.unwrap().decode().unwrap();

        shutdown_tx.send(true).unwrap();
        let _ = server.await;
        ack
    }

    #[tokio::test]
    async fn the_listener_accepts_a_valid_standby_over_tls() {
        let auth = compute_auth(SECRET, 9, &UUID, 1, 40_000);
        let hello = Hello {
            protocol_version: REPL_PROTOCOL_VERSION,
            node_id: 9,
            cluster_uuid: UUID,
            epoch: 1,
            last_received_lsn: 40_000,
            auth,
        };
        let ack = connect_and_handshake(hello, 55_000).await;
        assert!(ack.accepted, "{}", ack.message);
        assert_eq!(
            ack.primary_flushed_lsn, 55_000,
            "the ack advertises the live durable tail"
        );
    }

    #[tokio::test]
    async fn the_listener_rejects_a_bad_secret_over_tls() {
        let auth = compute_auth(b"wrong-secret", 9, &UUID, 1, 40_000);
        let hello = Hello {
            protocol_version: REPL_PROTOCOL_VERSION,
            node_id: 9,
            cluster_uuid: UUID,
            epoch: 1,
            last_received_lsn: 40_000,
            auth,
        };
        let ack = connect_and_handshake(hello, 55_000).await;
        assert!(!ack.accepted);
        assert_eq!(ack.message, "replication handshake rejected");
    }
}
