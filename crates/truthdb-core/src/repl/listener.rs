//! The primary's replication listener: a tokio accept loop (mirroring
//! `client_listener`) that TLS-wraps each standby connection, runs the
//! [`serve_handshake`] exchange, and on acceptance hands the connection to the
//! per-standby [`run_sender`] task, which registers the standby's slot and
//! streams log until the connection drops. TLS is completed BEFORE the
//! handshake — the shared-secret proof is never evaluated on a plaintext
//! socket.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

use super::handshake::HandshakeParams;
use super::sender::run_sender;
use super::server::serve_handshake;
use crate::storage::Storage;

/// The primary's replication identity + secret, shared across all standby
/// connections. Cheap to clone (`Arc`s). The epoch and durable watermark are
/// read from `storage` fresh per connection, so they track live progress.
#[derive(Clone)]
pub struct PrimaryReplContext {
    /// The cluster's shared secret — verified against each standby's HMAC proof.
    pub shared_secret: Arc<Vec<u8>>,
    /// This cluster's uuid.
    pub cluster_uuid: [u8; 16],
    /// The primary's live storage: the handshake reads its epoch and durable
    /// watermark, and the sender ships its WAL and drives its slots.
    pub storage: Arc<Storage>,
    /// The sender's idle heartbeat interval (also bounds how stale a
    /// direct-sync watermark advance can go unshipped).
    pub heartbeat: Duration,
}

impl PrimaryReplContext {
    fn params(&self, primary_flushed_lsn: u64) -> HandshakeParams<'_> {
        HandshakeParams {
            shared_secret: &self.shared_secret,
            cluster_uuid: self.cluster_uuid,
            primary_epoch: self.storage.epoch(),
            primary_flushed_lsn,
        }
    }
}

/// Accepts standby connections until `shutdown` flips, TLS-wrapping,
/// handshaking and then serving each in its own task. Per-connection failures
/// are isolated (a bad standby never stops the loop).
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
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_standby(tcp, acceptor, ctx, shutdown).await {
                        eprintln!("replication sender: {err}");
                    }
                });
            }
        }
    }
}

/// Completes the TLS handshake, then the replication handshake, then streams
/// log to the accepted standby until the connection ends. A rejected handshake
/// returns `Ok` (the refusal was the response).
async fn handle_standby(
    tcp: tokio::net::TcpStream,
    acceptor: TlsAcceptor,
    ctx: PrimaryReplContext,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    // TLS FIRST — the shared-secret handshake never runs on a plaintext socket.
    let mut tls = acceptor.accept(tcp).await?;
    let flushed = ctx.storage.wal_flushed_lsn();
    let params = ctx.params(flushed);
    match serve_handshake(&mut tls, &params).await? {
        Some(standby) => run_sender(tls, ctx.storage, standby, ctx.heartbeat, shutdown).await,
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::framing::{read_repl_frame, write_repl_frame};
    use crate::repl::handshake::compute_auth;
    use crate::repl::tls::{client_config_trusting, server_config_from_pem};
    use crate::repl::{Hello, HelloAck, REPL_PROTOCOL_VERSION, ReplFrame, ReplMsgType};
    use crate::storage::{Storage, StorageOptions};

    fn self_signed() -> (String, String) {
        let c = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        (c.cert.pem(), c.key_pair.serialize_pem())
    }

    const SECRET: &[u8] = b"listener-secret";
    const UUID: [u8; 16] = [6u8; 16];

    fn temp_storage(label: &str) -> (Arc<Storage>, std::path::PathBuf) {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "truthdb-repl-listener-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let storage = Storage::create(
            path.clone(),
            StorageOptions {
                size_gib: 1,
                wal_ratio: 0.05,
                metadata_ratio: 0.08,
                snapshot_ratio: 0.02,
                allocator_ratio: 0.02,
                reserved_ratio: 0.17,
                default_collation: None,
            },
        )
        .unwrap();
        (Arc::new(storage), path)
    }

    /// Spins up the listener on an ephemeral port with a self-signed cert and a
    /// real (empty) storage, then connects a real TLS standby that sends
    /// `hello` and returns the ack it gets.
    async fn connect_and_handshake(hello: Hello) -> HelloAck {
        let (cert_pem, key_pem) = self_signed();
        let acceptor = TlsAcceptor::from(
            server_config_from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap(),
        );
        let connector =
            tokio_rustls::TlsConnector::from(client_config_trusting(cert_pem.as_bytes()).unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (storage, path) = temp_storage("handshake");
        let ctx = PrimaryReplContext {
            shared_secret: Arc::new(SECRET.to_vec()),
            cluster_uuid: UUID,
            storage,
            heartbeat: Duration::from_secs(30),
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
        let _ = std::fs::remove_file(path);
        ack
    }

    #[tokio::test]
    async fn the_listener_accepts_a_valid_standby_over_tls() {
        // A fresh (empty) primary storage: epoch 0, and the standby resumes at
        // the primary's current tail (nothing to catch up).
        let auth = compute_auth(SECRET, 9, &UUID, 0, 0);
        let hello = Hello {
            protocol_version: REPL_PROTOCOL_VERSION,
            node_id: 9,
            cluster_uuid: UUID,
            epoch: 0,
            last_received_lsn: 0,
            auth,
        };
        let ack = connect_and_handshake(hello).await;
        assert!(ack.accepted, "{}", ack.message);
    }

    #[tokio::test]
    async fn the_listener_rejects_a_bad_secret_over_tls() {
        let auth = compute_auth(b"wrong-secret", 9, &UUID, 0, 0);
        let hello = Hello {
            protocol_version: REPL_PROTOCOL_VERSION,
            node_id: 9,
            cluster_uuid: UUID,
            epoch: 0,
            last_received_lsn: 0,
            auth,
        };
        let ack = connect_and_handshake(hello).await;
        assert!(!ack.accepted);
        assert_eq!(ack.message, "replication handshake rejected");
    }
}
