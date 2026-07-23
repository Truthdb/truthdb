//! The standby's replication receiver: a reconnecting client loop that dials
//! the primary's replication listener, completes TLS and the shared-secret
//! handshake, then applies every `LogData` frame via
//! [`Storage::apply_wal_stream`] and acknowledges each with a `FlushAck` (the
//! apply is durable before it returns, so received = flushed = applied). The
//! resume point is the standby's own persisted WAL tail — nothing is negotiated
//! or cached, so a crash or disconnect at any point resumes correctly (an
//! overlapping re-ship is idempotent).

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio_rustls::TlsConnector;

use super::framing::{read_repl_frame, write_repl_frame};
use super::handshake::compute_auth;
use super::tls::client_config_trusting;
use super::{FlushAck, Hello, HelloAck, LogData, REPL_PROTOCOL_VERSION, ReplFrame, ReplMsgType};
use crate::storage::Storage;

/// How a standby reaches and authenticates to its primary.
#[derive(Clone)]
pub struct ReceiverConfig {
    /// The primary's replication endpoint, `host:port`.
    pub primary_addr: String,
    /// The TLS server name the primary's certificate must match.
    pub server_name: String,
    /// PEM of the certificate (or CA) to trust for the primary.
    pub tls_ca_pem: Vec<u8>,
    /// The cluster's shared secret (the HMAC key for the handshake proof).
    pub shared_secret: Vec<u8>,
    /// This cluster's uuid — must match the primary's.
    pub cluster_uuid: [u8; 16],
    /// This standby's node id (also its replication-slot id on the primary).
    pub node_id: u32,
    /// Delay between reconnect attempts after a connection fails.
    pub reconnect_delay: Duration,
    /// How long the primary may stay completely silent (a healthy one
    /// heartbeats when idle) before the connection is presumed dead and
    /// re-dialed — a silent partition must not stall replication forever.
    pub stall_timeout: Duration,
}

/// Runs the standby's receive loop until `shutdown` flips: connect, stream,
/// and on any failure log it and reconnect after `reconnect_delay`. A
/// rejection notice from the primary (wrong secret, diverged timeline, slot
/// table full) is surfaced verbatim — most name the fix (e.g. reseed).
pub async fn run_standby_receiver(
    cfg: ReceiverConfig,
    storage: Arc<Storage>,
    mut shutdown: watch::Receiver<bool>,
) {
    // The seed must be `restore --standby`: a plain restore ran ARIES undo,
    // whose CLRs can mask shipped redo and silently diverge the replica. The
    // flag also keeps the file read-only from birth, so no local write can
    // slip in between opening and the first shipped frame.
    if !storage.is_standby() {
        eprintln!(
            "replication receiver refusing to start: this database is not a standby seed \
             (restore it with `truthdb-cli restore --standby`)"
        );
        return;
    }
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            res = connect_and_stream(&cfg, &storage) => {
                if let Err(err) = res {
                    eprintln!("replication receiver: {err}; reconnecting to {} in {:?}",
                        cfg.primary_addr, cfg.reconnect_delay);
                }
            }
        }
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = tokio::time::sleep(cfg.reconnect_delay) => {}
        }
    }
}

/// One connection's lifetime: dial, handshake, then apply frames until the
/// stream ends or fails.
async fn connect_and_stream(cfg: &ReceiverConfig, storage: &Arc<Storage>) -> io::Result<()> {
    let tcp = tokio::net::TcpStream::connect(&cfg.primary_addr).await?;
    let connector = TlsConnector::from(client_config_trusting(&cfg.tls_ca_pem)?);
    let domain = rustls::pki_types::ServerName::try_from(cfg.server_name.clone())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let mut tls = connector.connect(domain, tcp).await?;

    // Resume from the PERSISTED tail (see `standby_resume_lsn`): after a crash
    // between an apply's ring fsync and its superblock commit, or a redo-only
    // reopen that recovered extra durable ring bytes, the live tail runs ahead
    // of the persisted one — and the apply continuity check compares against
    // the persisted value, so resuming from the live tail would wedge every
    // reconnect on a 4305 gap. The overlap re-ship is idempotent.
    let last_received = storage.standby_resume_lsn();
    let epoch = storage.epoch();
    let hello = Hello {
        protocol_version: REPL_PROTOCOL_VERSION,
        node_id: u64::from(cfg.node_id),
        cluster_uuid: cfg.cluster_uuid,
        epoch,
        last_received_lsn: last_received,
        auth: compute_auth(
            &cfg.shared_secret,
            u64::from(cfg.node_id),
            &cfg.cluster_uuid,
            epoch,
            last_received,
        ),
    };
    let frame = ReplFrame::encode(ReplMsgType::Hello, &hello).map_err(io::Error::other)?;
    write_repl_frame(&mut tls, &frame).await?;
    let ack = read_ack(&mut tls).await?;
    if !ack.accepted {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!("primary rejected the handshake: {}", ack.message),
        ));
    }
    if ack.primary_epoch < epoch {
        // A primary behind this standby's epoch is a stale timeline (e.g. an
        // old primary that missed a failover). Following it would diverge.
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "primary epoch {} is behind this standby's epoch {epoch}: stale primary",
                ack.primary_epoch
            ),
        ));
    }

    stream_frames(&mut tls, storage, cfg.stall_timeout).await
}

async fn read_ack<S>(stream: &mut S) -> io::Result<HelloAck>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = read_repl_frame(stream).await?;
    if frame.msg_type != ReplMsgType::HelloAck {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected a HelloAck as the first frame",
        ));
    }
    frame.decode().map_err(io::Error::other)
}

async fn stream_frames<S>(stream: &mut S, storage: &Arc<Storage>, stall: Duration) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        // The primary heartbeats when idle; total silence past the deadline is
        // a dead peer or a silent partition — reconnect rather than stall
        // replication forever on a connection that will never speak again.
        let frame = tokio::time::timeout(stall, read_repl_frame(stream))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("primary sent nothing for {stall:?}"),
                )
            })??;
        match frame.msg_type {
            ReplMsgType::LogData => {
                let ld: LogData = frame.decode().map_err(io::Error::other)?;
                storage
                    .apply_wal_stream(ld.from_lsn, &ld.bytes)
                    .map_err(|err| io::Error::other(format!("apply failed: {err}")))?;
                ack_current(stream, storage).await?;
            }
            // A heartbeat keeps the connection verified while idle; ack it so
            // the primary's slot (and, later, lag metrics) track a quiet
            // standby too.
            ReplMsgType::Heartbeat => ack_current(stream, storage).await?,
            // A HelloAck after the handshake is the primary's rejection
            // notice (diverged timeline, slot table full, ...): fatal for
            // this connection, and the message names the operator fix.
            ReplMsgType::HelloAck => {
                let notice: HelloAck = frame.decode().map_err(io::Error::other)?;
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("primary refused the stream: {}", notice.message),
                ));
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected replication frame: {other:?}"),
                ));
            }
        }
    }
}

/// Acknowledges the standby's current PERSISTED watermark (`apply_wal_stream`
/// is durable before it returns, so received, flushed and applied coincide —
/// and the persisted value is what a restart resumes from, which is what the
/// primary's slot must hold log for).
async fn ack_current<S>(stream: &mut S, storage: &Arc<Storage>) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let tail = storage.standby_resume_lsn();
    let ack = FlushAck {
        received_lsn: tail,
        flushed_lsn: tail,
        applied_lsn: tail,
    };
    let frame = ReplFrame::encode(ReplMsgType::FlushAck, &ack).map_err(io::Error::other)?;
    write_repl_frame(stream, &frame).await
}
