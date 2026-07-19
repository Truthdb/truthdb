//! The primary side of a replication connection: after the transport has
//! established the (TLS-wrapped) stream, [`serve_handshake`] reads the standby's
//! [`Hello`], authenticates and fences it with [`evaluate_hello`], and answers
//! with a [`HelloAck`]. Generic over the stream, so it runs identically over a
//! plain stream (tests) and a TLS one (the listener). On acceptance it returns
//! the standby's identity so the caller can register its replication slot and
//! begin streaming log; on rejection it has already sent the negative ack.

use std::io;

use super::framing::{read_repl_frame, write_repl_frame};
use super::handshake::{HandshakeParams, evaluate_hello};
use super::{Hello, HelloAck, REPL_PROTOCOL_VERSION, ReplFrame, ReplMsgType};
use tokio::io::{AsyncRead, AsyncWrite};

/// A standby whose handshake the primary accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedStandby {
    pub node_id: u64,
    /// The LSN the standby has durably received — the primary resumes shipping
    /// from here.
    pub last_received_lsn: u64,
}

async fn write_ack<S>(stream: &mut S, ack: &HelloAck) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let frame = ReplFrame::encode(ReplMsgType::HelloAck, ack)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    write_repl_frame(stream, &frame).await
}

/// Runs the server side of the handshake. Returns `Ok(Some(standby))` when the
/// handshake succeeded (the positive ack has been sent), `Ok(None)` when it was
/// rejected (the negative ack has been sent), or an I/O error if the stream
/// failed.
pub async fn serve_handshake<S>(
    stream: &mut S,
    params: &HandshakeParams<'_>,
) -> io::Result<Option<AcceptedStandby>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = read_repl_frame(stream).await?;
    if frame.msg_type != ReplMsgType::Hello {
        // The first message must be a Hello — anything else is a protocol
        // violation, refused before any secret work.
        let ack = HelloAck {
            protocol_version: REPL_PROTOCOL_VERSION,
            accepted: false,
            primary_epoch: 0,
            primary_flushed_lsn: 0,
            message: "expected a Hello as the first message".to_string(),
        };
        write_ack(stream, &ack).await?;
        return Ok(None);
    }
    let hello: Hello = frame
        .decode()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let ack = evaluate_hello(&hello, params);
    let accepted = ack.accepted;
    let standby = AcceptedStandby {
        node_id: hello.node_id,
        last_received_lsn: hello.last_received_lsn,
    };
    write_ack(stream, &ack).await?;
    Ok(accepted.then_some(standby))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::handshake::compute_auth;

    const SECRET: &[u8] = b"cluster-secret";
    const UUID: [u8; 16] = [8u8; 16];

    fn params() -> HandshakeParams<'static> {
        HandshakeParams {
            shared_secret: SECRET,
            cluster_uuid: UUID,
            primary_epoch: 3,
            primary_flushed_lsn: 77_000,
        }
    }

    /// Drives one handshake: the primary runs `serve_handshake` on the server
    /// end while the test plays the standby on the client end, sending `hello`
    /// and reading the ack. Returns (server outcome, the ack the standby saw).
    async fn exchange(hello: Hello) -> (Option<AcceptedStandby>, HelloAck) {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_task =
            tokio::spawn(async move { serve_handshake(&mut server, &params()).await });
        let hello_frame = ReplFrame::encode(ReplMsgType::Hello, &hello).unwrap();
        write_repl_frame(&mut client, &hello_frame).await.unwrap();
        let ack: HelloAck = read_repl_frame(&mut client)
            .await
            .unwrap()
            .decode()
            .unwrap();
        let outcome = server_task.await.unwrap().expect("serve_handshake io");
        (outcome, ack)
    }

    fn hello(node_id: u64, epoch: u64, last_received_lsn: u64, auth: Vec<u8>) -> Hello {
        Hello {
            protocol_version: REPL_PROTOCOL_VERSION,
            node_id,
            cluster_uuid: UUID,
            epoch,
            last_received_lsn,
            auth,
        }
    }

    #[tokio::test]
    async fn a_valid_standby_is_accepted_over_the_wire() {
        let auth = compute_auth(SECRET, 5, &UUID, 1, 40_000);
        let (outcome, ack) = exchange(hello(5, 1, 40_000, auth)).await;
        assert!(ack.accepted, "{}", ack.message);
        assert_eq!(ack.primary_flushed_lsn, 77_000);
        assert_eq!(
            outcome,
            Some(AcceptedStandby {
                node_id: 5,
                last_received_lsn: 40_000,
            })
        );
    }

    #[tokio::test]
    async fn a_bad_secret_is_rejected_over_the_wire() {
        let auth = compute_auth(b"wrong", 5, &UUID, 1, 40_000);
        let (outcome, ack) = exchange(hello(5, 1, 40_000, auth)).await;
        assert!(!ack.accepted);
        assert_eq!(ack.message, "replication handshake rejected");
        assert_eq!(outcome, None);
    }

    /// The first frame must be a Hello; a Heartbeat first is refused.
    #[tokio::test]
    async fn a_non_hello_first_frame_is_refused() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_task =
            tokio::spawn(async move { serve_handshake(&mut server, &params()).await });
        let frame =
            ReplFrame::encode(ReplMsgType::Heartbeat, &super::super::Heartbeat::default()).unwrap();
        write_repl_frame(&mut client, &frame).await.unwrap();
        let ack: HelloAck = read_repl_frame(&mut client)
            .await
            .unwrap()
            .decode()
            .unwrap();
        assert!(!ack.accepted);
        assert_eq!(ack.message, "expected a Hello as the first message");
        assert_eq!(server_task.await.unwrap().unwrap(), None);
    }
}
