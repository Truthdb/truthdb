//! Async frame I/O for the replication protocol: reads and writes a [`ReplFrame`]
//! over any `AsyncRead`/`AsyncWrite` (a plain TCP stream or a TLS-wrapped one).
//! Generic over the stream so the same codec serves the listener, the senders,
//! and the receiver; the payload length is bounded [`REPL_MAX_PAYLOAD`] and
//! checked BEFORE the read buffer is allocated, so a peer cannot drive an
//! out-of-memory allocation with a bogus header.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{REPL_HEADER_LEN, REPL_MAX_PAYLOAD, ReplFrame, ReplMsgType, ReplProtoError};

/// Encodes and writes a whole frame (header + payload), then flushes.
pub async fn write_repl_frame<W>(w: &mut W, frame: &ReplFrame) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if frame.payload.len() > REPL_MAX_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            ReplProtoError::TooLarge,
        ));
    }
    let mut header = [0u8; REPL_HEADER_LEN];
    header[0..4].copy_from_slice(&(frame.payload.len() as u32).to_be_bytes());
    header[4..6].copy_from_slice(&(frame.msg_type as u16).to_be_bytes());
    header[6..8].copy_from_slice(&frame.flags.to_be_bytes());
    w.write_all(&header).await?;
    w.write_all(&frame.payload).await?;
    w.flush().await?;
    Ok(())
}

/// Reads one frame: the 8-byte header, then exactly `payload_len` bytes. The
/// declared length is bounded before allocating, and an unknown message type is
/// rejected. A clean EOF at a frame boundary surfaces as `UnexpectedEof` from
/// `read_exact`.
pub async fn read_repl_frame<R>(r: &mut R) -> io::Result<ReplFrame>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; REPL_HEADER_LEN];
    r.read_exact(&mut header).await?;
    let len = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
    if len > REPL_MAX_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            ReplProtoError::TooLarge,
        ));
    }
    let msg_type = ReplMsgType::try_from(u16::from_be_bytes(header[4..6].try_into().unwrap()))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let flags = u16::from_be_bytes(header[6..8].try_into().unwrap());
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    Ok(ReplFrame {
        msg_type,
        flags,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::{Hello, LogData, REPL_PROTOCOL_VERSION};

    /// Writes a frame into one end of an in-memory duplex and reads it from the
    /// other — the round-trip a real socket performs. The write runs in a
    /// separate task so a payload larger than the duplex buffer streams through
    /// concurrently with the read (a single task that writes-then-reads would
    /// deadlock once the buffer fills).
    async fn duplex_roundtrip(frame: ReplFrame) -> ReplFrame {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let writer = tokio::spawn(async move {
            write_repl_frame(&mut a, &frame).await.expect("write");
        });
        let got = read_repl_frame(&mut b).await.expect("read");
        writer.await.expect("writer task");
        got
    }

    #[tokio::test]
    async fn a_frame_round_trips_over_a_duplex() {
        let hello = Hello {
            protocol_version: REPL_PROTOCOL_VERSION,
            node_id: 11,
            cluster_uuid: [4u8; 16],
            epoch: 1,
            last_received_lsn: 4096,
            auth: vec![5, 6, 7],
        };
        let frame = ReplFrame::encode(ReplMsgType::Hello, &hello).unwrap();
        let back = duplex_roundtrip(frame.clone()).await;
        assert_eq!(back.msg_type, ReplMsgType::Hello);
        assert_eq!(back.payload, frame.payload);
        let decoded: Hello = back.decode().unwrap();
        assert_eq!(decoded.node_id, 11);
    }

    #[tokio::test]
    async fn a_large_log_data_frame_round_trips() {
        // A payload larger than the duplex buffer forces multiple read/write
        // chunks through read_exact/write_all.
        let bytes: Vec<u8> = (0..50_000u32).map(|i| (i % 256) as u8).collect();
        let frame = ReplFrame::encode(
            ReplMsgType::LogData,
            &LogData {
                from_lsn: 8192,
                bytes: bytes.clone(),
            },
        )
        .unwrap();
        let back = duplex_roundtrip(frame).await;
        let decoded: LogData = back.decode().unwrap();
        assert_eq!(decoded.from_lsn, 8192);
        assert_eq!(decoded.bytes, bytes);
    }

    #[tokio::test]
    async fn several_frames_stream_in_order() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let types = [
            ReplMsgType::Heartbeat,
            ReplMsgType::FlushAck,
            ReplMsgType::Heartbeat,
        ];
        for t in types {
            let f = ReplFrame {
                msg_type: t,
                flags: 0,
                payload: vec![],
            };
            write_repl_frame(&mut a, &f).await.unwrap();
        }
        for t in types {
            let f = read_repl_frame(&mut b).await.unwrap();
            assert_eq!(f.msg_type, t);
        }
    }

    #[tokio::test]
    async fn a_closed_stream_at_a_frame_boundary_is_eof() {
        let (a, mut b) = tokio::io::duplex(64);
        drop(a); // no bytes written, writer closed
        let err = read_repl_frame(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn an_oversized_declared_length_is_rejected_before_allocating() {
        let (mut a, mut b) = tokio::io::duplex(64);
        // Hand-write a header claiming more than the cap, then close.
        let mut header = [0u8; REPL_HEADER_LEN];
        header[0..4].copy_from_slice(&((REPL_MAX_PAYLOAD + 1) as u32).to_be_bytes());
        header[4..6].copy_from_slice(&(ReplMsgType::LogData as u16).to_be_bytes());
        write_repl_frame_raw(&mut a, &header).await;
        drop(a);
        let err = read_repl_frame(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// Writes raw header bytes (bypassing the encoder) for the malformed-input
    /// test.
    async fn write_repl_frame_raw<W: AsyncWrite + Unpin>(w: &mut W, header: &[u8]) {
        w.write_all(header).await.unwrap();
        w.flush().await.unwrap();
    }
}
