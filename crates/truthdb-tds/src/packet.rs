//! TDS packet framing (MS-TDS 2.2.3.1).
//!
//! Every TDS message is a sequence of packets, each with an 8-byte header:
//!
//! ```text
//! type u8 | status u8 | length u16 (BE, incl. header) | spid u16 (BE) |
//! packet_id u8 | window u8
//! ```
//!
//! A message ends at the packet whose status has the EOM bit set. We read a
//! whole message (reassembling packets) into one payload buffer, and write a
//! response by splitting a payload into packets of the negotiated size.

use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PKT_SQL_BATCH: u8 = 0x01;
pub const PKT_RPC: u8 = 0x03;
pub const PKT_TABULAR_RESULT: u8 = 0x04;
pub const PKT_ATTENTION: u8 = 0x06;
pub const PKT_TRANSACTION_MANAGER: u8 = 0x0e;
pub const PKT_LOGIN7: u8 = 0x10;
pub const PKT_PRELOGIN: u8 = 0x12;

const HEADER_LEN: usize = 8;
const STATUS_EOM: u8 = 0x01;

/// Upper bound on a fully reassembled message. A peer controls the EOM bit, so
/// without a cap it could stream non-EOM packets forever and grow the payload
/// Vec until the process is OOM-killed (a pre-auth remote DoS, since the very
/// first read is PRELOGIN). 16 MiB is far larger than any legitimate LOGIN7 or
/// SQL batch yet bounds the damage.
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

pub const DEFAULT_PACKET_SIZE: usize = 4096;
/// Clamp for a client-negotiated packet size (MS-TDS allows 512..=32767).
pub const MIN_PACKET_SIZE: usize = 512;
pub const MAX_PACKET_SIZE: usize = 32_767;

/// A fully reassembled TDS message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub kind: u8,
    pub payload: Vec<u8>,
}

/// Reads one complete message (all packets up to and including EOM).
pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Message> {
    let mut header = [0u8; HEADER_LEN];
    reader.read_exact(&mut header).await?;
    let kind = header[0];
    let mut payload = read_packet_body(reader, &header).await?;
    let mut status = header[1];

    // Continue while EOM is not set. All packets of a message share the type.
    while status & STATUS_EOM == 0 {
        reader.read_exact(&mut header).await?;
        if header[0] != kind {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TDS message packets changed type mid-stream",
            ));
        }
        status = header[1];
        payload.extend(read_packet_body(reader, &header).await?);
        if payload.len() > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TDS message exceeds the maximum reassembled size",
            ));
        }
    }

    Ok(Message { kind, payload })
}

async fn read_packet_body<R: AsyncRead + Unpin>(
    reader: &mut R,
    header: &[u8; HEADER_LEN],
) -> io::Result<Vec<u8>> {
    let length = u16::from_be_bytes([header[2], header[3]]) as usize;
    if length < HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TDS packet length smaller than its header",
        ));
    }
    let body_len = length - HEADER_LEN;
    let mut body = vec![0u8; body_len];
    reader.read_exact(&mut body).await?;
    Ok(body)
}

/// Writes a message as one or more packets of `packet_size` (splitting the
/// payload at packet-data boundaries; the last packet carries EOM).
pub async fn write_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    kind: u8,
    payload: &[u8],
    packet_size: usize,
) -> io::Result<()> {
    let data_per_packet = packet_size.clamp(MIN_PACKET_SIZE, MAX_PACKET_SIZE) - HEADER_LEN;
    let mut packet_id: u8 = 1;
    let mut offset = 0;
    loop {
        let end = (offset + data_per_packet).min(payload.len());
        let is_last = end == payload.len();
        let chunk = &payload[offset..end];
        let length = (HEADER_LEN + chunk.len()) as u16;
        let header = [
            kind,
            if is_last { STATUS_EOM } else { 0 },
            (length >> 8) as u8,
            (length & 0xff) as u8,
            0,
            0, // SPID (server sends 0 in Stage 4)
            packet_id,
            0, // window
        ];
        writer.write_all(&header).await?;
        writer.write_all(chunk).await?;
        packet_id = packet_id.wrapping_add(1);
        offset = end;
        if is_last {
            break;
        }
    }
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_packet_round_trip() {
        let mut buf = Vec::new();
        write_message(&mut buf, PKT_SQL_BATCH, b"hello tds", DEFAULT_PACKET_SIZE)
            .await
            .unwrap();
        // Header then body.
        assert_eq!(buf[0], PKT_SQL_BATCH);
        assert_eq!(buf[1], STATUS_EOM);
        assert_eq!(u16::from_be_bytes([buf[2], buf[3]]), (8 + 9) as u16);

        let mut cursor = std::io::Cursor::new(buf);
        let message = read_message(&mut cursor).await.unwrap();
        assert_eq!(message.kind, PKT_SQL_BATCH);
        assert_eq!(message.payload, b"hello tds");
    }

    #[tokio::test]
    async fn multi_packet_reassembly() {
        // Force tiny packets so a 2000-byte payload spans several.
        let payload: Vec<u8> = (0..2000).map(|i| (i % 251) as u8).collect();
        let mut buf = Vec::new();
        write_message(&mut buf, PKT_TABULAR_RESULT, &payload, MIN_PACKET_SIZE)
            .await
            .unwrap();
        // More than one packet was emitted.
        assert!(buf.len() > payload.len() + HEADER_LEN);

        let mut cursor = std::io::Cursor::new(buf);
        let message = read_message(&mut cursor).await.unwrap();
        assert_eq!(message.kind, PKT_TABULAR_RESULT);
        assert_eq!(message.payload, payload);
    }

    #[tokio::test]
    async fn oversized_message_is_rejected() {
        // A peer that never sets EOM cannot grow the payload without bound.
        let body = vec![0u8; 65527];
        let mut buf = Vec::new();
        let packets = MAX_MESSAGE_SIZE / body.len() + 2;
        for _ in 0..packets {
            let length = (HEADER_LEN + body.len()) as u16;
            buf.push(PKT_SQL_BATCH);
            buf.push(0); // status: no EOM
            buf.extend_from_slice(&length.to_be_bytes());
            buf.extend_from_slice(&[0, 0, 0, 0]); // spid, packet_id, window
            buf.extend_from_slice(&body);
        }
        let mut cursor = std::io::Cursor::new(buf);
        let err = read_message(&mut cursor).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
