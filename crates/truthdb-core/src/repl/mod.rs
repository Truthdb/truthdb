//! Stage 18 replication wire protocol: the message types and their bincode +
//! framed codec. Pure data — no I/O; the async listener, the per-standby
//! senders and the standby receiver frame these over a TLS socket. Mirrors the
//! native `truthdb-proto` codec: an 8-byte big-endian header (payload length
//! `u32`, message type `u16`, flags `u16`) followed by the bincode payload.
//!
//! Message flow: the standby dials in and sends `Hello`; the primary answers
//! `HelloAck` and, on acceptance, streams `LogData` (with `Heartbeat` when
//! idle) while the standby acknowledges progress with `FlushAck`. A `HelloAck
//! { accepted: false }` arriving *after* the handshake is the primary's
//! post-handshake rejection notice (diverged timeline, slot table full, ...)
//! — connection-fatal, with the operator fix in `message`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod framing;
pub mod handshake;
pub mod listener;
pub mod receiver;
pub mod sender;
pub mod server;
pub mod tls;

/// Replication protocol version. Bumped on any incompatible wire change.
pub const REPL_PROTOCOL_VERSION: u32 = 1;

/// The frame header size: `payload_len: u32` + `msg_type: u16` + `flags: u16`.
pub const REPL_HEADER_LEN: usize = 8;

/// Upper bound on a framed payload — a `LogData` WAL-range chunk plus overhead.
/// A sender chunks a large flushed range to stay under it.
pub const REPL_MAX_PAYLOAD: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ReplProtoError {
    #[error("truncated replication frame")]
    Truncated,
    #[error("replication frame length mismatch")]
    LengthMismatch,
    #[error("replication payload exceeds the {REPL_MAX_PAYLOAD}-byte limit")]
    TooLarge,
    #[error("unknown replication message type {0}")]
    UnknownMsgType(u16),
    #[error("replication encode error: {0}")]
    Encode(String),
    #[error("replication decode error: {0}")]
    Decode(String),
}

#[repr(u16)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ReplMsgType {
    /// Standby → primary on connect: identity, auth, and where it left off.
    Hello = 1,
    /// Primary → standby: the handshake result.
    HelloAck = 2,
    /// Primary → standby: a raw WAL ring range `[from_lsn, from_lsn + bytes.len())`.
    LogData = 3,
    /// Standby → primary: how far it has received / flushed / applied.
    FlushAck = 4,
    /// Either direction: liveness.
    Heartbeat = 5,
    /// A former primary learns a standby was promoted (epoch fence).
    Promoted = 6,
}

impl TryFrom<u16> for ReplMsgType {
    type Error = ReplProtoError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ReplMsgType::Hello),
            2 => Ok(ReplMsgType::HelloAck),
            3 => Ok(ReplMsgType::LogData),
            4 => Ok(ReplMsgType::FlushAck),
            5 => Ok(ReplMsgType::Heartbeat),
            6 => Ok(ReplMsgType::Promoted),
            other => Err(ReplProtoError::UnknownMsgType(other)),
        }
    }
}

/// Standby → primary on connect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub protocol_version: u32,
    pub node_id: u64,
    pub cluster_uuid: [u8; 16],
    /// The replication epoch the standby last saw (0 if fresh); the primary
    /// fences a stale one.
    pub epoch: u64,
    /// The LSN the standby has durably received, so the primary resumes shipping
    /// from there.
    pub last_received_lsn: u64,
    /// Proof of the shared secret. Opaque at this layer; the handshake slice
    /// verifies it (constant-time).
    pub auth: Vec<u8>,
}

/// Primary → standby: the handshake result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloAck {
    pub protocol_version: u32,
    pub accepted: bool,
    pub primary_epoch: u64,
    /// The primary's current durable tail — the standby cannot ask beyond it.
    pub primary_flushed_lsn: u64,
    pub message: String,
}

/// Primary → standby: raw WAL ring bytes to apply at `from_lsn` (as
/// [`crate::storage::Storage::read_wal_range`] produced them).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogData {
    pub from_lsn: u64,
    pub bytes: Vec<u8>,
}

/// Standby → primary: the standby's watermarks, so the primary advances the
/// standby's replication slot and (D2) releases sync-commit waiters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FlushAck {
    pub received_lsn: u64,
    pub flushed_lsn: u64,
    pub applied_lsn: u64,
}

/// Either direction: liveness, carrying a millisecond wall-clock stamp.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Heartbeat {
    pub time_ms: u64,
}

/// A former primary is told a standby was promoted at `new_epoch`; it must stop
/// and reseed (epoch fencing).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Promoted {
    pub new_epoch: u64,
}

/// A framed replication message: a type tag, flags, and an opaque bincode
/// payload.
#[derive(Debug, Clone)]
pub struct ReplFrame {
    pub msg_type: ReplMsgType,
    pub flags: u16,
    pub payload: Vec<u8>,
}

impl ReplFrame {
    /// Builds a frame from a typed message, bincode-encoding the payload.
    pub fn encode<T: Serialize>(msg_type: ReplMsgType, msg: &T) -> Result<Self, ReplProtoError> {
        let payload = bincode::serialize(msg).map_err(|e| ReplProtoError::Encode(e.to_string()))?;
        Ok(ReplFrame {
            msg_type,
            flags: 0,
            payload,
        })
    }

    /// Decodes the payload as `T`.
    pub fn decode<T: for<'de> Deserialize<'de>>(&self) -> Result<T, ReplProtoError> {
        bincode::deserialize(&self.payload).map_err(|e| ReplProtoError::Decode(e.to_string()))
    }
}

/// Serializes a frame to bytes (8-byte header + payload).
pub fn encode_repl_frame(frame: &ReplFrame) -> Result<Vec<u8>, ReplProtoError> {
    if frame.payload.len() > REPL_MAX_PAYLOAD {
        return Err(ReplProtoError::TooLarge);
    }
    let mut out = Vec::with_capacity(REPL_HEADER_LEN + frame.payload.len());
    out.extend_from_slice(&(frame.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&(frame.msg_type as u16).to_be_bytes());
    out.extend_from_slice(&frame.flags.to_be_bytes());
    out.extend_from_slice(&frame.payload);
    Ok(out)
}

/// Deserializes a whole frame from a complete buffer (header + exact payload).
/// The async reader reads the 8-byte header, then exactly `payload_len` bytes,
/// then calls this on the assembled buffer.
pub fn decode_repl_frame(bytes: &[u8]) -> Result<ReplFrame, ReplProtoError> {
    if bytes.len() < REPL_HEADER_LEN {
        return Err(ReplProtoError::Truncated);
    }
    let len = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if len > REPL_MAX_PAYLOAD {
        return Err(ReplProtoError::TooLarge);
    }
    let msg_type = ReplMsgType::try_from(u16::from_be_bytes(bytes[4..6].try_into().unwrap()))?;
    let flags = u16::from_be_bytes(bytes[6..8].try_into().unwrap());
    if bytes.len() != REPL_HEADER_LEN + len {
        return Err(ReplProtoError::LengthMismatch);
    }
    Ok(ReplFrame {
        msg_type,
        flags,
        payload: bytes[REPL_HEADER_LEN..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trips a typed message through `ReplFrame::encode` → bytes → decode.
    fn roundtrip_frame(frame: &ReplFrame) -> ReplFrame {
        let bytes = encode_repl_frame(frame).expect("encode");
        assert_eq!(bytes.len(), REPL_HEADER_LEN + frame.payload.len());
        decode_repl_frame(&bytes).expect("decode")
    }

    #[test]
    fn hello_round_trips() {
        let hello = Hello {
            protocol_version: REPL_PROTOCOL_VERSION,
            node_id: 42,
            cluster_uuid: [7u8; 16],
            epoch: 3,
            last_received_lsn: 987654321,
            auth: vec![1, 2, 3, 4],
        };
        let frame = ReplFrame::encode(ReplMsgType::Hello, &hello).expect("encode");
        let back = roundtrip_frame(&frame);
        assert_eq!(back.msg_type, ReplMsgType::Hello);
        let decoded: Hello = back.decode().expect("decode payload");
        assert_eq!(decoded.node_id, 42);
        assert_eq!(decoded.cluster_uuid, [7u8; 16]);
        assert_eq!(decoded.last_received_lsn, 987654321);
        assert_eq!(decoded.auth, vec![1, 2, 3, 4]);
    }

    #[test]
    fn every_message_type_round_trips() {
        let cases: Vec<ReplFrame> = vec![
            ReplFrame::encode(
                ReplMsgType::HelloAck,
                &HelloAck {
                    protocol_version: 1,
                    accepted: true,
                    primary_epoch: 2,
                    primary_flushed_lsn: 4096,
                    message: "welcome".to_string(),
                },
            )
            .unwrap(),
            ReplFrame::encode(
                ReplMsgType::LogData,
                &LogData {
                    from_lsn: 4096,
                    bytes: vec![9u8; 300],
                },
            )
            .unwrap(),
            ReplFrame::encode(
                ReplMsgType::FlushAck,
                &FlushAck {
                    received_lsn: 8192,
                    flushed_lsn: 8000,
                    applied_lsn: 7000,
                },
            )
            .unwrap(),
            ReplFrame::encode(ReplMsgType::Heartbeat, &Heartbeat { time_ms: 123 }).unwrap(),
            ReplFrame::encode(ReplMsgType::Promoted, &Promoted { new_epoch: 5 }).unwrap(),
        ];
        for frame in &cases {
            let back = roundtrip_frame(frame);
            assert_eq!(back.msg_type, frame.msg_type);
            assert_eq!(back.payload, frame.payload);
        }
    }

    #[test]
    fn a_log_data_frame_carries_the_raw_bytes_verbatim() {
        let bytes: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let frame = ReplFrame::encode(
            ReplMsgType::LogData,
            &LogData {
                from_lsn: 65536,
                bytes: bytes.clone(),
            },
        )
        .unwrap();
        let back = roundtrip_frame(&frame);
        let decoded: LogData = back.decode().unwrap();
        assert_eq!(decoded.from_lsn, 65536);
        assert_eq!(decoded.bytes, bytes);
    }

    #[test]
    fn a_short_buffer_is_truncated() {
        assert!(matches!(
            decode_repl_frame(&[0u8; 4]),
            Err(ReplProtoError::Truncated)
        ));
    }

    #[test]
    fn a_wrong_declared_length_is_a_mismatch() {
        // Header claims 100 payload bytes, but only 8 (header) are present.
        let mut bytes = 100u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&(ReplMsgType::Heartbeat as u16).to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        assert!(matches!(
            decode_repl_frame(&bytes),
            Err(ReplProtoError::LengthMismatch)
        ));
    }

    #[test]
    fn an_unknown_message_type_is_rejected() {
        let mut bytes = 0u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&999u16.to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        assert!(matches!(
            decode_repl_frame(&bytes),
            Err(ReplProtoError::UnknownMsgType(999))
        ));
    }

    #[test]
    fn an_oversized_declared_length_is_rejected_before_allocating() {
        let mut bytes = ((REPL_MAX_PAYLOAD + 1) as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(&(ReplMsgType::LogData as u16).to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        assert!(matches!(
            decode_repl_frame(&bytes),
            Err(ReplProtoError::TooLarge)
        ));
    }
}
