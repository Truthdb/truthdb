use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use truthdb_proto::{Frame, MsgType, ProtoError};

const HEADER_LEN: usize = 8;
const MAX_PAYLOAD: usize = 8 * 1024 * 1024; // 8 MiB

pub async fn read_frame(stream: &mut TcpStream) -> Result<Frame, ProtoError> {
    let mut header = [0u8; HEADER_LEN];

    stream
        .read_exact(&mut header)
        .await
        .map_err(|e| ProtoError::Decode(e.to_string()))?;

    let len = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
    let msg_type_raw = u16::from_be_bytes(header[4..6].try_into().unwrap());
    let flags = u16::from_be_bytes(header[6..8].try_into().unwrap());

    if len > MAX_PAYLOAD {
        return Err(ProtoError::Decode(format!(
            "payload too large: {} > {}",
            len, MAX_PAYLOAD
        )));
    }

    let msg_type = MsgType::try_from(msg_type_raw)?;

    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| ProtoError::Decode(e.to_string()))?;

    Ok(Frame {
        msg_type,
        flags,
        payload,
    })
}

pub async fn write_frame(stream: &mut TcpStream, frame: &Frame) -> Result<(), ProtoError> {
    let len = frame.payload.len();

    if len > MAX_PAYLOAD {
        return Err(ProtoError::Encode(format!(
            "payload too large: {} > {}",
            len, MAX_PAYLOAD
        )));
    }

    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(&(len as u32).to_be_bytes());
    header[4..6].copy_from_slice(&(frame.msg_type as u16).to_be_bytes());
    header[6..8].copy_from_slice(&frame.flags.to_be_bytes());

    stream
        .write_all(&header)
        .await
        .map_err(|e| ProtoError::Encode(e.to_string()))?;

    stream
        .write_all(&frame.payload)
        .await
        .map_err(|e| ProtoError::Encode(e.to_string()))?;

    stream
        .flush()
        .await
        .map_err(|e| ProtoError::Encode(e.to_string()))?;

    Ok(())
}
