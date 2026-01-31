use crate::{Frame, MsgType, ProtoError};

pub const HEADER_LEN: usize = 8;

pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, ProtoError> {
    let mut out = Vec::with_capacity(HEADER_LEN + frame.payload.len());

    out.extend_from_slice(&(frame.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&(frame.msg_type as u16).to_be_bytes());
    out.extend_from_slice(&frame.flags.to_be_bytes());
    out.extend_from_slice(&frame.payload);

    Ok(out)
}

pub fn decode_frame(bytes: &[u8]) -> Result<Frame, ProtoError> {
    if bytes.len() < HEADER_LEN {
        return Err(ProtoError::Truncated);
    }

    let len = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let msg_type = MsgType::try_from(u16::from_be_bytes(bytes[4..6].try_into().unwrap()))?;
    let flags = u16::from_be_bytes(bytes[6..8].try_into().unwrap());

    if bytes.len() != HEADER_LEN + len {
        return Err(ProtoError::LengthMismatch);
    }

    Ok(Frame {
        msg_type,
        flags,
        payload: bytes[8..].to_vec(),
    })
}
