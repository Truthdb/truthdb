use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtoError {
    #[error("truncated frame")]
    Truncated,

    #[error("length mismatch")]
    LengthMismatch,

    #[error("unknown message type {0}")]
    UnknownMsgType(u16),

    #[error("encode error: {0}")]
    Encode(String),

    #[error("decode error: {0}")]
    Decode(String),
}
