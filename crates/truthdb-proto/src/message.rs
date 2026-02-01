use crate::ProtoError;
use serde::{Deserialize, Serialize};

#[repr(u16)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MsgType {
    HelloReq = 1,
    HelloResp = 2,
    HeartbeatReq = 3,
    HeartbeatResp = 4,
    CommandReq = 5,
    CommandResp = 6,
}

impl TryFrom<u16> for MsgType {
    type Error = ProtoError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(MsgType::HelloReq),
            2 => Ok(MsgType::HelloResp),
            3 => Ok(MsgType::HeartbeatReq),
            4 => Ok(MsgType::HeartbeatResp),
            5 => Ok(MsgType::CommandReq),
            6 => Ok(MsgType::CommandResp),
            other => Err(ProtoError::UnknownMsgType(other)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HelloReq {
    pub protocol_version: u32,
    pub client_name: String,
    pub client_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HelloResp {
    pub protocol_version: u32,
    pub server_name: String,
    pub server_version: String,
    pub capabilities: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct HeartbeatReq {
    pub nonce: u64,
    pub client_time_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct HeartbeatResp {
    pub nonce: u64,
    pub server_time_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandReq {
    pub id: u64,
    pub command: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandResp {
    pub id: u64,
    pub ok: bool,
    pub message: String,
}

pub fn encode_message<T: Serialize>(msg: &T) -> Result<Vec<u8>, ProtoError> {
    bincode::serialize(msg).map_err(|e| ProtoError::Encode(e.to_string()))
}

pub fn decode_message<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ProtoError> {
    bincode::deserialize(bytes).map_err(|e| ProtoError::Decode(e.to_string()))
}
