use crate::ProtoError;
use serde::{Deserialize, Serialize};

#[repr(u16)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MsgType {
    HelloReq = 1,
    HelloResp = 2,
}

impl TryFrom<u16> for MsgType {
    type Error = ProtoError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(MsgType::HelloReq),
            2 => Ok(MsgType::HelloResp),
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
