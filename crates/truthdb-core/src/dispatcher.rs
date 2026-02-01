use std::time::{SystemTime, UNIX_EPOCH};

use truthdb_proto::{
    Frame, HeartbeatReq, HeartbeatResp, MsgType, ProtoError, decode_message, encode_message,
};

#[derive(Debug, Default)]
pub struct Dispatcher;

impl Dispatcher {
    pub fn new() -> Self {
        Dispatcher
    }

    pub fn dispatch(&self, frame: Frame) -> Result<Option<Frame>, ProtoError> {
        match frame.msg_type {
            MsgType::HelloReq => Ok(Some(Frame {
                msg_type: MsgType::HelloResp,
                flags: 0,
                payload: Vec::new(),
            })),
            MsgType::HeartbeatReq => {
                let req: HeartbeatReq = decode_message(&frame.payload)?;
                let server_time_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| ProtoError::Encode(e.to_string()))?
                    .as_millis() as u64;

                let resp = HeartbeatResp {
                    nonce: req.nonce,
                    server_time_ms,
                };

                Ok(Some(Frame {
                    msg_type: MsgType::HeartbeatResp,
                    flags: 0,
                    payload: encode_message(&resp)?,
                }))
            }
            _ => Ok(None),
        }
    }
}
