use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use truthdb_proto::{
    CommandReq, CommandResp, Frame, HeartbeatReq, HeartbeatResp, HelloReq, HelloResp, MsgType,
    PROTOCOL_VERSION, ProtoError, decode_message, encode_message,
};

use crate::engine::Engine;

pub struct Dispatcher {
    engine: Arc<Mutex<Engine>>,
}

impl Dispatcher {
    pub fn new(engine: Arc<Mutex<Engine>>) -> Self {
        Dispatcher { engine }
    }

    pub fn dispatch(&self, frame: Frame) -> Result<Option<Frame>, ProtoError> {
        match frame.msg_type {
            MsgType::HelloReq => {
                let _req: HelloReq = decode_message(&frame.payload)?;
                let resp = HelloResp {
                    protocol_version: PROTOCOL_VERSION,
                    server_name: "truthdb".to_string(),
                    server_version: env!("CARGO_PKG_VERSION").to_string(),
                    capabilities: 0,
                };

                Ok(Some(Frame {
                    msg_type: MsgType::HelloResp,
                    flags: 0,
                    payload: encode_message(&resp)?,
                }))
            }
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
            MsgType::CommandReq => {
                let req: CommandReq = decode_message(&frame.payload)?;
                let resp = match self.engine.lock() {
                    Ok(mut engine) => match engine.execute(&req.command) {
                        Ok(message) => CommandResp {
                            id: req.id,
                            ok: true,
                            message,
                        },
                        Err(err) => CommandResp {
                            id: req.id,
                            ok: false,
                            message: err.to_string(),
                        },
                    },
                    Err(_) => CommandResp {
                        id: req.id,
                        ok: false,
                        message: "engine lock poisoned".to_string(),
                    },
                };

                Ok(Some(Frame {
                    msg_type: MsgType::CommandResp,
                    flags: 0,
                    payload: encode_message(&resp)?,
                }))
            }
            _ => Ok(None),
        }
    }
}
