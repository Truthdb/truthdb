#[derive(Debug, Clone)]
pub struct Frame {
    pub msg_type: crate::MsgType,
    pub flags: u16,
    pub payload: Vec<u8>,
}
