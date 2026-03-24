use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use truthdb_net::{read_frame, write_frame};
use truthdb_proto::ProtoError;

use crate::dispatcher::Dispatcher;
use crate::engine::Engine;

#[derive(Error, Debug)]
pub enum ClientListenerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("proto error: {0}")]
    Proto(#[from] ProtoError),

    #[error("invalid addr: {0}")]
    Addr(#[from] std::net::AddrParseError),
}

pub struct ClientListener {
    addr: SocketAddr,
    engine: Arc<Mutex<Engine>>,
}

impl ClientListener {
    pub fn new(
        host: &str,
        port: u16,
        engine: Arc<Mutex<Engine>>,
    ) -> Result<Self, ClientListenerError> {
        let addr: SocketAddr = format!("{host}:{port}").parse()?;
        Ok(ClientListener { addr, engine })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), ClientListenerError> {
        let listener = TcpListener::bind(self.addr).await?;

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    break;
                }
                res = listener.accept() => {
                    let (stream, _) = res?;
                    let mut conn_shutdown = shutdown.clone();
                    let engine = Arc::clone(&self.engine);
                    tokio::spawn(async move {
                        if let Err(err) = handle_client(stream, engine, &mut conn_shutdown).await
                            && !is_expected_disconnect(&err)
                        {
                            eprintln!("client handler error: {err}");
                        }
                    });
                }
            }
        }

        Ok(())
    }
}

async fn handle_client(
    mut stream: TcpStream,
    engine: Arc<Mutex<Engine>>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), ClientListenerError> {
    let dispatcher = Dispatcher::new(engine);

    loop {
        let frame = tokio::select! {
            _ = shutdown.changed() => {
                return Ok(());
            }
            res = read_frame(&mut stream) => {
                res?
            }
        };

        if let Some(resp) = dispatcher.dispatch(frame)? {
            write_frame(&mut stream, &resp).await?;
        }
    }
}

fn is_expected_disconnect(err: &ClientListenerError) -> bool {
    let ClientListenerError::Proto(ProtoError::Decode(msg)) = err else {
        return false;
    };

    let msg = msg.to_ascii_lowercase();
    msg.contains("early eof")
        || msg.contains("unexpected eof")
        || msg.contains("connection reset")
        || msg.contains("broken pipe")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn early_eof_is_treated_as_expected_disconnect() {
        let err = ClientListenerError::Proto(ProtoError::Decode("early eof".to_string()));
        assert!(is_expected_disconnect(&err));
    }

    #[test]
    fn other_proto_errors_are_not_treated_as_expected_disconnect() {
        let err = ClientListenerError::Proto(ProtoError::Decode("payload too large".to_string()));
        assert!(!is_expected_disconnect(&err));
    }
}
