use std::net::SocketAddr;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use truthdb_net::{read_frame, write_frame};
use truthdb_proto::ProtoError;

use crate::dispatcher::Dispatcher;

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
}

impl ClientListener {
    pub fn new(host: &str, port: u16) -> Result<Self, ClientListenerError> {
        let addr: SocketAddr = format!("{host}:{port}").parse()?;
        Ok(ClientListener { addr })
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
                    tokio::spawn(async move {
                        if let Err(err) = handle_client(stream, &mut conn_shutdown).await {
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
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), ClientListenerError> {
    let dispatcher = Dispatcher::new();

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
