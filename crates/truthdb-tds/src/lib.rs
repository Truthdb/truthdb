//! TDS (Tabular Data Stream) gateway for TruthDB.
//!
//! A plaintext SQL Server-protocol front end so real drivers (pymssql,
//! go-mssqldb with `encrypt=disable`) can connect to TruthDB. Stage 4 covers
//! the packet layer, PRELOGIN (encryption not supported), LOGIN7 with
//! config-file auth, and SQLBatch → token-stream execution over the engine's
//! typed SQL results. TLS and RPC/prepared statements arrive in Stage 9.

pub mod login;
pub mod packet;
pub mod server;
pub mod token;
pub mod typeinfo;

use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{debug, error, info};
use truthdb_core::engine::Engine;

pub use server::{TdsConfig, serve_connection};

/// A TDS listener bound to an address; serves connections until shutdown.
pub struct TdsListener {
    listener: TcpListener,
    engine: Arc<Mutex<Engine>>,
    config: Arc<TdsConfig>,
}

impl TdsListener {
    /// Binds the listener. Returns the bound address for logging/tests.
    pub async fn bind(
        addr: &str,
        port: u16,
        engine: Arc<Mutex<Engine>>,
        config: TdsConfig,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind((addr, port)).await?;
        Ok(TdsListener {
            listener,
            engine,
            config: Arc::new(config),
        })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Accepts connections until `shutdown` flips to true.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> std::io::Result<()> {
        loop {
            tokio::select! {
                accepted = self.listener.accept() => {
                    let (stream, peer) = accepted?;
                    debug!(%peer, "TDS connection accepted");
                    let engine = Arc::clone(&self.engine);
                    let config = Arc::clone(&self.config);
                    tokio::spawn(async move {
                        if let Err(err) = handle(stream, engine, config).await {
                            debug!(%peer, error = %err, "TDS connection closed");
                        }
                    });
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("TDS listener shutting down");
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn handle(
    stream: TcpStream,
    engine: Arc<Mutex<Engine>>,
    config: Arc<TdsConfig>,
) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    match serve_connection(stream, engine, config).await {
        Ok(()) => Ok(()),
        Err(err) => {
            error!(error = %err, "TDS connection error");
            Err(err)
        }
    }
}
