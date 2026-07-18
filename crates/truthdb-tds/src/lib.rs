//! TDS (Tabular Data Stream) gateway for TruthDB.
//!
//! A SQL Server-protocol front end so real drivers (pymssql, go-mssqldb) can
//! connect to TruthDB. Covers the packet layer, PRELOGIN, optional TLS (the
//! tunneled handshake of MS-TDS 2.2.6.5; see [`tls`]), LOGIN7 with config-file
//! auth, SQLBatch → token-stream execution over the engine's typed SQL results,
//! and RPC `sp_executesql` for parameterized queries (see [`rpc`]). Handle-based
//! prepared statements (`sp_prepare`/`sp_execute`) arrive later in Stage 9.

pub mod login;
pub mod packet;
pub mod rpc;
pub mod server;
pub mod throttle;
pub mod tls;
pub mod token;
pub mod typeinfo;

use std::net::IpAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{debug, error, info};
use truthdb_core::session::EngineHandle;

pub use server::{Encryption, TdsConfig, serve_connection};
pub use throttle::LoginThrottle;

/// A TDS listener bound to an address; serves connections until shutdown.
pub struct TdsListener {
    listener: TcpListener,
    engine: EngineHandle,
    config: Arc<TdsConfig>,
    /// Per-`(login, IP)` brute-force throttle, shared across connections.
    throttle: LoginThrottle,
}

impl TdsListener {
    /// Binds the listener. Returns the bound address for logging/tests.
    pub async fn bind(
        addr: &str,
        port: u16,
        engine: EngineHandle,
        config: TdsConfig,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind((addr, port)).await?;
        Ok(TdsListener {
            listener,
            engine,
            config: Arc::new(config),
            throttle: LoginThrottle::new(),
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
                    // TDS is request/response with multi-write framing; Nagle
                    // + delayed ACK stalls each round trip without this.
                    let _ = stream.set_nodelay(true);
                    debug!(%peer, "TDS connection accepted");
                    let engine = self.engine.clone();
                    let config = Arc::clone(&self.config);
                    let throttle = self.throttle.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handle(stream, engine, config, throttle, peer.ip()).await {
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
    engine: EngineHandle,
    config: Arc<TdsConfig>,
    throttle: LoginThrottle,
    peer: IpAddr,
) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    match serve_connection(stream, engine, config, throttle, peer).await {
        Ok(()) => Ok(()),
        Err(err) => {
            error!(error = %err, "TDS connection error");
            Err(err)
        }
    }
}
