use tokio::sync::watch;
use tracing::info;
use tracing_subscriber::EnvFilter;
use truthdb_core::client_listener::ClientListener;
use truthdb_core::storage::Storage;
mod config;
use config::Config;

async fn wait_for_shutdown_signal() {
    // systemd stops services by sending SIGTERM. Also handle SIGINT for dev convenience.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        let mut sigint =
            signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => {},
            _ = sigint.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() {
    // Emit tracing logs to stdout/stderr. Under systemd, these show up in journald.
    // Override levels with RUST_LOG, e.g. RUST_LOG=debug.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_level(true)
        .init();

    // Load layered config: embedded default, then OS-standard config file if present
    let config = Config::load();
    let storage_path = config.storage.resolved_path();
    info!(
        "Loaded config: addr={} port={} storage_path={} storage_size_gib={}",
        config.network.addr,
        config.network.port,
        storage_path.display(),
        config.storage.size_gib
    );

    let client_listener = match ClientListener::new(&config.network.addr, config.network.port) {
        Ok(client_listener) => client_listener,
        Err(err) => {
            eprintln!("Failed to initialize server: {err}");
            return;
        }
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener_task = tokio::spawn(async move {
        if let Err(err) = client_listener.run(shutdown_rx).await {
            eprintln!("Client listener error: {err}");
        }
    });

    info!("Starting TruthDB...");

    let _storage = Storage {};

    info!("TruthDB running (waiting for stop signal)");
    wait_for_shutdown_signal().await;

    info!("Stop signal received; shutting down...");
    let _ = shutdown_tx.send(true);
    let _ = listener_task.await;
    info!("TruthDB exiting");
}
