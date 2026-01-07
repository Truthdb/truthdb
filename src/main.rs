use tracing::info;
use tracing_subscriber::EnvFilter;
use truthdb_core::storage::Storage;

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

    info!("Starting TruthDB...");

    let _storage = Storage {};

    info!("TruthDB running (waiting for stop signal)");
    wait_for_shutdown_signal().await;

    info!("Stop signal received; shutting down...");
    info!("TruthDB exiting");
}
