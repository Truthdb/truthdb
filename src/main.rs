#[cfg(not(target_os = "linux"))]
compile_error!("TruthDB must be built for Linux targets. Use Docker or a Linux environment.");

use std::sync::{Arc, Mutex};

use tokio::sync::watch;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
use truthdb_core::client_listener::ClientListener;
use truthdb_core::engine::Engine;
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
    debug!(?config, "Loaded config");
    let storage_path = config.storage.resolved_path();
    info!(
        "Loaded config: addr={} port={} storage_path={} storage_size_gib={}",
        config.network.addr,
        config.network.port,
        storage_path.display(),
        config.storage.size_gib
    );

    let storage_opts = truthdb_core::storage::StorageOptions {
        size_gib: config.storage.size_gib,
        wal_ratio: config.storage.wal_ratio,
        metadata_ratio: config.storage.metadata_ratio,
        snapshot_ratio: config.storage.snapshot_ratio,
        allocator_ratio: config.storage.allocator_ratio,
        reserved_ratio: config.storage.reserved_ratio,
    };

    let storage = if storage_path.exists() {
        match Storage::open(storage_path) {
            Ok(storage) => storage,
            Err(err) => {
                eprintln!("Failed to open storage: {err}");
                return;
            }
        }
    } else {
        match Storage::create(storage_path, storage_opts) {
            Ok(storage) => storage,
            Err(err) => {
                eprintln!("Failed to create storage: {err}");
                return;
            }
        }
    };

    let engine = match Engine::new(storage) {
        Ok(engine) => Arc::new(Mutex::new(engine)),
        Err(err) => {
            eprintln!("Failed to initialize engine: {err}");
            return;
        }
    };

    let client_listener = match ClientListener::new(
        &config.network.addr,
        config.network.port,
        Arc::clone(&engine),
    ) {
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

    info!("TruthDB running (waiting for stop signal)");
    wait_for_shutdown_signal().await;

    info!("Stop signal received; shutting down...");
    let _ = shutdown_tx.send(true);
    let _ = listener_task.await;
    info!("TruthDB exiting");
}
