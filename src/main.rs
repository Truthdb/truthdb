use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use tracing::info;
use tracing_subscriber::EnvFilter;
use truthdb_core::storage::Storage;

fn main() {
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

    let should_stop = Arc::new(AtomicBool::new(false));

    // systemd stops services by sending SIGTERM. Also handle SIGINT for dev convenience.
    #[cfg(unix)]
    {
        use signal_hook::consts::signal::{SIGINT, SIGTERM};
        use signal_hook::flag;

        // If registration fails, keep running; worst case the service must be killed.
        let _ = flag::register(SIGTERM, Arc::clone(&should_stop));
        let _ = flag::register(SIGINT, Arc::clone(&should_stop));
    }

    info!("TruthDB running (waiting for stop signal)");
    while !should_stop.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_secs(1));
    }

    info!("Stop signal received; shutting down...");
    info!("TruthDB exiting");
}
