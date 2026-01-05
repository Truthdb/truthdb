use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use tracing::info;
use truthdb_core::storage::Storage;

fn main() {
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
