use tracing::info;
use truthdb_core::storage::Storage;

fn main() {
    info!("Starting TruthDB...");
    let _storage = Storage {};
    info!("TruthDB exiting...");
}
