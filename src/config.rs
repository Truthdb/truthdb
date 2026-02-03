use directories::ProjectDirs;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,

    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    #[serde(default = "default_addr")]
    pub addr: String,

    #[serde(default = "default_port")]
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_storage_path")]
    pub path: String,

    #[serde(default = "default_storage_size_gib")]
    pub size_gib: u64,

    #[serde(default = "default_storage_wal_ratio")]
    pub wal_ratio: f64,

    #[serde(default = "default_storage_metadata_ratio")]
    pub metadata_ratio: f64,

    #[serde(default = "default_storage_snapshot_ratio")]
    pub snapshot_ratio: f64,

    #[serde(default = "default_storage_allocator_ratio")]
    pub allocator_ratio: f64,

    #[serde(default = "default_storage_reserved_ratio")]
    pub reserved_ratio: f64,

    #[serde(default = "default_storage_group_sync_batches")]
    pub group_sync_batches: u32,

    #[serde(default = "default_storage_group_sync_ms")]
    pub group_sync_ms: u64,

    #[serde(default = "default_storage_backpressure_timeout_ms")]
    pub backpressure_timeout_ms: u64,

    #[serde(default = "default_storage_snapshot_min_interval_ms")]
    pub snapshot_min_interval_ms: u64,

    #[serde(default = "default_storage_snapshot_wal_threshold")]
    pub snapshot_wal_threshold: f64,
}

fn default_addr() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    9623
}

fn default_storage_path() -> String {
    "truth.db".to_string()
}

fn default_storage_size_gib() -> u64 {
    10
}

fn default_storage_wal_ratio() -> f64 {
    0.05
}

fn default_storage_metadata_ratio() -> f64 {
    0.08
}

fn default_storage_snapshot_ratio() -> f64 {
    0.02
}

fn default_storage_allocator_ratio() -> f64 {
    0.02
}

fn default_storage_reserved_ratio() -> f64 {
    0.17
}

fn default_storage_group_sync_batches() -> u32 {
    32
}

fn default_storage_group_sync_ms() -> u64 {
    5
}

fn default_storage_backpressure_timeout_ms() -> u64 {
    50
}

fn default_storage_snapshot_min_interval_ms() -> u64 {
    1000
}

fn default_storage_snapshot_wal_threshold() -> f64 {
    0.7
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            addr: default_addr(),
            port: default_port(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            path: default_storage_path(),
            size_gib: default_storage_size_gib(),
            wal_ratio: default_storage_wal_ratio(),
            metadata_ratio: default_storage_metadata_ratio(),
            snapshot_ratio: default_storage_snapshot_ratio(),
            allocator_ratio: default_storage_allocator_ratio(),
            reserved_ratio: default_storage_reserved_ratio(),
            group_sync_batches: default_storage_group_sync_batches(),
            group_sync_ms: default_storage_group_sync_ms(),
            backpressure_timeout_ms: default_storage_backpressure_timeout_ms(),
            snapshot_min_interval_ms: default_storage_snapshot_min_interval_ms(),
            snapshot_wal_threshold: default_storage_snapshot_wal_threshold(),
        }
    }
}

impl StorageConfig {
    /// Resolve `storage.path` using systemd `STATE_DIRECTORY` when available,
    /// otherwise fall back to the OS data dir (via `directories` crate).
    pub fn resolved_path(&self) -> PathBuf {
        let path = PathBuf::from(&self.path);
        if path.is_absolute() {
            return path;
        }

        if let Some(state_dir) = systemd_state_directory() {
            return state_dir.join(&path);
        }

        if let Some(proj_dirs) = ProjectDirs::from("org", "truthdb", "truthdb") {
            return proj_dirs.data_dir().join(&path);
        }

        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(&path)
    }
}

fn systemd_state_directory() -> Option<PathBuf> {
    let raw = std::env::var_os("STATE_DIRECTORY")?;
    let raw = raw.to_string_lossy();
    let first = raw.split(':').next()?.trim();
    if first.is_empty() {
        return None;
    }
    Some(Path::new(first).to_path_buf())
}

impl Config {
    /// Load config: embedded default, then override with OS-standard config file if present.
    pub fn load() -> Self {
        // Load embedded default config
        let default_str = include_str!("../config/default.toml");
        let mut config: Config = toml::from_str(default_str).unwrap_or_default();

        // Try to override with OS-standard config file
        if let Some(proj_dirs) = ProjectDirs::from("org", "truthdb", "truthdb") {
            let mut config_path = PathBuf::from(proj_dirs.config_dir());
            config_path.push("truthdb.toml");
            if config_path.exists()
                && let Ok(contents) = fs::read_to_string(&config_path)
                && let Ok(override_cfg) = toml::from_str::<ConfigOverride>(&contents)
            {
                if let Some(addr) = override_cfg.addr {
                    config.network.addr = addr;
                }
                if let Some(port) = override_cfg.port {
                    config.network.port = port;
                }
                if let Some(network) = override_cfg.network {
                    apply_network_override(&mut config.network, network);
                }
                if let Some(storage) = override_cfg.storage {
                    apply_storage_override(&mut config.storage, storage);
                }
            }
        }
        config
    }
}

#[derive(Debug, Deserialize, Default)]
struct ConfigOverride {
    addr: Option<String>,
    port: Option<u16>,
    network: Option<NetworkConfigOverride>,
    storage: Option<StorageConfigOverride>,
}

#[derive(Debug, Deserialize, Default)]
struct NetworkConfigOverride {
    addr: Option<String>,
    port: Option<u16>,
}

#[derive(Debug, Deserialize, Default)]
struct StorageConfigOverride {
    path: Option<String>,
    size_gib: Option<u64>,
    wal_ratio: Option<f64>,
    metadata_ratio: Option<f64>,
    snapshot_ratio: Option<f64>,
    allocator_ratio: Option<f64>,
    reserved_ratio: Option<f64>,
    group_sync_batches: Option<u32>,
    group_sync_ms: Option<u64>,
    backpressure_timeout_ms: Option<u64>,
    snapshot_min_interval_ms: Option<u64>,
    snapshot_wal_threshold: Option<f64>,
}

fn apply_network_override(target: &mut NetworkConfig, source: NetworkConfigOverride) {
    if let Some(addr) = source.addr {
        target.addr = addr;
    }
    if let Some(port) = source.port {
        target.port = port;
    }
}

fn apply_storage_override(target: &mut StorageConfig, source: StorageConfigOverride) {
    if let Some(path) = source.path {
        target.path = path;
    }
    if let Some(size_gib) = source.size_gib {
        target.size_gib = size_gib;
    }
    if let Some(wal_ratio) = source.wal_ratio {
        target.wal_ratio = wal_ratio;
    }
    if let Some(metadata_ratio) = source.metadata_ratio {
        target.metadata_ratio = metadata_ratio;
    }
    if let Some(snapshot_ratio) = source.snapshot_ratio {
        target.snapshot_ratio = snapshot_ratio;
    }
    if let Some(allocator_ratio) = source.allocator_ratio {
        target.allocator_ratio = allocator_ratio;
    }
    if let Some(reserved_ratio) = source.reserved_ratio {
        target.reserved_ratio = reserved_ratio;
    }
    if let Some(group_sync_batches) = source.group_sync_batches {
        target.group_sync_batches = group_sync_batches;
    }
    if let Some(group_sync_ms) = source.group_sync_ms {
        target.group_sync_ms = group_sync_ms;
    }
    if let Some(backpressure_timeout_ms) = source.backpressure_timeout_ms {
        target.backpressure_timeout_ms = backpressure_timeout_ms;
    }
    if let Some(snapshot_min_interval_ms) = source.snapshot_min_interval_ms {
        target.snapshot_min_interval_ms = snapshot_min_interval_ms;
    }
    if let Some(snapshot_wal_threshold) = source.snapshot_wal_threshold {
        target.snapshot_wal_threshold = snapshot_wal_threshold;
    }
}
