use directories::ProjectDirs;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub tds: TdsConfig,

    #[serde(default)]
    pub replication: ReplicationConfig,
}

/// Physical replication (Stage 18). Disabled by default. A `primary` runs the
/// replication listener on `port` (default 9624, TLS mandatory) and streams WAL
/// to authenticated standbys; a `standby` dials `primary_addr` and applies the
/// stream (its database file must be seeded with `truthdb-cli restore
/// --standby`). Both roles need the same `cluster_uuid` and `shared_secret`.
#[derive(Deserialize)]
pub struct ReplicationConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub role: ReplicationRole,

    /// Primary: listener bind address.
    #[serde(default = "default_addr")]
    pub addr: String,

    /// Primary: listener port.
    #[serde(default = "default_replication_port")]
    pub port: u16,

    /// This node's id (also its replication-slot id on the primary; standbys
    /// must use distinct ids).
    #[serde(default = "default_replication_node_id")]
    pub node_id: u32,

    /// The cluster's identity, shared by every member: a UUID (hyphens
    /// optional). Required when enabled.
    #[serde(default)]
    pub cluster_uuid: Option<String>,

    /// The cluster's shared secret (the handshake's HMAC key). Required when
    /// enabled; must not be empty.
    #[serde(default)]
    pub shared_secret: Option<String>,

    /// Primary: PEM certificate chain path for the replication listener's TLS.
    #[serde(default)]
    pub tls_cert: Option<String>,

    /// Primary: PEM private key path (paired with `tls_cert`).
    #[serde(default)]
    pub tls_key: Option<String>,

    /// Standby: PEM path of the certificate (or CA) to trust for the primary.
    #[serde(default)]
    pub tls_ca: Option<String>,

    /// Standby: the primary's replication endpoint, `host:port`.
    #[serde(default)]
    pub primary_addr: Option<String>,

    /// Standby: the TLS server name the primary's certificate must match
    /// (defaults to the host part of `primary_addr`).
    #[serde(default)]
    pub server_name: Option<String>,

    /// Primary: the sender's idle heartbeat interval.
    #[serde(default = "default_replication_heartbeat_ms")]
    pub heartbeat_ms: u64,

    /// Standby: delay between reconnect attempts.
    #[serde(default = "default_replication_reconnect_ms")]
    pub reconnect_delay_ms: u64,

    /// Both roles: how long the peer may stay completely silent before the
    /// connection is presumed dead (a healthy pair exchanges heartbeats/acks).
    #[serde(default = "default_replication_stall_ms")]
    pub stall_timeout_ms: u64,

    /// Primary: drop a replication slot once it lags the WAL tail by more than
    /// this many bytes (the standby must reseed). 0 = unlimited retention.
    #[serde(default)]
    pub max_slot_retain_bytes: u64,
}

/// The shared secret must not appear in logs (`debug!(?config)` logs the whole
/// config at startup), so Debug redacts it.
impl std::fmt::Debug for ReplicationConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationConfig")
            .field("enabled", &self.enabled)
            .field("role", &self.role)
            .field("addr", &self.addr)
            .field("port", &self.port)
            .field("node_id", &self.node_id)
            .field("cluster_uuid", &self.cluster_uuid)
            .field("shared_secret", &self.shared_secret.as_ref().map(|_| "***"))
            .field("tls_cert", &self.tls_cert)
            .field("tls_key", &self.tls_key)
            .field("tls_ca", &self.tls_ca)
            .field("primary_addr", &self.primary_addr)
            .field("server_name", &self.server_name)
            .field("heartbeat_ms", &self.heartbeat_ms)
            .field("reconnect_delay_ms", &self.reconnect_delay_ms)
            .field("stall_timeout_ms", &self.stall_timeout_ms)
            .field("max_slot_retain_bytes", &self.max_slot_retain_bytes)
            .finish()
    }
}

/// `[replication] role = "primary" | "standby"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationRole {
    #[default]
    Primary,
    Standby,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        ReplicationConfig {
            enabled: false,
            role: ReplicationRole::default(),
            addr: default_addr(),
            port: default_replication_port(),
            node_id: default_replication_node_id(),
            cluster_uuid: None,
            shared_secret: None,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            primary_addr: None,
            server_name: None,
            heartbeat_ms: default_replication_heartbeat_ms(),
            reconnect_delay_ms: default_replication_reconnect_ms(),
            stall_timeout_ms: default_replication_stall_ms(),
            max_slot_retain_bytes: 0,
        }
    }
}

fn default_replication_port() -> u16 {
    9624
}

fn default_replication_node_id() -> u32 {
    1
}

fn default_replication_heartbeat_ms() -> u64 {
    1000
}

fn default_replication_reconnect_ms() -> u64 {
    1000
}

fn default_replication_stall_ms() -> u64 {
    30_000
}

impl ReplicationConfig {
    /// Parses `cluster_uuid` into its 16 raw bytes (hyphens optional).
    pub fn cluster_uuid_bytes(&self) -> Result<[u8; 16], String> {
        let raw = self
            .cluster_uuid
            .as_deref()
            .ok_or("replication.cluster_uuid is required when replication is enabled")?;
        let hex: String = raw.chars().filter(|c| *c != '-').collect();
        if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(format!(
                "replication.cluster_uuid must be a UUID (32 hex digits, hyphens optional): {raw}"
            ));
        }
        let mut out = [0u8; 16];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).expect("checked hex");
        }
        Ok(out)
    }
}

/// TDS (SQL Server protocol) gateway settings. Disabled by default; when
/// enabled it listens on `port` (default 1433). Authentication is against
/// catalog logins with salted PBKDF2 hashes, NOT this config: `[tds.auth]` only
/// SEEDS logins into the catalog on the first boot (see [`Self::auth`]).
#[derive(Debug, Deserialize)]
pub struct TdsConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_addr")]
    pub addr: String,

    #[serde(default = "default_tds_port")]
    pub port: u16,

    #[serde(default = "default_tds_database")]
    pub database: String,

    /// First-boot seed only: `username = password` entries are hashed into
    /// catalog logins the first time the server starts (and `sa` is always
    /// ensured). The catalog is authoritative — this map is NEVER consulted for
    /// authentication. Once the built-in `sa` login exists the seed is inert:
    /// editing this map does not add, remove, or rotate a login; manage logins
    /// with `CREATE`/`ALTER`/`DROP LOGIN`. (Dropping `sa` re-arms the seed on the
    /// next start, recreating every configured login that is currently absent.)
    #[serde(default)]
    pub auth: HashMap<String, String>,

    /// PEM certificate chain path — enables TLS when set (with `tls_key`).
    #[serde(default)]
    pub tls_cert: Option<String>,

    /// PEM private key path (paired with `tls_cert`).
    #[serde(default)]
    pub tls_key: Option<String>,

    /// Encryption policy: `off` never encrypts, `optional` (the default)
    /// encrypts when the client asks, `required` refuses clients that will not
    /// encrypt. `required` needs `tls_cert`/`tls_key`.
    #[serde(default)]
    pub encryption: Encryption,
}

/// `[tds] encryption = "off" | "optional" | "required"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Encryption {
    Off,
    #[default]
    Optional,
    Required,
}

impl Default for TdsConfig {
    fn default() -> Self {
        TdsConfig {
            enabled: false,
            addr: default_addr(),
            port: default_tds_port(),
            database: default_tds_database(),
            auth: HashMap::new(),
            tls_cert: None,
            tls_key: None,
            encryption: Encryption::default(),
        }
    }
}

fn default_tds_port() -> u16 {
    1433
}

fn default_tds_database() -> String {
    "truthdb".to_string()
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

    /// The database's default collation: what a character column declared
    /// without an explicit COLLATE gets. Applied when the database file is
    /// created and stamped into it; an existing file keeps the collation it was
    /// created with, since its keys are already encoded under it.
    #[serde(default)]
    pub default_collation: Option<String>,
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
            default_collation: None,
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

/// Path of the system-wide config file installed by the Debian package.
const SYSTEM_CONFIG_PATH: &str = "/etc/truthdb/truthdb.toml";

impl Config {
    /// Load config: embedded default, then per-user XDG override, then system-wide override.
    ///
    /// Apply order is lowest-priority first so later applications win for fields they set.
    /// `/etc/truthdb/truthdb.toml` is the highest priority — when a sysadmin uncomments a
    /// value there, it overrides anything in `~/.config/org.truthdb/truthdb/truthdb.toml`.
    /// Missing files no-op.
    pub fn load() -> Self {
        let default_str = include_str!("../config/default.toml");
        let mut config: Config = toml::from_str(default_str).unwrap_or_default();

        // Per-user XDG config (dev convenience).
        if let Some(proj_dirs) = ProjectDirs::from("org", "truthdb", "truthdb") {
            let mut config_path = PathBuf::from(proj_dirs.config_dir());
            config_path.push("truthdb.toml");
            apply_override_file(&config_path, &mut config);
        }

        // System-wide config installed by the .deb (highest priority).
        apply_override_file(Path::new(SYSTEM_CONFIG_PATH), &mut config);

        config
    }
}

/// Read a TOML override file at `path` and apply it to `config`. No-op if the file
/// does not exist, cannot be read, or fails to parse.
fn apply_override_file(path: &Path, config: &mut Config) {
    if !path.exists() {
        return;
    }
    let Ok(contents) = fs::read_to_string(path) else {
        return;
    };
    let Ok(override_cfg) = toml::from_str::<ConfigOverride>(&contents) else {
        return;
    };
    apply_override(override_cfg, config);
}

/// Apply a parsed override to `config`. Each field is set only if the override specifies it.
fn apply_override(override_cfg: ConfigOverride, config: &mut Config) {
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
    if let Some(tds) = override_cfg.tds {
        apply_tds_override(&mut config.tds, tds);
    }
    if let Some(replication) = override_cfg.replication {
        apply_replication_override(&mut config.replication, replication);
    }
}

#[derive(Debug, Deserialize, Default)]
struct ConfigOverride {
    addr: Option<String>,
    port: Option<u16>,
    network: Option<NetworkConfigOverride>,
    storage: Option<StorageConfigOverride>,
    tds: Option<TdsConfigOverride>,
    replication: Option<ReplicationConfigOverride>,
}

#[derive(Debug, Deserialize, Default)]
struct ReplicationConfigOverride {
    enabled: Option<bool>,
    role: Option<ReplicationRole>,
    addr: Option<String>,
    port: Option<u16>,
    node_id: Option<u32>,
    cluster_uuid: Option<String>,
    shared_secret: Option<String>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_ca: Option<String>,
    primary_addr: Option<String>,
    server_name: Option<String>,
    heartbeat_ms: Option<u64>,
    reconnect_delay_ms: Option<u64>,
    stall_timeout_ms: Option<u64>,
    max_slot_retain_bytes: Option<u64>,
}

fn apply_replication_override(target: &mut ReplicationConfig, source: ReplicationConfigOverride) {
    if let Some(enabled) = source.enabled {
        target.enabled = enabled;
    }
    if let Some(role) = source.role {
        target.role = role;
    }
    if let Some(addr) = source.addr {
        target.addr = addr;
    }
    if let Some(port) = source.port {
        target.port = port;
    }
    if let Some(node_id) = source.node_id {
        target.node_id = node_id;
    }
    if source.cluster_uuid.is_some() {
        target.cluster_uuid = source.cluster_uuid;
    }
    if source.shared_secret.is_some() {
        target.shared_secret = source.shared_secret;
    }
    if source.tls_cert.is_some() {
        target.tls_cert = source.tls_cert;
    }
    if source.tls_key.is_some() {
        target.tls_key = source.tls_key;
    }
    if source.tls_ca.is_some() {
        target.tls_ca = source.tls_ca;
    }
    if source.primary_addr.is_some() {
        target.primary_addr = source.primary_addr;
    }
    if source.server_name.is_some() {
        target.server_name = source.server_name;
    }
    if let Some(heartbeat_ms) = source.heartbeat_ms {
        target.heartbeat_ms = heartbeat_ms;
    }
    if let Some(reconnect_delay_ms) = source.reconnect_delay_ms {
        target.reconnect_delay_ms = reconnect_delay_ms;
    }
    if let Some(stall_timeout_ms) = source.stall_timeout_ms {
        target.stall_timeout_ms = stall_timeout_ms;
    }
    if let Some(max_slot_retain_bytes) = source.max_slot_retain_bytes {
        target.max_slot_retain_bytes = max_slot_retain_bytes;
    }
}

#[derive(Debug, Deserialize, Default)]
struct TdsConfigOverride {
    enabled: Option<bool>,
    addr: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    auth: Option<HashMap<String, String>>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
}

fn apply_tds_override(target: &mut TdsConfig, source: TdsConfigOverride) {
    if let Some(enabled) = source.enabled {
        target.enabled = enabled;
    }
    if let Some(addr) = source.addr {
        target.addr = addr;
    }
    if let Some(port) = source.port {
        target.port = port;
    }
    if let Some(database) = source.database {
        target.database = database;
    }
    if let Some(auth) = source.auth {
        target.auth = auth;
    }
    if source.tls_cert.is_some() {
        target.tls_cert = source.tls_cert;
    }
    if source.tls_key.is_some() {
        target.tls_key = source.tls_key;
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> Config {
        let default_str = include_str!("../config/default.toml");
        toml::from_str(default_str).unwrap()
    }

    #[test]
    fn override_sets_only_specified_fields() {
        let toml = r#"
            [network]
            port = 12345

            [storage]
            size_gib = 42
        "#;
        let mut config = default_config();
        let override_cfg: ConfigOverride = toml::from_str(toml).unwrap();
        apply_override(override_cfg, &mut config);

        assert_eq!(config.network.port, 12345);
        assert_eq!(config.storage.size_gib, 42);
        // Unset fields keep defaults.
        assert_eq!(config.network.addr, "0.0.0.0");
        assert_eq!(config.storage.path, "truth.db");
    }

    #[test]
    fn replication_override_applies_and_uuid_parses() {
        let toml = r#"
            [replication]
            enabled = true
            role = "standby"
            node_id = 3
            cluster_uuid = "8f0e7a34-2d51-4c11-9c9e-3f6d2a7b1c05"
            shared_secret = "s3cret"
            primary_addr = "primary.example:9624"
            tls_ca = "/tmp/ca.pem"
        "#;
        let mut config = default_config();
        assert!(!config.replication.enabled, "disabled by default");
        let override_cfg: ConfigOverride = toml::from_str(toml).unwrap();
        apply_override(override_cfg, &mut config);

        assert!(config.replication.enabled);
        assert_eq!(config.replication.role, ReplicationRole::Standby);
        assert_eq!(config.replication.node_id, 3);
        assert_eq!(config.replication.port, 9624, "default port kept");
        let uuid = config.replication.cluster_uuid_bytes().unwrap();
        assert_eq!(uuid[0], 0x8f);
        assert_eq!(uuid[15], 0x05);

        config.replication.cluster_uuid = Some("not-a-uuid".to_string());
        assert!(config.replication.cluster_uuid_bytes().is_err());
        config.replication.cluster_uuid = None;
        assert!(config.replication.cluster_uuid_bytes().is_err());
    }

    #[test]
    fn override_file_missing_is_noop() {
        let mut config = default_config();
        apply_override_file(
            Path::new("/nonexistent/truthdb-test-missing.toml"),
            &mut config,
        );
        // Still default.
        assert_eq!(config.network.port, 9623);
    }

    #[test]
    fn override_file_malformed_is_noop() {
        let dir = std::env::temp_dir().join(format!(
            "truthdb-config-test-{}-malformed",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("truthdb.toml");
        fs::write(&path, "this is not valid toml === {{{").unwrap();

        let mut config = default_config();
        apply_override_file(&path, &mut config);
        assert_eq!(config.network.port, 9623);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn override_file_applies_when_valid() {
        let dir =
            std::env::temp_dir().join(format!("truthdb-config-test-{}-valid", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("truthdb.toml");
        fs::write(
            &path,
            r#"
                [network]
                addr = "127.0.0.1"
                port = 7777
            "#,
        )
        .unwrap();

        let mut config = default_config();
        apply_override_file(&path, &mut config);

        assert_eq!(config.network.addr, "127.0.0.1");
        assert_eq!(config.network.port, 7777);

        fs::remove_dir_all(&dir).ok();
    }
}
