use directories::ProjectDirs;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub tds: TdsConfig,

    #[serde(default)]
    pub replication: ReplicationConfig,

    #[serde(default)]
    pub s3: S3Config,
}

/// The S3-compatible HTTP API. Disabled by default. Requests are signed with
/// AWS SigV4 against access keys created with `s3 create access key` (native
/// protocol); `tls_cert`/`tls_key` serve it over HTTPS.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_addr")]
    pub addr: String,

    #[serde(default = "default_s3_port")]
    pub port: u16,

    /// The region clients sign for (`--region` / `AWS_REGION`).
    #[serde(default = "default_s3_region")]
    pub region: String,

    /// PEM certificate chain path — enables HTTPS when set (with `tls_key`).
    #[serde(default)]
    pub tls_cert: Option<String>,

    /// PEM private key path (paired with `tls_cert`).
    #[serde(default)]
    pub tls_key: Option<String>,
}

impl Default for S3Config {
    fn default() -> Self {
        S3Config {
            enabled: false,
            addr: default_addr(),
            port: default_s3_port(),
            region: default_s3_region(),
            tls_cert: None,
            tls_key: None,
        }
    }
}

fn default_s3_port() -> u16 {
    9000
}

fn default_s3_region() -> String {
    "us-east-1".to_string()
}

/// Physical replication (Stage 18). Disabled by default. A `primary` runs the
/// replication listener on `port` (default 9624, TLS mandatory) and streams WAL
/// to authenticated standbys; a `standby` dials `primary_addr` and applies the
/// stream (its database file must be seeded with `truthdb-cli restore
/// --standby`). Both roles need the same `cluster_uuid` and `shared_secret`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// Primary: D2 synchronous commit — every commit waits (after local
    /// durability) for a standby's acknowledgement. Availability-first: a
    /// commit that waits past `sync_timeout_ms` marks the link
    /// NOT_SYNCHRONIZED and proceeds, as do later commits, until a standby
    /// catches back up.
    #[serde(default)]
    pub synchronous_commit: bool,

    /// Primary: the synchronous-commit wait bound (the strictness knob).
    #[serde(default = "default_replication_sync_timeout_ms")]
    pub sync_timeout_ms: u64,
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
            .field("synchronous_commit", &self.synchronous_commit)
            .field("sync_timeout_ms", &self.sync_timeout_ms)
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
            synchronous_commit: false,
            sync_timeout_ms: default_replication_sync_timeout_ms(),
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

fn default_replication_sync_timeout_ms() -> u64 {
    10_000
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    #[serde(default = "default_addr")]
    pub addr: String,

    #[serde(default = "default_port")]
    pub port: u16,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Load config: embedded default, then per-user XDG file, then system-wide file.
    ///
    /// Later files win for the keys they set. `/etc/truthdb/truthdb.toml` is the highest
    /// priority — when a sysadmin uncomments a value there, it overrides anything in
    /// `~/.config/org.truthdb/truthdb/truthdb.toml`. Missing files are skipped; a file that
    /// cannot be read, is not valid TOML, or sets an unknown key is an error.
    pub fn load() -> Result<Self, String> {
        // An explicit config file (tests, ad-hoc instances): applied INSTEAD
        // of the per-user and system files, so two instances on one machine
        // cannot bleed into each other.
        if let Some(path) = std::env::var_os("TRUTHDB_CONFIG") {
            return load_layers(&[PathBuf::from(path)]);
        }

        let mut files = Vec::new();
        // Per-user XDG config (dev convenience).
        if let Some(proj_dirs) = ProjectDirs::from("org", "truthdb", "truthdb") {
            files.push(proj_dirs.config_dir().join("truthdb.toml"));
        }
        // System-wide config installed by the .deb (highest priority).
        files.push(PathBuf::from(SYSTEM_CONFIG_PATH));
        load_layers(&files)
    }
}

/// Merge the embedded default and then each of `files` in order, and deserialize the
/// result. The config is deserialized after every file, so an unknown key or a wrongly
/// typed value is reported against the file that introduced it.
fn load_layers(files: &[PathBuf]) -> Result<Config, String> {
    let mut merged: toml::Table = toml::from_str(include_str!("../config/default.toml"))
        .expect("embedded default.toml is valid TOML");
    let mut config: Config = toml::Value::Table(merged.clone())
        .try_into()
        .expect("embedded default.toml matches Config");
    for path in files {
        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(format!("{}: {err}", path.display())),
        };
        let mut layer: toml::Table =
            toml::from_str(&contents).map_err(|err| format!("{}: {err}", path.display()))?;
        move_legacy_network_keys(&mut layer);
        merge_tables(&mut merged, layer, "");
        config = toml::Value::Table(merged.clone())
            .try_into()
            .map_err(|err| format!("{}: {err}", path.display()))?;
    }
    Ok(config)
}

/// `addr` and `port` were top-level keys before `[network]` existed. They still work;
/// within one file `[network]` wins.
fn move_legacy_network_keys(layer: &mut toml::Table) {
    for key in ["addr", "port"] {
        let Some(value) = layer.remove(key) else {
            continue;
        };
        let network = layer
            .entry("network")
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        if let toml::Value::Table(network) = network {
            network.entry(key).or_insert(value);
        }
    }
}

/// Merge `layer` into `base`: tables merge key by key, any other value replaces. The
/// exception is `tds.auth`: a file that sets it replaces the whole login seed map.
fn merge_tables(base: &mut toml::Table, layer: toml::Table, prefix: &str) {
    for (key, value) in layer {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match (base.get_mut(&key), value) {
            (Some(toml::Value::Table(base_table)), toml::Value::Table(layer_table))
                if path != "tds.auth" =>
            {
                merge_tables(base_table, layer_table, &path);
            }
            (_, value) => {
                base.insert(key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("truthdb-config-test-{}-{name}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write(dir: &Path, file: &str, contents: &str) -> PathBuf {
        let path = dir.join(file);
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn embedded_default_loads() {
        let config = load_layers(&[]).unwrap();
        assert_eq!(config.network.addr, "0.0.0.0");
        assert_eq!(config.network.port, 9623);
        assert_eq!(config.storage.path, "truth.db");
        assert!(!config.tds.enabled);
        assert_eq!(config.tds.encryption, Encryption::Optional);
        assert!(!config.replication.enabled, "disabled by default");
    }

    #[test]
    fn file_sets_only_specified_fields() {
        let dir = test_dir("partial");
        let path = write(
            &dir,
            "a.toml",
            "[network]\nport = 12345\n\n[storage]\nsize_gib = 42\n",
        );
        let config = load_layers(&[path]).unwrap();

        assert_eq!(config.network.port, 12345);
        assert_eq!(config.storage.size_gib, 42);
        // Unset fields keep defaults.
        assert_eq!(config.network.addr, "0.0.0.0");
        assert_eq!(config.storage.path, "truth.db");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn file_sets_tds_encryption() {
        let dir = test_dir("encryption");
        let path = write(&dir, "a.toml", "[tds]\nencryption = \"required\"\n");
        let config = load_layers(&[path]).unwrap();
        assert_eq!(config.tds.encryption, Encryption::Required);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn file_sets_s3() {
        let dir = test_dir("s3");
        let path = write(
            &dir,
            "a.toml",
            "[s3]\nenabled = true\nport = 9100\nregion = \"eu-north-1\"\n",
        );
        let config = load_layers(&[path]).unwrap();
        assert!(config.s3.enabled);
        assert_eq!(config.s3.port, 9100);
        assert_eq!(config.s3.region, "eu-north-1");
        assert_eq!(config.s3.tls_cert, None);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn file_sets_default_collation() {
        let dir = test_dir("collation");
        let path = write(
            &dir,
            "a.toml",
            "[storage]\ndefault_collation = \"Finnish_Swedish_CI_AS\"\n",
        );
        let config = load_layers(&[path]).unwrap();
        assert_eq!(
            config.storage.default_collation.as_deref(),
            Some("Finnish_Swedish_CI_AS")
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn replication_section_applies_and_uuid_parses() {
        let dir = test_dir("replication");
        let path = write(
            &dir,
            "a.toml",
            r#"
                [replication]
                enabled = true
                role = "standby"
                node_id = 3
                cluster_uuid = "8f0e7a34-2d51-4c11-9c9e-3f6d2a7b1c05"
                shared_secret = "s3cret"
                primary_addr = "primary.example:9624"
                tls_ca = "/tmp/ca.pem"
            "#,
        );
        let mut config = load_layers(&[path]).unwrap();

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
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unknown_key_is_an_error_naming_key_and_file() {
        let dir = test_dir("unknown");
        let path = write(&dir, "a.toml", "[tds]\nencrytion = \"required\"\n");
        let err = load_layers(std::slice::from_ref(&path)).unwrap_err();
        assert!(err.contains("encrytion"), "{err}");
        assert!(err.contains(&path.display().to_string()), "{err}");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn wrong_type_is_an_error_naming_file() {
        let dir = test_dir("type");
        let path = write(&dir, "a.toml", "[network]\nport = \"9623\"\n");
        let err = load_layers(std::slice::from_ref(&path)).unwrap_err();
        assert!(err.contains(&path.display().to_string()), "{err}");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn malformed_file_is_an_error_naming_file() {
        let dir = test_dir("malformed");
        let path = write(&dir, "a.toml", "this is not valid toml === {{{");
        let err = load_layers(std::slice::from_ref(&path)).unwrap_err();
        assert!(err.contains(&path.display().to_string()), "{err}");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unreadable_file_is_an_error() {
        let dir = test_dir("unreadable");
        // A directory where a file is expected cannot be read as a string.
        let path = dir.join("a.toml");
        fs::create_dir_all(&path).unwrap();
        let err = load_layers(std::slice::from_ref(&path)).unwrap_err();
        assert!(err.contains(&path.display().to_string()), "{err}");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_file_is_skipped() {
        let config =
            load_layers(&[PathBuf::from("/nonexistent/truthdb-test-missing.toml")]).unwrap();
        assert_eq!(config.network.port, 9623);
    }

    #[test]
    fn legacy_top_level_addr_and_port_apply() {
        let dir = test_dir("legacy");
        let path = write(&dir, "a.toml", "addr = \"127.0.0.1\"\nport = 7777\n");
        let config = load_layers(&[path]).unwrap();
        assert_eq!(config.network.addr, "127.0.0.1");
        assert_eq!(config.network.port, 7777);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn network_section_wins_over_legacy_keys_in_same_file() {
        let dir = test_dir("legacy-vs-network");
        let path = write(&dir, "a.toml", "port = 7777\n\n[network]\nport = 8888\n");
        let config = load_layers(&[path]).unwrap();
        assert_eq!(config.network.port, 8888);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn later_file_wins() {
        let dir = test_dir("order");
        let user = write(
            &dir,
            "user.toml",
            "[network]\nport = 1111\naddr = \"127.0.0.1\"\n",
        );
        let system = write(&dir, "system.toml", "[network]\nport = 2222\n");
        let config = load_layers(&[user, system]).unwrap();
        assert_eq!(config.network.port, 2222);
        assert_eq!(
            config.network.addr, "127.0.0.1",
            "kept from the earlier file"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn later_tds_auth_replaces_earlier_map() {
        let dir = test_dir("auth");
        let user = write(&dir, "user.toml", "[tds.auth]\nalice = \"a\"\n");
        let system = write(&dir, "system.toml", "[tds.auth]\nbob = \"b\"\n");
        let config = load_layers(&[user, system]).unwrap();
        assert_eq!(config.tds.auth.len(), 1);
        assert_eq!(config.tds.auth.get("bob").map(String::as_str), Some("b"));
        fs::remove_dir_all(&dir).ok();
    }
}
