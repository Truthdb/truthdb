#[cfg(not(target_os = "linux"))]
compile_error!("TruthDB must be built for Linux targets. Use Docker or a Linux environment.");

use std::process::ExitCode;

use tokio::sync::watch;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;
use truthdb_engine::client_listener::ClientListener;
use truthdb_engine::engine::Engine;
use truthdb_relational::storage::{RelStorage, Storage};
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
async fn main() -> ExitCode {
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
    let config = match Config::load() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("Invalid config: {err}");
            return ExitCode::FAILURE;
        }
    };
    debug!(?config, "Loaded config");
    let storage_path = config.storage.resolved_path();
    info!(
        "Loaded config: addr={} port={} storage_path={} storage_size_gib={}",
        config.network.addr,
        config.network.port,
        storage_path.display(),
        config.storage.size_gib
    );

    let Some(size_bytes) = config.storage.size_gib.checked_mul(1 << 30) else {
        eprintln!("Invalid config: storage.size_gib is too large");
        return ExitCode::FAILURE;
    };
    let storage_opts = truthdb_relational::storage::StorageOptions {
        size_bytes,
        wal_ratio: config.storage.wal_ratio,
        metadata_ratio: config.storage.metadata_ratio,
        snapshot_ratio: config.storage.snapshot_ratio,
        allocator_ratio: config.storage.allocator_ratio,
        reserved_ratio: config.storage.reserved_ratio,
        default_collation: config.storage.default_collation.clone(),
    };

    let keys = match config.encryption.key_source() {
        Ok(keys) => keys,
        Err(err) => {
            eprintln!("Invalid config: {err}");
            return ExitCode::FAILURE;
        }
    };
    let storage = if storage_path.exists() {
        match RelStorage::open_with_keys(
            storage_path,
            truthdb_engine::database_engines,
            keys.as_ref(),
        ) {
            Ok(storage) => storage,
            Err(err) => {
                eprintln!("Failed to open storage: {err}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        match RelStorage::create_with_keys(
            storage_path,
            storage_opts,
            truthdb_engine::database_engines,
            keys.as_ref(),
        ) {
            Ok(storage) => storage,
            Err(err) => {
                eprintln!("Failed to create storage: {err}");
                return ExitCode::FAILURE;
            }
        }
    };

    if keys.is_some() && !storage.core_arc().is_encrypted() {
        warn!(
            "[encryption] is configured but {} is not encrypted: run `encryption enable` \
             to encrypt it",
            config.storage.resolved_path().display()
        );
    }

    let engine = match Engine::new(storage) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("Failed to initialize engine: {err}");
            return ExitCode::FAILURE;
        }
    };
    // The default database (id 1) is named by `[tds] database` — the one
    // source the sessions' database identity already comes from; storage's
    // name→id resolution must agree with it.
    if let Err(err) = engine.set_default_database(&config.tds.database) {
        eprintln!("Failed to set the default database name: {err}");
        return ExitCode::FAILURE;
    }
    // First boot migrates `[tds.auth]` config users into catalog logins (and
    // always ensures `sa`), then config auth is dead. Runs before the engine
    // thread is spawned — single-threaded, no session — and is a no-op once any
    // login already exists. A sorted map gives deterministic object-id order.
    let config_users: std::collections::BTreeMap<String, String> =
        config.tds.auth.clone().into_iter().collect();
    match engine.migrate_logins(&config_users) {
        Ok(created) if !created.is_empty() => {
            info!(
                "Migrated {} login(s) into the catalog: {}",
                created.len(),
                created.join(", ")
            );
        }
        Ok(_) => {}
        Err(err) => {
            eprintln!("Failed to migrate config logins: {err}");
            return ExitCode::FAILURE;
        }
    }
    match engine.access_key_secrets_in_plaintext() {
        Ok(true) => warn!(
            "S3 access key secrets are stored in plaintext: {} is not encrypted. Configure \
             [encryption] and run `encryption enable` to protect them",
            config.storage.resolved_path().display()
        ),
        Ok(false) => {}
        Err(err) => warn!("Could not check the S3 access keys: {err}"),
    }
    // The replication tasks share the storage directly (it is internally
    // synchronized); the handle must be taken before the engine moves into its
    // worker pool.
    let storage_arc = engine.storage_arc().core_arc();
    // The engine runs on its own thread behind a message channel; the async
    // listeners talk to it through a cloneable handle.
    let (engine, engine_join) = truthdb_engine::session::spawn_engine(engine);

    let client_listener =
        match ClientListener::new(&config.network.addr, config.network.port, engine.clone()) {
            Ok(client_listener) => client_listener,
            Err(err) => {
                eprintln!("Failed to initialize server: {err}");
                return ExitCode::FAILURE;
            }
        };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener_task = tokio::spawn(async move {
        if let Err(err) = client_listener.run(shutdown_rx).await {
            eprintln!("Client listener error: {err}");
        }
    });

    // Optional TDS (SQL Server protocol) gateway.
    let tds_task = if config.tds.enabled {
        let tls = match (&config.tds.tls_cert, &config.tds.tls_key) {
            (Some(cert_path), Some(key_path)) => match load_tds_tls(cert_path, key_path) {
                Ok(tls) => {
                    info!("TDS TLS enabled (cert {cert_path})");
                    Some(tls)
                }
                Err(err) => {
                    eprintln!("Failed to load TDS TLS certificate/key: {err}");
                    return ExitCode::FAILURE;
                }
            },
            (None, None) => None,
            _ => {
                eprintln!("TDS TLS needs both tls_cert and tls_key");
                return ExitCode::FAILURE;
            }
        };
        let encryption = match config.tds.encryption {
            crate::config::Encryption::Off => truthdb_tds::Encryption::Off,
            crate::config::Encryption::Optional => truthdb_tds::Encryption::Optional,
            crate::config::Encryption::Required => truthdb_tds::Encryption::Required,
        };
        // `required` with no certificate could satisfy nobody: every connection
        // would be refused. Say so at startup rather than at each client.
        if encryption == truthdb_tds::Encryption::Required && tls.is_none() {
            eprintln!("TDS encryption = \"required\" needs tls_cert and tls_key");
            return ExitCode::FAILURE;
        }
        if encryption == truthdb_tds::Encryption::Off && tls.is_some() {
            info!("TDS encryption = \"off\": the configured certificate will not be offered");
        }
        let tds_config = truthdb_tds::TdsConfig {
            database: config.tds.database.clone(),
            tls,
            encryption,
        };
        match truthdb_tds::TdsListener::bind(
            &config.tds.addr,
            config.tds.port,
            engine.clone(),
            tds_config,
        )
        .await
        {
            Ok(listener) => {
                info!(
                    "TDS gateway listening on {}:{}",
                    config.tds.addr, config.tds.port
                );
                let shutdown_rx = shutdown_tx.subscribe();
                Some(tokio::spawn(async move {
                    if let Err(err) = listener.run(shutdown_rx).await {
                        eprintln!("TDS listener error: {err}");
                    }
                }))
            }
            Err(err) => {
                eprintln!("Failed to start TDS gateway: {err}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };

    // Optional S3-compatible HTTP API.
    let s3_task = if config.s3.enabled {
        let tls = match (&config.s3.tls_cert, &config.s3.tls_key) {
            (Some(cert_path), Some(key_path)) => {
                let acceptor = std::fs::read(cert_path).and_then(|cert| {
                    let key = std::fs::read(key_path)?;
                    truthdb_core::repl::tls::acceptor_from_pem(&cert, &key)
                });
                match acceptor {
                    Ok(acceptor) => Some(acceptor),
                    Err(err) => {
                        eprintln!("Failed to load S3 TLS certificate/key: {err}");
                        return ExitCode::FAILURE;
                    }
                }
            }
            (None, None) => None,
            _ => {
                eprintln!("S3 TLS needs both tls_cert and tls_key");
                return ExitCode::FAILURE;
            }
        };
        match tokio::net::TcpListener::bind((config.s3.addr.as_str(), config.s3.port)).await {
            Ok(listener) => {
                info!(
                    "S3 API listening on {}:{}{}",
                    config.s3.addr,
                    config.s3.port,
                    if tls.is_some() { " (TLS)" } else { "" }
                );
                let state = std::sync::Arc::new(truthdb_s3::server::S3State {
                    engine: engine.clone(),
                    region: config.s3.region.clone(),
                    signing_keys: Default::default(),
                    storage: storage_arc.clone(),
                });
                let shutdown_rx = shutdown_tx.subscribe();
                Some(tokio::spawn(truthdb_s3::server::serve(
                    listener,
                    state,
                    tls,
                    shutdown_rx,
                )))
            }
            Err(err) => {
                eprintln!("Failed to start the S3 API: {err}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };

    // Optional physical replication (Stage 18): a primary's listener + senders,
    // or a standby's receiver.
    let repl_task = if config.replication.enabled {
        match start_replication(&config.replication, storage_arc, &shutdown_tx).await {
            Ok(task) => Some(task),
            Err(err) => {
                eprintln!("Failed to start replication: {err}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };

    info!("Starting TruthDB...");

    info!("TruthDB running (waiting for stop signal)");
    wait_for_shutdown_signal().await;

    info!("Stop signal received; shutting down...");
    let _ = shutdown_tx.send(true);
    let _ = listener_task.await;
    if let Some(tds_task) = tds_task {
        let _ = tds_task.await;
    }
    if let Some(repl_task) = repl_task {
        let _ = repl_task.await;
    }
    if let Some(s3_task) = s3_task {
        let _ = s3_task.await;
    }
    // Stop the engine thread and wait for it to drain/exit.
    engine.shutdown();
    let _ = tokio::task::spawn_blocking(move || engine_join.join()).await;
    info!("TruthDB exiting");
    ExitCode::SUCCESS
}

/// Loads a PEM certificate chain and private key into a TDS TLS config.
fn load_tds_tls(cert_path: &str, key_path: &str) -> std::io::Result<truthdb_tds::tls::TlsConfig> {
    let cert = std::fs::read(cert_path)?;
    let key = std::fs::read(key_path)?;
    truthdb_tds::tls::TlsConfig::from_pem(&cert, &key)
}

/// Validates the replication config and spawns the role's task: a primary's
/// TLS listener (+ per-standby senders), or a standby's reconnecting receiver.
/// A misconfiguration is a startup error, not a degraded run.
async fn start_replication(
    cfg: &config::ReplicationConfig,
    storage: std::sync::Arc<Storage>,
    shutdown_tx: &watch::Sender<bool>,
) -> Result<tokio::task::JoinHandle<()>, String> {
    use truthdb_core::repl::listener::{PrimaryReplContext, run_repl_listener};
    use truthdb_core::repl::receiver::{ReceiverConfig, run_standby_receiver};

    let cluster_uuid = cfg.cluster_uuid_bytes()?;
    if cfg.heartbeat_ms == 0 {
        return Err("replication.heartbeat_ms must be greater than 0".to_string());
    }
    if cfg.reconnect_delay_ms == 0 {
        return Err("replication.reconnect_delay_ms must be greater than 0".to_string());
    }
    if cfg.stall_timeout_ms == 0 {
        return Err("replication.stall_timeout_ms must be greater than 0".to_string());
    }
    // Both stall deadlines assume the peer speaks at least once per window (a
    // caught-up pair only exchanges heartbeats + their acks): a stall window
    // that a healthy idle link cannot meet makes every connection flap.
    if cfg.stall_timeout_ms < cfg.heartbeat_ms.saturating_mul(2) {
        return Err(format!(
            "replication.stall_timeout_ms ({}) must be at least twice heartbeat_ms ({}): \
             a caught-up standby only speaks when the primary heartbeats, so a tighter \
             stall window tears down healthy idle connections",
            cfg.stall_timeout_ms, cfg.heartbeat_ms
        ));
    }
    let secret =
        cfg.shared_secret.clone().filter(|s| !s.is_empty()).ok_or(
            "replication.shared_secret is required (non-empty) when replication is enabled",
        )?;

    match cfg.role {
        config::ReplicationRole::Primary => {
            let (cert_path, key_path) = match (&cfg.tls_cert, &cfg.tls_key) {
                (Some(cert), Some(key)) => (cert, key),
                _ => return Err("a replication primary needs tls_cert and tls_key".to_string()),
            };
            let cert = std::fs::read(cert_path)
                .map_err(|err| format!("replication tls_cert {cert_path}: {err}"))?;
            let key = std::fs::read(key_path)
                .map_err(|err| format!("replication tls_key {key_path}: {err}"))?;
            let acceptor = truthdb_core::repl::tls::acceptor_from_pem(&cert, &key)
                .map_err(|err| format!("replication TLS: {err}"))?;
            if cfg.max_slot_retain_bytes > 0 {
                storage
                    .set_max_slot_retain_bytes(cfg.max_slot_retain_bytes)
                    .map_err(|err| err.to_string())?;
            }
            if cfg.synchronous_commit {
                if cfg.sync_timeout_ms == 0 {
                    return Err("replication.sync_timeout_ms must be greater than 0".to_string());
                }
                storage.arm_sync_commit(std::time::Duration::from_millis(cfg.sync_timeout_ms));
                info!(
                    "Synchronous commit armed (timeout {} ms, availability-first)",
                    cfg.sync_timeout_ms
                );
            }
            let listener = tokio::net::TcpListener::bind((cfg.addr.as_str(), cfg.port))
                .await
                .map_err(|err| format!("replication listener {}:{}: {err}", cfg.addr, cfg.port))?;
            info!(
                "Replication primary listening on {}:{} (node {})",
                cfg.addr, cfg.port, cfg.node_id
            );
            let ctx = PrimaryReplContext {
                shared_secret: std::sync::Arc::new(secret.into_bytes()),
                cluster_uuid,
                storage,
                heartbeat: std::time::Duration::from_millis(cfg.heartbeat_ms),
                stall_timeout: std::time::Duration::from_millis(cfg.stall_timeout_ms),
                chunk_bytes: truthdb_core::repl::sender::DEFAULT_CHUNK_BYTES,
            };
            let shutdown_rx = shutdown_tx.subscribe();
            Ok(tokio::spawn(run_repl_listener(
                listener,
                acceptor,
                ctx,
                shutdown_rx,
            )))
        }
        config::ReplicationRole::Standby => {
            let primary_addr = cfg
                .primary_addr
                .clone()
                .ok_or("a replication standby needs primary_addr")?;
            let ca_path = cfg
                .tls_ca
                .as_ref()
                .ok_or("a replication standby needs tls_ca (the certificate to trust)")?;
            let tls_ca_pem = std::fs::read(ca_path)
                .map_err(|err| format!("replication tls_ca {ca_path}: {err}"))?;
            if !storage.is_standby() {
                return Err("this database is not a standby seed; restore it with \
                     `truthdb-cli restore --standby`"
                    .to_string());
            }
            if cfg.synchronous_commit {
                return Err(
                    "replication.synchronous_commit applies to the primary role; \
                     remove it from this standby's config"
                        .to_string(),
                );
            }
            let server_name = cfg.server_name.clone().unwrap_or_else(|| {
                let host = primary_addr
                    .rsplit_once(':')
                    .map(|(host, _)| host)
                    .unwrap_or(primary_addr.as_str());
                // A bracketed IPv6 host ("[2001:db8::1]:9624") is not a valid
                // TLS server name with the brackets on.
                host.strip_prefix('[')
                    .and_then(|h| h.strip_suffix(']'))
                    .unwrap_or(host)
                    .to_string()
            });
            let receiver_cfg = ReceiverConfig {
                primary_addr: primary_addr.clone(),
                server_name,
                tls_ca_pem,
                shared_secret: secret.into_bytes(),
                cluster_uuid,
                node_id: cfg.node_id,
                reconnect_delay: std::time::Duration::from_millis(cfg.reconnect_delay_ms),
                stall_timeout: std::time::Duration::from_millis(cfg.stall_timeout_ms),
            };
            info!(
                "Replication standby (node {}) following {}",
                cfg.node_id, primary_addr
            );
            let shutdown_rx = shutdown_tx.subscribe();
            Ok(tokio::spawn(run_standby_receiver(
                receiver_cfg,
                storage,
                shutdown_rx,
            )))
        }
    }
}
