use directories::ProjectDirs;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_port() -> u16 {
    9623
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: default_port(),
        }
    }
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
                && let Ok(override_cfg) = toml::from_str::<Config>(&contents)
            {
                config.port = override_cfg.port;
            }
        }
        config
    }
}
