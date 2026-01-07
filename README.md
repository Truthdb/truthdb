# truthdb

The main TruthDB executable.

## Runtime

`truthdb` runs as a Tokio application (async runtime) and is intended to be a long-lived process.

- Shutdown: exits cleanly on SIGTERM (systemd) and SIGINT (Ctrl+C)
- Logging: uses `tracing`; configure verbosity with `RUST_LOG` (e.g. `RUST_LOG=info`)
