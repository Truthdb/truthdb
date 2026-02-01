mod config;

use std::io::{self, Write};

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::Config;

#[derive(Parser, Debug)]
#[command(name = "truthdb-cli")]
#[command(about = "Command-line client for TruthDB")]
#[command(version)]
struct Cli {
    /// Host of the TruthDB server.
    #[arg(long, env = "TRUTHDB_HOST")]
    host: Option<String>,

    /// Port of the TruthDB server.
    #[arg(long, env = "TRUTHDB_PORT")]
    port: Option<u16>,

    /// Command to run (defaults to an interactive REPL).
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    /// Start an interactive session (psql-like REPL).
    Repl,
}

fn main() -> Result<()> {
    let config = Config::load();
    let cli = Cli::parse();
    let host = cli.host.unwrap_or(config.host);
    let port = cli.port.unwrap_or(config.port);
    let addr = format!("{host}:{port}");

    match cli.command.unwrap_or(Command::Repl) {
        Command::Repl => repl(&addr),
    }
}

fn repl(addr: &str) -> Result<()> {
    eprintln!("Connecting to {addr} (protocol not implemented yet). Type \\\\q to exit.");

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    loop {
        write!(stdout, "truthdb> ")?;
        stdout.flush()?;

        let mut line = String::new();
        let bytes = stdin.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed == "\\q" || trimmed == "quit" || trimmed == "exit" {
            break;
        }

        println!("(not implemented) would send: {trimmed}");
    }

    Ok(())
}
