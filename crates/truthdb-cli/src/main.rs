use std::io::{self, Write};

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "truthdb-cli")]
#[command(about = "Command-line client for TruthDB")]
#[command(version)]
struct Cli {
    /// Address of the TruthDB server (host:port).
    #[arg(long, env = "TRUTHDB_ADDR", default_value = "127.0.0.1:7777")]
    addr: String,

    /// Command to run (defaults to an interactive REPL).
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    /// Start an interactive session (psql-like REPL).
    Repl,

    /// Print the resolved server address and exit.
    ShowAddr,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command.unwrap_or(Command::Repl) {
        Command::Repl => repl(&cli.addr),
        Command::ShowAddr => {
            println!("{}", cli.addr);
            Ok(())
        }
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
