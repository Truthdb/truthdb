#[cfg(not(target_os = "linux"))]
compile_error!("truthdb-cli must be built for Linux targets. Use Docker or a Linux environment.");

mod config;
mod render;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::Config;
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use truthdb_net::{read_frame, write_frame};
use truthdb_proto::{
    CommandReq, CommandResp, Frame, HelloReq, HelloResp, MsgType, PROTOCOL_VERSION, decode_message,
    encode_message,
};

#[derive(Parser, Debug)]
#[command(name = "truthdb-cli")]
#[command(about = "Command-line client for TruthDB")]
#[command(version = env!("TRUTHDB_VERSION"))]
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
    /// OFFLINE: grow a stopped server's storage file by whole GiB. The
    /// server must not be running — this rewrites the file's region layout
    /// directly (crash-safe: the header flips last; a re-run completes an
    /// interrupted grow).
    Grow {
        /// Path to the storage file (e.g. /var/lib/truthdb/truth.db).
        #[arg(long)]
        path: std::path::PathBuf,
        /// GiB to add to the data region. A safe minimum applies (the
        /// relocated tail regions must clear the old layout); the error
        /// names it when the request is below it.
        #[arg(long)]
        add_gib: u64,
    },
    /// OFFLINE: restore a database file from a `TDBBAK1` full backup, optionally
    /// followed by an ordered chain of `BACKUP LOG` archives. The destination
    /// must not exist and the server must not be running.
    Restore {
        /// Path to the `TDBBAK1` full-backup file.
        #[arg(long)]
        full: std::path::PathBuf,
        /// Destination path for the restored database file (must not exist).
        #[arg(long)]
        to: std::path::PathBuf,
        /// A `BACKUP LOG` archive to apply after the full backup. Repeat in
        /// chain order (oldest first).
        #[arg(long = "log")]
        log: Vec<std::path::PathBuf>,
        /// Point-in-time restore: stop recovery at this wall-clock time
        /// (milliseconds since the Unix epoch). Transactions that committed
        /// after it are undone.
        #[arg(long = "stopat")]
        stopat: Option<u64>,
        /// Restore as a replication STANDBY seed: recovery repeats history
        /// only (no undo of in-flight transactions) and the file opens
        /// read-only, ready to follow a primary. Incompatible with --stopat.
        #[arg(long, conflicts_with = "stopat")]
        standby: bool,
    },
    /// OFFLINE: promote a replication standby to a read-write primary (manual
    /// failover). The server must be stopped. Finishes recovery (redo + undo
    /// of in-flight shipped transactions), clears the standby mode, and bumps
    /// the replication epoch — fencing the old timeline: the old primary (and
    /// any standby seeded before the failover) must reseed from a fresh
    /// backup of this node before it can follow it.
    Promote {
        /// Path to the standby's storage file (e.g. /var/lib/truthdb/truth.db).
        #[arg(long)]
        path: std::path::PathBuf,
    },
    /// ONLINE: show this server's replication state (role, slots, connected
    /// standbys, lag, sync-commit status) by querying the `sys.dm_repl_*`
    /// views over the native protocol.
    ReplStatus,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load();
    let cli = Cli::parse();
    let host = cli.host.unwrap_or(config.host);
    let port = cli.port.unwrap_or(config.port);
    let addr = format!("{host}:{port}");

    match cli.command.unwrap_or(Command::Repl) {
        Command::Repl => repl(&addr).await,
        Command::Grow { path, add_gib } => {
            match truthdb_core::storage::Storage::grow_data_region(&path, add_gib) {
                Ok(data_pages) => {
                    println!(
                        "grew {} by {add_gib} GiB: data region now {data_pages} pages ({} GiB)",
                        path.display(),
                        data_pages * 4096 / (1024 * 1024 * 1024),
                    );
                    Ok(())
                }
                Err(err) => Err(anyhow::anyhow!("grow failed: {err}")),
            }
        }
        Command::Restore {
            full,
            to,
            log,
            stopat,
            standby,
        } => {
            let result = if standby {
                truthdb_core::storage::Storage::restore_full_standby(&to, &full, &log)
            } else {
                truthdb_core::storage::Storage::restore_full_with_logs(&to, &full, &log, stopat)
            };
            match result {
                Ok(()) => {
                    println!(
                        "restored {} from {}{}{}{}",
                        to.display(),
                        full.display(),
                        if log.is_empty() {
                            String::new()
                        } else {
                            format!(" + {} log archive(s)", log.len())
                        },
                        match stopat {
                            Some(ts) => format!(" (stopped at {ts})"),
                            None => String::new(),
                        },
                        if standby { " (standby seed)" } else { "" }
                    );
                    Ok(())
                }
                Err(err) => Err(anyhow::anyhow!("restore failed: {err}")),
            }
        }
        Command::ReplStatus => repl_status(&addr).await,
        Command::Promote { path } => match truthdb_core::storage::Storage::promote(&path) {
            Ok(epoch) => {
                println!(
                    "promoted {} to primary (replication epoch {epoch}); reconfigure it as \
                     role = \"primary\" and reseed any other standby from a fresh backup",
                    path.display()
                );
                Ok(())
            }
            Err(err) => Err(anyhow::anyhow!("promote failed: {err}")),
        },
    }
}

/// Queries the replication DMVs and prints them.
async fn repl_status(addr: &str) -> Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    send_hello(&mut stream).await?;
    let mut id = 1u64;
    for (title, query) in [
        (
            "replica states",
            "SELECT * FROM sys.dm_repl_replica_states;",
        ),
        ("slots", "SELECT * FROM sys.dm_repl_slots;"),
    ] {
        let resp = send_command(&mut stream, id, query).await?;
        id = id.wrapping_add(1);
        println!("-- {title} --");
        let rendered = render::render(resp.ok, &resp.message);
        if rendered.is_empty() {
            println!("(none)");
        } else {
            println!("{rendered}");
        }
    }
    Ok(())
}

async fn repl(addr: &str) -> Result<()> {
    eprintln!("Connecting to {addr}. Type \\\\q to exit.");

    let mut stream = TcpStream::connect(addr).await?;
    send_hello(&mut stream).await?;

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut reader = io::BufReader::new(stdin);
    let mut line = String::new();
    let mut buffer = String::new();
    let mut next_id: u64 = 1;

    loop {
        let prompt = if buffer.trim().is_empty() {
            b"truthdb> ".as_slice()
        } else {
            b"......> ".as_slice()
        };
        stdout.write_all(prompt).await?;
        stdout.flush().await?;

        line.clear();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            if buffer.trim().is_empty() {
                break;
            }
            anyhow::bail!("incomplete command at end of input");
        }

        let chunk = line.trim_end_matches(['\n', '\r']);
        let trimmed = chunk.trim();

        if buffer.trim().is_empty() && (trimmed == "\\q" || trimmed == "quit" || trimmed == "exit")
        {
            break;
        }

        // A standalone `GO` (T-SQL batch separator) submits the buffered
        // statements without becoming part of them.
        if trimmed.eq_ignore_ascii_case("go") {
            let command = buffer.trim();
            if !command.is_empty() {
                submit(&mut stream, &mut next_id, command).await?;
            }
            buffer.clear();
            continue;
        }

        if trimmed.is_empty() && buffer.trim().is_empty() {
            continue;
        }

        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(chunk);

        if !command_is_complete(&buffer) {
            continue;
        }

        let command = buffer.trim().to_string();
        if command.is_empty() {
            buffer.clear();
            continue;
        }

        submit(&mut stream, &mut next_id, &command).await?;
        buffer.clear();
    }

    Ok(())
}

/// Sends a completed command and prints the rendered response.
async fn submit(stream: &mut TcpStream, next_id: &mut u64, command: &str) -> Result<()> {
    let resp = send_command(stream, *next_id, command).await?;
    *next_id = next_id.wrapping_add(1);
    let rendered = render::render(resp.ok, &resp.message);
    if !rendered.is_empty() {
        println!("{rendered}");
    }
    Ok(())
}

/// Decides when a buffered command is ready to submit. Legacy ES commands
/// (a `{`-bodied create index / insert document / search) complete when
/// their JSON braces balance; everything else is SQL and completes on a
/// top-level `;` (string/comment/bracket aware). A `GO` line is handled by
/// the caller.
fn command_is_complete(input: &str) -> bool {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return false;
    }
    // An ES command is a `{`-bodied create index / insert document / search.
    // Requiring the `{` avoids misreading SQL that merely starts with the
    // same word (e.g. `INSERT document VALUES ...` into a table named
    // `document`) as ES and then waiting forever for a brace.
    let lower = trimmed.to_ascii_lowercase();
    let is_es = (lower.starts_with("create index")
        || lower.starts_with("insert document")
        || lower.starts_with("search"))
        && trimmed.contains('{');
    if is_es {
        es_braces_balanced(trimmed)
    } else {
        sql_has_terminator(trimmed)
    }
}

fn es_braces_balanced(trimmed: &str) -> bool {
    let Some(start) = trimmed.find(['{', '[']) else {
        return true;
    };

    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    for ch in trimmed[start..].chars() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' | '[' => depth += 1,
            '}' | ']' => {
                depth -= 1;
                if depth < 0 {
                    return true;
                }
            }
            _ => {}
        }
    }

    depth == 0 && !in_string
}

/// True when the SQL buffer holds a top-level `;`, ignoring `;` inside
/// string literals `'…'`, delimited identifiers `[…]` / `"…"`, and
/// `--` / `/* */` comments.
fn sql_has_terminator(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        // `''` is an escaped quote; stay in the string.
                        if bytes.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if bytes.get(i + 1) == Some(&b'"') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'[' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b']' {
                        if bytes.get(i + 1) == Some(&b']') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                let mut depth = 1;
                while i < bytes.len() && depth > 0 {
                    if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                continue;
            }
            b';' => return true,
            _ => {}
        }
        i += 1;
    }
    false
}

async fn send_hello(stream: &mut TcpStream) -> Result<()> {
    let req = HelloReq {
        protocol_version: PROTOCOL_VERSION,
        client_name: "truthdb-cli".to_string(),
        client_version: env!("TRUTHDB_VERSION").to_string(),
    };

    let frame = Frame {
        msg_type: MsgType::HelloReq,
        flags: 0,
        payload: encode_message(&req)?,
    };

    write_frame(stream, &frame).await?;

    let resp_frame = read_frame(stream).await?;
    if resp_frame.msg_type != MsgType::HelloResp {
        anyhow::bail!("unexpected response: {:?}", resp_frame.msg_type);
    }

    let resp: HelloResp = decode_message(&resp_frame.payload)?;
    eprintln!(
        "Connected: {} {} (proto {})",
        resp.server_name, resp.server_version, resp.protocol_version
    );

    Ok(())
}

async fn send_command(stream: &mut TcpStream, id: u64, command: &str) -> Result<CommandResp> {
    let req = CommandReq {
        id,
        command: command.to_string(),
    };

    let frame = Frame {
        msg_type: MsgType::CommandReq,
        flags: 0,
        payload: encode_message(&req)?,
    };

    write_frame(stream, &frame).await?;

    let resp_frame = read_frame(stream).await?;
    if resp_frame.msg_type != MsgType::CommandResp {
        anyhow::bail!("unexpected response: {:?}", resp_frame.msg_type);
    }

    let resp: CommandResp = decode_message(&resp_frame.payload)?;
    Ok(resp)
}
