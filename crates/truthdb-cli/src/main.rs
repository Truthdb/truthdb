mod config;

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

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load();
    let cli = Cli::parse();
    let host = cli.host.unwrap_or(config.host);
    let port = cli.port.unwrap_or(config.port);
    let addr = format!("{host}:{port}");

    match cli.command.unwrap_or(Command::Repl) {
        Command::Repl => repl(&addr).await,
    }
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

        let command = buffer.trim();
        if command.is_empty() {
            buffer.clear();
            continue;
        }

        let resp = send_command(&mut stream, next_id, command).await?;
        next_id = next_id.wrapping_add(1);
        let status = if resp.ok { "ok" } else { "err" };
        if resp.message.contains('\n') {
            println!("[{status}]");
            println!("{}", resp.message);
        } else {
            println!("[{status}] {}", resp.message);
        }
        buffer.clear();
    }

    Ok(())
}

fn command_is_complete(input: &str) -> bool {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return false;
    }

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

async fn send_hello(stream: &mut TcpStream) -> Result<()> {
    let req = HelloReq {
        protocol_version: PROTOCOL_VERSION,
        client_name: "truthdb-cli".to_string(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
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
